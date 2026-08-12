package messaging

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeReadyConsumer implements jetstream.Consumer, faking only Consume (the
// method beginConsuming calls). Embedding a nil jetstream.Consumer satisfies
// the rest of the (large) interface at compile time.
type fakeReadyConsumer struct {
	jetstream.Consumer
	consumeCalls atomic.Int32
	// consumeErr, if set, is returned by Consume instead of succeeding —
	// used to simulate a transient failure (e.g. right after reconnect).
	consumeErr error
}

func (f *fakeReadyConsumer) Consume(jetstream.MessageHandler, ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	f.consumeCalls.Add(1)
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return &fakeConsumeContext{}, nil
}

type fakeConsumeContext struct{}

func (*fakeConsumeContext) Stop()                   {}
func (*fakeConsumeContext) Drain()                  {}
func (*fakeConsumeContext) Closed() <-chan struct{} { ch := make(chan struct{}); close(ch); return ch }

// TestSetupStreamsAndConsume_FiresOnSetupReadyBeforeConsuming verifies
// onSetupReady fires exactly once, synchronously within setupStreamsAndConsume
// with c.js/mainCons/cancelCons already populated, and before beginConsuming's
// live consume loops start — see onSetupReady's doc comment. (UT-MSG-080)
func TestSetupStreamsAndConsume_FiresOnSetupReadyBeforeConsuming(t *testing.T) {
	c := NewClient(ClientConfig{AgentName: "test-agent", TopicName: "t", DeferConsume: true}, slog.Default())

	fakeMain := &fakeReadyConsumer{}
	fakeCancel := &fakeReadyConsumer{}

	var (
		mu                  sync.Mutex
		jsReadyAtCallback   bool
		consumingAtCallback bool
		callbackFired       int
	)
	c.SetOnSetupReady(func() {
		mu.Lock()
		callbackFired++
		mu.Unlock()
		jsReadyAtCallback = c.js != nil && c.mainCons != nil && c.cancelCons != nil
		consumingAtCallback = c.consuming
		// Mirrors main.go's real callback: drain "on restart" (nothing to
		// fake here) then start live consumption.
		c.StartConsuming()
	})

	c.mu.Lock()
	c.conn = nil // never dereferenced by this test path
	c.js = fakeJetStream{}
	c.mainCons = fakeMain
	c.cancelCons = fakeCancel
	c.mu.Unlock()

	// Calls the real production function directly — finishSetup is the part
	// of setupStreamsAndConsume that needs no live NATS connection to exercise.
	ok := c.finishSetup()
	if !ok {
		t.Fatal("finishSetup must report success when beginConsuming (called from within the callback) succeeds")
	}

	if callbackFired != 1 {
		t.Fatalf("onSetupReady must fire exactly once, fired %d times", callbackFired)
	}
	if !jsReadyAtCallback {
		t.Fatal("onSetupReady must fire only after js/mainCons/cancelCons are already populated")
	}
	if consumingAtCallback {
		t.Fatal("onSetupReady must fire BEFORE live consumption begins (consuming was already true)")
	}
	if fakeMain.consumeCalls.Load() != 1 || fakeCancel.consumeCalls.Load() != 1 {
		t.Fatalf("StartConsuming called from within onSetupReady must start exactly one live consumer per topic, got main=%d cancel=%d",
			fakeMain.consumeCalls.Load(), fakeCancel.consumeCalls.Load())
	}
}

// fakeJetStream is a minimal non-nil jetstream.JetStream so c.js != nil
// checks pass; no method on it is called by this test.
type fakeJetStream struct {
	jetstream.JetStream
}

// TestBeginConsuming_ConcurrentCallsStartExactlyOneConsumeLoopEach verifies
// beginConsuming holds c.mu for its entire check-then-act sequence, so under
// concurrent calls only one caller ever invokes Consume() per topic. (UT-MSG-090)
func TestBeginConsuming_ConcurrentCallsStartExactlyOneConsumeLoopEach(t *testing.T) {
	// Many trials, each releasing a batch of goroutines simultaneously via a
	// shared start barrier, to maximize the odds of catching a race.
	const trials = 200
	const goroutinesPerTrial = 8

	for trial := range trials {
		c := NewClient(ClientConfig{}, slog.Default())
		fakeMain := &fakeReadyConsumer{}
		fakeCancel := &fakeReadyConsumer{}
		c.mainCons = fakeMain
		c.cancelCons = fakeCancel

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(goroutinesPerTrial)
		for range goroutinesPerTrial {
			go func() {
				defer wg.Done()
				<-start
				_ = c.beginConsuming()
			}()
		}
		close(start)
		wg.Wait()

		if got := fakeMain.consumeCalls.Load(); got != 1 {
			t.Fatalf("trial %d: exactly one goroutine must win and call Consume() on the main consumer under concurrent beginConsuming calls, got %d calls", trial, got)
		}
		if got := fakeCancel.consumeCalls.Load(); got != 1 {
			t.Fatalf("trial %d: exactly one goroutine must win and call Consume() on the cancel consumer under concurrent beginConsuming calls, got %d calls", trial, got)
		}
		if len(c.consumers) != 2 {
			t.Fatalf("trial %d: c.consumers must contain exactly 2 entries (one per topic) after concurrent beginConsuming calls, got %d", trial, len(c.consumers))
		}
	}
}

// TestFinishSetup_DoesNotReportSuccessWhenBeginConsumingFailsAfterCallback
// verifies finishSetup reports failure (not success) when the callback's
// StartConsuming() fails to actually start consuming, so attemptSetup retries
// instead of latching setupDone and stranding the client — see finishSetup's
// doc comment. (UT-MSG-095)
func TestFinishSetup_DoesNotReportSuccessWhenBeginConsumingFailsAfterCallback(t *testing.T) {
	c := NewClient(ClientConfig{AgentName: "test-agent", TopicName: "t", DeferConsume: true}, slog.Default())

	fakeMain := &fakeReadyConsumer{consumeErr: errors.New("transient: connection draining")}
	fakeCancel := &fakeReadyConsumer{}

	c.SetOnSetupReady(func() {
		// Mirrors main.go's real callback shape: call StartConsuming()
		// synchronously and ignore its error, exactly like the production
		// callback does (StartConsuming itself returns nothing).
		c.StartConsuming()
	})

	c.mu.Lock()
	c.js = fakeJetStream{}
	c.mainCons = fakeMain
	c.cancelCons = fakeCancel
	c.mu.Unlock()

	ok := c.finishSetup()
	if ok {
		t.Fatal("finishSetup must report failure when the callback's StartConsuming() failed to start consuming — " +
			"reporting success here would let attemptSetup latch setupDone and permanently strand the client")
	}
	if c.isConsuming() {
		t.Fatal("client must not be marked consuming when the main consumer's Consume() call failed")
	}

	// Simulate the retry this failure is supposed to trigger: the fake
	// consumer's transient error clears (as it would on the next successful
	// reconnect), and doSetup's retry path calls finishSetup again directly
	// (consumeRequested is already latched true, so it takes the plain
	// beginConsuming branch, not onSetupReady again).
	fakeMain.consumeErr = nil
	ok = c.finishSetup()
	if !ok {
		t.Fatal("finishSetup must succeed once beginConsuming succeeds on retry")
	}
	if !c.isConsuming() {
		t.Fatal("client must be marked consuming after a successful retry")
	}
	if fakeMain.consumeCalls.Load() != 2 {
		t.Fatalf("main consumer's Consume() must be retried directly (not via a second onSetupReady call), got %d calls", fakeMain.consumeCalls.Load())
	}
}
