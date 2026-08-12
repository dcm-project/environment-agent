package messaging

import (
	"context"
	"log/slog"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestAttemptSetup_SetupDoneShortCircuits verifies attemptSetup's setupDone
// guard returns before setupStreamsAndConsume runs again, so a NATS
// reconnect can't re-append to c.consumers. A nil *nats.Conn is safe here
// only because that guard fires first.
func TestAttemptSetup_SetupDoneShortCircuits(t *testing.T) {
	c := NewClient(ClientConfig{AgentName: "test-agent", TopicName: "t"}, slog.Default())
	c.setupDone = true // simulate: initial setup already completed once

	done := c.attemptSetup(context.Background(), nil)
	if !done {
		t.Fatal("attemptSetup must report done immediately when setupDone is already true")
	}
	if len(c.consumers) != 0 {
		t.Fatalf("consumers slice must not grow when setup is short-circuited, got %d entries", len(c.consumers))
	}
}

// TestAttemptSetup_RecoversAfterTransientFailure is the regression test for
// the setupOnce-recovery fix: a failed setupStreamsAndConsume attempt must
// NOT latch setupDone, so a later retry (background retry loop or a fresh
// connect) can still succeed. TestAttemptSetup_SetupDoneShortCircuits only
// covers the already-done short-circuit; this covers actual recovery.
func TestAttemptSetup_RecoversAfterTransientFailure(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	defer srv.Shutdown()

	conn, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Simulate the control-plane-owned request stream (REQ-MSG-048), same as
	// suite_test.go's Ginkgo fixture, so the second (recovery) attempt has
	// somewhere to bind its durable consumers.
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setupCancel()
	if _, err := js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name: RequestStreamName, Subjects: []string{"dcm.agent.>"},
	}); err != nil {
		t.Fatal(err)
	}

	c := NewClient(ClientConfig{AgentName: "test-agent", TopicName: "setup-recovery-topic"}, slog.Default())
	defer c.Stop()

	// First attempt: an already-expired context makes initInternalStreams'
	// JetStream call fail immediately, no need to break the real connection.
	failCtx, failCancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer failCancel()
	time.Sleep(2 * time.Millisecond)

	if done := c.attemptSetup(failCtx, conn); done {
		t.Fatal("attemptSetup must report failure when setupStreamsAndConsume fails")
	}
	if c.setupDone {
		t.Fatal("setupDone must stay false after a failed setup attempt, or no retry can ever recover")
	}

	// Second attempt: fresh context, same connection — must actually retry
	// setupStreamsAndConsume (not stay stuck) and succeed.
	okCtx, okCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer okCancel()
	if done := c.attemptSetup(okCtx, conn); !done {
		t.Fatal("attemptSetup must succeed on retry after the earlier transient failure")
	}
	if !c.setupDone {
		t.Fatal("setupDone must be true after a successful setup attempt")
	}
}

// TestBeginConsuming_ConsumingFlagShortCircuits complements the above:
// even if setupStreamsAndConsume were ever re-entered (e.g. a future
// DeferConsume/StartConsuming race), beginConsuming has its own independent
// consuming-flag guard preventing a second append to c.consumers.
func TestBeginConsuming_ConsumingFlagShortCircuits(t *testing.T) {
	c := NewClient(ClientConfig{}, slog.Default())
	c.consuming = true // simulate: live consumption already started once

	if err := c.beginConsuming(); err != nil {
		t.Fatalf("beginConsuming must be a no-op once consuming is true, got err: %v", err)
	}
	if len(c.consumers) != 0 {
		t.Fatalf("consumers slice must not grow on a repeated beginConsuming call, got %d entries", len(c.consumers))
	}
}
