package messaging

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeCancelConsumer implements jetstream.Consumer, faking only Fetch (the
// single method drainCancelTopic calls). Embedding a nil jetstream.Consumer
// satisfies the rest of the (large) interface at compile time; those
// methods are never invoked by the code under test.
type fakeCancelConsumer struct {
	jetstream.Consumer
	mu      sync.Mutex
	fetches int
	// failN leading Fetch calls return a transient error; the call after
	// that returns an empty batch, ending the drain loop naturally.
	failN int
}

func (f *fakeCancelConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	f.mu.Lock()
	f.fetches++
	n := f.fetches
	f.mu.Unlock()
	if n <= f.failN {
		return nil, errors.New("transient fetch error")
	}
	return emptyMessageBatch{}, nil
}

func (f *fakeCancelConsumer) fetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetches
}

type emptyMessageBatch struct{}

func (emptyMessageBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)
	return ch
}

func (emptyMessageBatch) Error() error { return nil }

// TestDrainCancelTopic_ContinuesPastTransientFetchError is a regression test
// for cancel-topic drain aborting on the first Fetch error instead of
// continuing and letting drainTimeout bound the loop.
// A transient Fetch failure must not truncate the drain — REQ-MSG-090
// requires the deny list to be fully populated before main-topic processing
// begins, so silently stopping early on one bad Fetch could let a
// should-have-been-denied resource through.
func TestDrainCancelTopic_ContinuesPastTransientFetchError(t *testing.T) {
	c := NewClient(ClientConfig{}, slog.Default())
	fc := &fakeCancelConsumer{failN: 2}

	c.drainCancelTopic(context.Background(), fc)

	if got := fc.fetchCount(); got < 3 {
		t.Fatalf("drainCancelTopic must retry past transient Fetch errors instead of aborting; got %d Fetch call(s), want >= 3 (2 failures + 1 success)", got)
	}
}
