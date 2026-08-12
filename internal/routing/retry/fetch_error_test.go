package retry

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeJetStream/fakeConsumer/fakeBatch embed the real (nil) interfaces so
// only the methods fetchAllFromConsumer actually calls need overriding.
type fakeJetStream struct {
	jetstream.JetStream
	consumer jetstream.Consumer
	err      error
}

func (f *fakeJetStream) Consumer(context.Context, string, string) (jetstream.Consumer, error) {
	return f.consumer, f.err
}

type fakeConsumer struct {
	jetstream.Consumer
	info    *jetstream.ConsumerInfo
	infoErr error
	fetchFn func() (jetstream.MessageBatch, error)
}

func (f *fakeConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	return f.info, f.infoErr
}

func (f *fakeConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return f.fetchFn()
}

type fakeBatch struct {
	msgs chan jetstream.Msg
	err  error
}

func (f *fakeBatch) Messages() <-chan jetstream.Msg { return f.msgs }
func (f *fakeBatch) Error() error                   { return f.err }

func closedMsgsChan() chan jetstream.Msg {
	ch := make(chan jetstream.Msg)
	close(ch)
	return ch
}

func testProcessor(js jetstream.JetStream) *Processor {
	return &Processor{deps: ProcessorDeps{
		JSProvider: func() jetstream.JetStream { return js },
		Logger:     slog.Default(),
	}}
}

// TestFetchAllFromConsumer_TopLevelFetchErrorIsReturned: a request-level
// Fetch failure must be returned, not swallowed as the expected timeout.
// (UT-RCM-010)
func TestFetchAllFromConsumer_TopLevelFetchErrorIsReturned(t *testing.T) {
	wantErr := errors.New("connection lost mid-fetch")
	cons := &fakeConsumer{
		info:    &jetstream.ConsumerInfo{NumPending: 5},
		fetchFn: func() (jetstream.MessageBatch, error) { return nil, wantErr },
	}
	p := testProcessor(&fakeJetStream{consumer: cons})

	msgs, err := p.fetchAllFromConsumer(context.Background(), "stream", "consumer")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the real Fetch error to be returned, got err=%v", err)
	}
	if msgs != nil {
		t.Fatalf("expected no messages collected, got %d", len(msgs))
	}
}

// TestFetchAllFromConsumer_BatchErrorIsReturned: a non-nil
// MessageBatch.Error() is a genuine mid-fetch failure and must be returned.
// (UT-RCM-020)
func TestFetchAllFromConsumer_BatchErrorIsReturned(t *testing.T) {
	wantErr := errors.New("consumer deleted mid-fetch")
	cons := &fakeConsumer{
		info: &jetstream.ConsumerInfo{NumPending: 5},
		fetchFn: func() (jetstream.MessageBatch, error) {
			return &fakeBatch{msgs: closedMsgsChan(), err: wantErr}, nil
		},
	}
	p := testProcessor(&fakeJetStream{consumer: cons})

	msgs, err := p.fetchAllFromConsumer(context.Background(), "stream", "consumer")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the real batch error to be returned, got err=%v", err)
	}
	if msgs != nil {
		t.Fatalf("expected no messages collected, got %d", len(msgs))
	}
}

// TestFetchAllFromConsumer_ExpectedTimeoutStillReturnsNilError: an empty
// batch with no error still ends the loop with a nil error. (UT-RCM-030)
func TestFetchAllFromConsumer_ExpectedTimeoutStillReturnsNilError(t *testing.T) {
	cons := &fakeConsumer{
		info: &jetstream.ConsumerInfo{NumPending: 5},
		fetchFn: func() (jetstream.MessageBatch, error) {
			return &fakeBatch{msgs: closedMsgsChan()}, nil
		},
	}
	p := testProcessor(&fakeJetStream{consumer: cons})

	msgs, err := p.fetchAllFromConsumer(context.Background(), "stream", "consumer")
	if err != nil {
		t.Fatalf("expected nil error for an expected empty/timeout batch, got %v", err)
	}
	if msgs != nil {
		t.Fatalf("expected no messages collected, got %d", len(msgs))
	}
}
