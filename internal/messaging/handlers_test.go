package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
)

// captureHandler records slog records for assertion.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) lastRecord() slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) == 0 {
		return slog.Record{}
	}
	return h.records[len(h.records)-1]
}

func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

// fakeMsg implements the subset of jetstream.Msg needed by handlers.
type fakeMsg struct {
	data     []byte
	subject  string
	ackErr   error
	nakErr   error
	meta     *jetstream.MsgMetadata
	metaErr  error
	ackCount int
	nakCount int
}

func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Reply() string        { return "" }
func (m *fakeMsg) Headers() nats.Header { return nil }

func (m *fakeMsg) Ack() error {
	m.ackCount++
	return m.ackErr
}

func (m *fakeMsg) Nak() error { return nil }

func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	_ = d
	m.nakCount++
	return m.nakErr
}

func (m *fakeMsg) InProgress() error               { return nil }
func (m *fakeMsg) Term() error                     { return nil }
func (m *fakeMsg) TermWithReason(string) error     { return nil }
func (m *fakeMsg) DoubleAck(context.Context) error { return nil }

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return m.meta, nil
}

func buildTestCE(id, ceType string) []byte {
	data, err := json.Marshal(map[string]string{"id": id, "type": ceType})
	if err != nil {
		panic(err)
	}
	return data
}

func TestExtractCEIdentity(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		wantID   string
		wantType string
	}{
		{
			name:     "valid CE with id and type",
			input:    buildTestCE("evt-123", cloudevent.TypeRequestCreate),
			wantID:   "evt-123",
			wantType: cloudevent.TypeRequestCreate,
		},
		{
			name:     "id present, type missing",
			input:    []byte(`{"id":"evt-456"}`),
			wantID:   "evt-456",
			wantType: "unknown",
		},
		{
			name:     "id missing",
			input:    []byte(`{"type":cloudevent.TypeRequestCreate}`),
			wantID:   "unknown",
			wantType: "unknown",
		},
		{
			name:     "malformed JSON",
			input:    []byte(`not json`),
			wantID:   "unknown",
			wantType: "unknown",
		},
		{
			name:     "nil input",
			input:    nil,
			wantID:   "unknown",
			wantType: "unknown",
		},
		{
			name:     "empty slice",
			input:    []byte{},
			wantID:   "unknown",
			wantType: "unknown",
		},
		{
			name:     "empty id string",
			input:    []byte(`{"id":"","type":cloudevent.TypeRequestDelete}`),
			wantID:   "unknown",
			wantType: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotType := extractCEIdentity(tt.input)
			if gotID != tt.wantID {
				t.Errorf("extractCEIdentity() id = %q, want %q", gotID, tt.wantID)
			}
			if gotType != tt.wantType {
				t.Errorf("extractCEIdentity() type = %q, want %q", gotType, tt.wantType)
			}
		})
	}
}

func TestHandleMainMessage_AckFailure(t *testing.T) {
	ch := &captureHandler{}
	logger := slog.New(ch)

	c := &Client{
		logger:      logger,
		mainHandler: func(_ context.Context, _ []byte) error { return nil },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-main-001", cloudevent.TypeRequestCreate),
		subject: "agent-test.main",
		ackErr:  errors.New("ack timeout"),
		meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: 42, Consumer: 7},
			NumDelivered: 2,
		},
	}

	c.handleMainMessage(msg)

	if msg.ackCount != 1 {
		t.Errorf("expected 1 ack call, got %d", msg.ackCount)
	}
	if ch.count() != 1 {
		t.Fatalf("expected 1 log record, got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to ack main message, may be redelivered" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	assertAttr(t, rec, "ce_id", "evt-main-001")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestCreate)
	assertAttr(t, rec, "subject", "agent-test.main")
	assertAttrUint64(t, rec, "stream_seq", 42)
	assertAttrUint64(t, rec, "consumer_seq", 7)
	assertAttrUint64(t, rec, "num_delivered", 2)
}

func TestHandleMainMessage_NakFailure(t *testing.T) {
	ch := &captureHandler{}
	logger := slog.New(ch)

	c := &Client{
		logger:      logger,
		mainHandler: func(_ context.Context, _ []byte) error { return errors.New("handler error") },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-nak-001", cloudevent.TypeRequestDelete),
		subject: "agent-test.main",
		nakErr:  errors.New("nak failed"),
		meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: 10, Consumer: 3},
			NumDelivered: 1,
		},
	}

	c.handleMainMessage(msg)

	if msg.nakCount != 1 {
		t.Errorf("expected 1 nak call, got %d", msg.nakCount)
	}
	if msg.ackCount != 0 {
		t.Errorf("expected 0 ack calls on handler-error path, got %d", msg.ackCount)
	}
	if ch.count() != 1 {
		t.Fatalf("expected 1 log record, got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to nak main message" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	assertAttr(t, rec, "ce_id", "evt-nak-001")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestDelete)
}

func TestHandleCancelMessage_AckFailure(t *testing.T) {
	ch := &captureHandler{}
	logger := slog.New(ch)

	c := &Client{
		logger:        logger,
		cancelHandler: func(_ context.Context, _ []byte) error { return nil },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-cancel-001", cloudevent.TypeRequestCancel),
		subject: "agent-test.cancel",
		ackErr:  errors.New("ack timeout"),
		meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: 5, Consumer: 1},
			NumDelivered: 1,
		},
	}

	c.handleCancelMessage(msg)

	if msg.ackCount != 1 {
		t.Errorf("expected 1 ack call, got %d", msg.ackCount)
	}
	if ch.count() != 1 {
		t.Fatalf("expected 1 log record, got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to ack cancel message, may be redelivered" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	assertAttr(t, rec, "ce_id", "evt-cancel-001")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestCancel)
	assertAttr(t, rec, "subject", "agent-test.cancel")
}

func TestHandleCancelMessage_NakFailure(t *testing.T) {
	ch := &captureHandler{}
	logger := slog.New(ch)

	c := &Client{
		logger:        logger,
		cancelHandler: func(_ context.Context, _ []byte) error { return errors.New("cancel handler error") },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-cancel-nak", cloudevent.TypeRequestCancel),
		subject: "agent-test.cancel",
		nakErr:  errors.New("nak failed"),
		meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: 8, Consumer: 2},
			NumDelivered: 3,
		},
	}

	c.handleCancelMessage(msg)

	if msg.nakCount != 1 {
		t.Errorf("expected 1 nak call, got %d", msg.nakCount)
	}
	if msg.ackCount != 0 {
		t.Errorf("expected 0 ack calls on handler-error path, got %d", msg.ackCount)
	}
	if ch.count() != 1 {
		t.Fatalf("expected 1 log record, got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to nak cancel message" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	assertAttr(t, rec, "ce_id", "evt-cancel-nak")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestCancel)
	assertAttrUint64(t, rec, "stream_seq", 8)
	assertAttrUint64(t, rec, "num_delivered", 3)
}

func TestHandleMainMessage_MetadataUnavailable(t *testing.T) {
	ch := &captureHandler{}
	logger := slog.New(ch)

	c := &Client{
		logger:      logger,
		mainHandler: func(_ context.Context, _ []byte) error { return nil },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-no-meta", cloudevent.TypeRequestCreate),
		subject: "agent-test.main",
		ackErr:  errors.New("ack timeout"),
		metaErr: errors.New("no metadata"),
	}

	c.handleMainMessage(msg)

	if ch.count() != 1 {
		t.Fatalf("expected 1 log record, got %d", ch.count())
	}
	rec := ch.lastRecord()
	assertAttr(t, rec, "ce_id", "evt-no-meta")
	assertAttrExists(t, rec, "meta_error")
	assertAttrNotExists(t, rec, "stream_seq")
}

func assertAttr(t *testing.T, rec slog.Record, key, want string) {
	t.Helper()
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = true
			if a.Value.String() != want {
				t.Errorf("attr %q = %q, want %q", key, a.Value.String(), want)
			}
			return false
		}
		return true
	})
	if !found {
		t.Errorf("attr %q not found in log record", key)
	}
}

func assertAttrUint64(t *testing.T, rec slog.Record, key string, want uint64) {
	t.Helper()
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = true
			if a.Value.Uint64() != want {
				t.Errorf("attr %q = %d, want %d", key, a.Value.Uint64(), want)
			}
			return false
		}
		return true
	})
	if !found {
		t.Errorf("attr %q not found in log record", key)
	}
}

func assertAttrExists(t *testing.T, rec slog.Record, key string) {
	t.Helper()
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = true
			return false
		}
		return true
	})
	if !found {
		t.Errorf("attr %q not found in log record", key)
	}
}

func assertAttrNotExists(t *testing.T, rec slog.Record, key string) {
	t.Helper()
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			t.Errorf("attr %q should not exist in log record but was found", key)
			return false
		}
		return true
	})
}
