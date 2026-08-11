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
	data      []byte
	subject   string
	ackErr    error
	nakErr    error
	termErr   error
	meta      *jetstream.MsgMetadata
	metaErr   error
	ackCount  int
	nakCount  int
	termCount int
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

func (m *fakeMsg) InProgress() error { return nil }
func (m *fakeMsg) Term() error {
	m.termCount++
	return m.termErr
}
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

func buildTestCEWithResourceID(id, ceType, resourceID string) []byte {
	data, err := json.Marshal(map[string]any{
		"id": id, "type": ceType, "data": map[string]string{"resource_id": resourceID},
	})
	if err != nil {
		panic(err)
	}
	return data
}

func TestExtractLogFields(t *testing.T) {
	tests := []struct {
		name           string
		input          []byte
		wantResourceID string
		wantID         string
		wantType       string
	}{
		{
			name:           "valid CE with id, type, and resource_id",
			input:          []byte(`{"id":"evt-123","type":"dcm.request.create","data":{"resource_id":"res-1"}}`),
			wantResourceID: "res-1",
			wantID:         "evt-123",
			wantType:       cloudevent.TypeRequestCreate,
		},
		{
			name:     "id present, type missing",
			input:    []byte(`{"id":"evt-456"}`),
			wantID:   "evt-456",
			wantType: "unknown",
		},
		{
			name:     "id missing",
			input:    []byte(`{"type":"dcm.request.create"}`),
			wantID:   "unknown",
			wantType: cloudevent.TypeRequestCreate,
		},
		{
			name:           "malformed JSON",
			input:          []byte(`not json`),
			wantResourceID: "",
			wantID:         "unknown",
			wantType:       "unknown",
		},
		{
			name:           "nil input",
			input:          nil,
			wantResourceID: "",
			wantID:         "unknown",
			wantType:       "unknown",
		},
		{
			name:           "empty slice",
			input:          []byte{},
			wantResourceID: "",
			wantID:         "unknown",
			wantType:       "unknown",
		},
		{
			name:     "empty id string",
			input:    []byte(`{"id":"","type":"dcm.request.delete"}`),
			wantID:   "unknown",
			wantType: cloudevent.TypeRequestDelete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResourceID, gotID, gotType := extractLogFields(tt.input)
			if gotResourceID != tt.wantResourceID {
				t.Errorf("extractLogFields() resourceID = %q, want %q", gotResourceID, tt.wantResourceID)
			}
			if gotID != tt.wantID {
				t.Errorf("extractLogFields() id = %q, want %q", gotID, tt.wantID)
			}
			if gotType != tt.wantType {
				t.Errorf("extractLogFields() type = %q, want %q", gotType, tt.wantType)
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
		data:    buildTestCEWithResourceID("evt-main-001", cloudevent.TypeRequestCreate, "res-main-001"),
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
	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + ack failure), got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to ack main message, may be redelivered" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	assertAttr(t, rec, "resource_id", "res-main-001")
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
	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + nak failure), got %d", ch.count())
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
	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + ack failure), got %d", ch.count())
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
	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + nak failure), got %d", ch.count())
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

	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + ack failure), got %d", ch.count())
	}
	received := ch.records[0]
	assertAttrExists(t, received, "meta_error")
	assertAttrNotExists(t, received, "stream_seq")

	rec := ch.lastRecord()
	assertAttr(t, rec, "ce_id", "evt-no-meta")
	assertAttrExists(t, rec, "meta_error")
	assertAttrNotExists(t, rec, "stream_seq")
}

// TestHandleMainMessage_MaxDeliverMetadataFailure verifies the MaxDeliver
// guard's metadata-failure log includes correlation fields and the Nak()
// outcome, rather than silently discarding them.
func TestHandleMainMessage_MaxDeliverMetadataFailure(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:      slog.New(ch),
		cfg:         ClientConfig{MaxDeliver: 3},
		mainHandler: func(_ context.Context, _ []byte) error { return nil },
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-maxdeliver-meta", cloudevent.TypeRequestCreate, "res-maxdeliver-meta"),
		subject: "agent-test.main",
		metaErr: errors.New("no metadata"),
	}

	c.handleMainMessage(msg)

	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + metadata failure), got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to get message metadata for MaxDeliver guard" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	assertAttr(t, rec, "resource_id", "res-maxdeliver-meta")
	assertAttr(t, rec, "ce_id", "evt-maxdeliver-meta")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestCreate)
	assertAttrExists(t, rec, "error")
	assertAttrExists(t, rec, "nak_error")
}

func TestHandleMainMessage_SuccessLogsReceiptAndAck(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:      slog.New(ch),
		mainHandler: func(_ context.Context, _ []byte) error { return nil },
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-main-ok", cloudevent.TypeRequestCreate, "res-main-ok"),
		subject: "agent-test.main",
		meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: 1, Consumer: 1},
			NumDelivered: 1,
		},
	}

	c.handleMainMessage(msg)

	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + acked), got %d", ch.count())
	}
	received := ch.records[0]
	if received.Message != "message received" {
		t.Errorf("unexpected first log message: %s", received.Message)
	}
	if received.Level != slog.LevelInfo {
		t.Errorf("expected receipt log at INFO, got %s", received.Level)
	}
	assertAttr(t, received, "resource_id", "res-main-ok")
	assertAttr(t, received, "ce_id", "evt-main-ok")
	assertAttr(t, received, "ce_type", cloudevent.TypeRequestCreate)
	assertAttrUint64(t, received, "num_delivered", 1)

	acked := ch.lastRecord()
	if acked.Message != "main message acked" {
		t.Errorf("unexpected second log message: %s", acked.Message)
	}
	if acked.Level != slog.LevelInfo {
		t.Errorf("expected ack-success log at INFO, got %s", acked.Level)
	}
	assertAttr(t, acked, "resource_id", "res-main-ok")
	assertAttr(t, acked, "ce_id", "evt-main-ok")
}

func TestHandleMainMessage_HandlerErrorLogsWarnOnSuccessfulNak(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:      slog.New(ch),
		mainHandler: func(_ context.Context, _ []byte) error { return errors.New("sp unavailable") },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-main-nak", cloudevent.TypeRequestCreate),
		subject: "agent-test.main",
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
	}

	c.handleMainMessage(msg)

	if msg.nakCount != 1 {
		t.Errorf("expected 1 nak call, got %d", msg.nakCount)
	}
	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + nacked), got %d", ch.count())
	}
	nacked := ch.lastRecord()
	if nacked.Message != "main message nacked, handler failed" {
		t.Errorf("unexpected log message: %s", nacked.Message)
	}
	if nacked.Level != slog.LevelWarn {
		t.Errorf("expected nak-success log at WARN, got %s", nacked.Level)
	}
	assertAttr(t, nacked, "ce_id", "evt-main-nak")
	assertAttrExists(t, nacked, "resource_id")
	assertAttrExists(t, nacked, "error")
}

func TestHandleCancelMessage_SuccessLogsReceiptAndAck(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:        slog.New(ch),
		cancelHandler: func(_ context.Context, _ []byte) error { return nil },
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-cancel-ok", cloudevent.TypeRequestCancel, "res-cancel-ok"),
		subject: "agent-test.cancel",
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
	}

	c.handleCancelMessage(msg)

	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (received + acked), got %d", ch.count())
	}
	assertAttr(t, ch.records[0], "resource_id", "res-cancel-ok")
	assertAttr(t, ch.records[0], "ce_id", "evt-cancel-ok")
	if ch.records[0].Message != "message received" {
		t.Errorf("unexpected first log message: %s", ch.records[0].Message)
	}
	acked := ch.lastRecord()
	if acked.Message != "cancel message acked" {
		t.Errorf("unexpected second log message: %s", acked.Message)
	}
	assertAttr(t, acked, "resource_id", "res-cancel-ok")
}

// fakePublishJetStream is a minimal jetstream.JetStream that only implements
// Publish, used to exercise publishMaxDeliverError's success/failure logging
// without a real NATS connection.
type fakePublishJetStream struct {
	jetstream.JetStream
	publishErr error
}

func (f *fakePublishJetStream) Publish(_ context.Context, _ string, _ []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return &jetstream.PubAck{}, nil
}

func TestPublishMaxDeliverError_FailureLogsCEType(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger: slog.New(ch),
		cfg:    ClientConfig{AgentName: "agent-test"},
		topics: TopicNames{Main: "agent-test.main"},
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-maxdeliver", cloudevent.TypeRequestCreate, "res-maxdeliver"),
		subject: "agent-test.main",
		meta:    &jetstream.MsgMetadata{NumDelivered: 3},
	}

	c.publishMaxDeliverError(msg)

	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (max-delivery warn + publish failure warn), got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "failed to publish max-deliver error CE" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	if rec.Level != slog.LevelWarn {
		t.Errorf("expected publish-failure log at WARN, got %s", rec.Level)
	}
	assertAttr(t, rec, "resource_id", "res-maxdeliver")
	assertAttr(t, rec, "ce_id", "evt-maxdeliver")
	assertAttr(t, rec, "published_ce_type", cloudevent.TypeError)
	assertAttrExists(t, rec, "error")
}

func TestPublishMaxDeliverError_SuccessLogsInfo(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger: slog.New(ch),
		cfg:    ClientConfig{AgentName: "agent-test"},
		topics: TopicNames{Main: "agent-test.main"},
		js:     &fakePublishJetStream{},
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-maxdeliver-ok", cloudevent.TypeRequestCreate, "res-maxdeliver-ok"),
		subject: "agent-test.main",
		meta:    &jetstream.MsgMetadata{NumDelivered: 3},
	}

	c.publishMaxDeliverError(msg)

	if ch.count() != 2 {
		t.Fatalf("expected 2 log records (max-delivery warn + publish success info), got %d", ch.count())
	}
	rec := ch.lastRecord()
	if rec.Message != "published max-deliver error CE" {
		t.Errorf("unexpected message: %s", rec.Message)
	}
	if rec.Level != slog.LevelInfo {
		t.Errorf("expected publish-success log at INFO, got %s", rec.Level)
	}
	assertAttr(t, rec, "resource_id", "res-maxdeliver-ok")
	assertAttr(t, rec, "ce_id", "evt-maxdeliver-ok")
	assertAttr(t, rec, "published_ce_type", cloudevent.TypeError)
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

func TestHandleMainMessage_PanicRecovery(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:      slog.New(ch),
		mainHandler: func(_ context.Context, _ []byte) error { panic("boom") },
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-panic-main", cloudevent.TypeRequestCreate, "res-panic-main"),
		subject: "agent-test.main",
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
	}

	c.handleMainMessage(msg)

	if msg.nakCount != 1 {
		t.Errorf("expected 1 NakWithDelay after panic, got %d", msg.nakCount)
	}
	if msg.ackCount != 0 {
		t.Errorf("expected 0 acks after panic, got %d", msg.ackCount)
	}
	rec := ch.lastRecord()
	if rec.Message != "panic in main message handler" {
		t.Errorf("unexpected log message: %s", rec.Message)
	}
	assertAttrExists(t, rec, "panic")
	assertAttrExists(t, rec, "stack")
	assertAttr(t, rec, "resource_id", "res-panic-main")
	assertAttr(t, rec, "ce_id", "evt-panic-main")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestCreate)
	assertAttrNotExists(t, rec, "nak_error")
}

// TestHandleMainMessage_PanicRecovery_NakError verifies the nak_error attr
// on the panic log surfaces a real error (not just a nil placeholder) when
// the NakWithDelay resolution call issued from the recover path itself fails.
func TestHandleMainMessage_PanicRecovery_NakError(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:      slog.New(ch),
		mainHandler: func(_ context.Context, _ []byte) error { panic("boom") },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-panic-main-nakerr", cloudevent.TypeRequestCreate),
		subject: "agent-test.main",
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
		nakErr:  errors.New("nak failed after panic"),
	}

	c.handleMainMessage(msg)

	rec := ch.lastRecord()
	assertAttr(t, rec, "nak_error", "nak failed after panic")
}

func TestHandleCancelMessage_PanicRecovery(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:        slog.New(ch),
		cancelHandler: func(_ context.Context, _ []byte) error { panic("cancel boom") },
	}

	msg := &fakeMsg{
		data:    buildTestCEWithResourceID("evt-panic-cancel", cloudevent.TypeRequestCancel, "res-panic-cancel"),
		subject: "agent-test.cancel",
	}

	c.handleCancelMessage(msg)

	if msg.termCount != 1 {
		t.Errorf("expected 1 Term after panic (cancel has no MaxDeliver), got %d", msg.termCount)
	}
	if msg.nakCount != 0 {
		t.Errorf("expected 0 naks after cancel panic, got %d", msg.nakCount)
	}
	if msg.ackCount != 0 {
		t.Errorf("expected 0 acks after cancel panic, got %d", msg.ackCount)
	}
	rec := ch.lastRecord()
	if rec.Message != "panic in cancel message handler" {
		t.Errorf("unexpected log message: %s", rec.Message)
	}
	assertAttrExists(t, rec, "panic")
	assertAttrExists(t, rec, "stack")
	assertAttr(t, rec, "resource_id", "res-panic-cancel")
	assertAttr(t, rec, "ce_id", "evt-panic-cancel")
	assertAttr(t, rec, "ce_type", cloudevent.TypeRequestCancel)
	assertAttrNotExists(t, rec, "term_error")
}

// TestHandleCancelMessage_PanicRecovery_TermError verifies the term_error
// attr on the panic log surfaces a real error (not just a nil placeholder)
// when the Term resolution call issued from the recover path itself fails.
func TestHandleCancelMessage_PanicRecovery_TermError(t *testing.T) {
	ch := &captureHandler{}
	c := &Client{
		logger:        slog.New(ch),
		cancelHandler: func(_ context.Context, _ []byte) error { panic("cancel boom") },
	}

	msg := &fakeMsg{
		data:    buildTestCE("evt-panic-cancel-termerr", cloudevent.TypeRequestCancel),
		subject: "agent-test.cancel",
		termErr: errors.New("term failed after panic"),
	}

	c.handleCancelMessage(msg)

	rec := ch.lastRecord()
	assertAttr(t, rec, "term_error", "term failed after panic")
}
