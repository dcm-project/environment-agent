package messaging

import (
	"context"
	"encoding/json"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func (c *Client) handleCancelMessage(msg jetstream.Msg) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("panic in cancel message handler",
				"panic", r, "subject", msg.Subject(), "stack", string(debug.Stack()))
			_ = msg.Term()
		}
	}()

	if err := c.cancelHandler(context.Background(), msg.Data()); err != nil {
		if nakErr := msg.NakWithDelay(c.nakDelay()); nakErr != nil {
			c.logMessageResolutionFailure("failed to nak cancel message", msg, nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		c.logMessageResolutionFailure("failed to ack cancel message, may be redelivered", msg, err)
	}
}

func (c *Client) handleMainMessage(msg jetstream.Msg) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("panic in main message handler",
				"panic", r, "subject", msg.Subject(), "stack", string(debug.Stack()))
			_ = msg.NakWithDelay(c.nakDelay())
		}
	}()

	if c.cfg.MaxDeliver > 0 {
		meta, err := msg.Metadata()
		if err != nil {
			c.logger.Warn("failed to get message metadata for MaxDeliver guard", "error", err)
			_ = msg.Nak()
			return
		}
		if meta.NumDelivered >= uint64(c.cfg.MaxDeliver) {
			c.publishMaxDeliverError(msg)
			// If Term fails (e.g. a transient connection drop, IT-RCM-080),
			// JetStream will redeliver this message and this MaxDeliver
			// guard fires again on the next attempt, publishing a duplicate
			// terminal error CE with a distinct id/Nats-Msg-Id (PublishCE
			// mints a fresh CE each call, so dedup does not apply across
			// separate publishMaxDeliverError calls). Not worth retrying
			// Term itself — it would race the same connection issue — but
			// worth logging so operators can see it happened.
			if err := msg.Term(); err != nil {
				c.logMessageResolutionFailure("failed to terminate max-delivery message, may be redelivered", msg, err)
			}
			return
		}
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if c.cfg.HandlerTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), c.cfg.HandlerTimeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	if err := c.mainHandler(ctx, msg.Data()); err != nil {
		if nakErr := msg.NakWithDelay(c.nakDelay()); nakErr != nil {
			c.logMessageResolutionFailure("failed to nak main message", msg, nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		c.logMessageResolutionFailure("failed to ack main message, may be redelivered", msg, err)
	}
}

func (c *Client) nakDelay() time.Duration {
	if c.cfg.NakDelay > 0 {
		return c.cfg.NakDelay
	}
	return routing.DefaultNakDelay
}

func (c *Client) publishMaxDeliverError(msg jetstream.Msg) {
	resourceID, ceType := extractCEFields(msg.Data())
	ceID, _ := extractCEIdentity(msg.Data())
	// When resourceID is empty/unknown (malformed inbound message), the CP
	// silently drops the response error CE we're about to publish below
	// (it requires a non-empty resource_id to correlate). Log the CE id and
	// stream/consumer sequence here so an operator can still correlate the
	// incident from agent-side logs against NATS stream state, even though
	// nothing reaches the CP for it.
	attrs := []any{"resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject()}
	if meta, metaErr := msg.Metadata(); metaErr == nil {
		attrs = append(attrs, "stream_seq", meta.Sequence.Stream, "consumer_seq", meta.Sequence.Consumer)
	}
	c.logger.Warn("max delivery exceeded, terminating message", attrs...)

	errData := routing.ErrorData{
		ResponseContext: routing.ResponseContext{
			ResourceID: resourceID,
			AgentName:  c.cfg.AgentName,
			TopicName:  c.topics.Main,
		},
		Error:   routing.ErrorMaxDeliveryExceeded,
		Details: "max delivery attempts exceeded",
	}
	if err := cloudevent.PublishCE(context.Background(), c.PublishWithMsgID, cloudevent.SubjectResponses, c.cfg.AgentName, cloudevent.TypeError, errData); err != nil {
		c.logger.Warn("failed to publish max-deliver error CE", "error", err, "resource_id", resourceID)
	}
}

func (c *Client) logMessageResolutionFailure(msgText string, msg jetstream.Msg, err error) {
	ceID, ceType := extractCEIdentity(msg.Data())
	attrs := make([]any, 0, 14)
	attrs = append(attrs, "error", err, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject())
	if meta, metaErr := msg.Metadata(); metaErr == nil {
		attrs = append(attrs,
			"stream_seq", meta.Sequence.Stream,
			"consumer_seq", meta.Sequence.Consumer,
			"num_delivered", meta.NumDelivered,
		)
	} else {
		attrs = append(attrs, "meta_error", metaErr)
	}
	c.logger.Warn(msgText, attrs...)
}

func extractCEIdentity(data []byte) (id, ceType string) {
	var envelope struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.ID == "" {
		return "unknown", "unknown"
	}
	if envelope.Type == "" {
		envelope.Type = "unknown"
	}
	return envelope.ID, envelope.Type
}

func extractCEFields(data []byte) (resourceID, ceType string) {
	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return "unknown", "unknown"
	}
	var payload struct {
		ResourceID string `json:"resource_id"`
	}
	if json.Unmarshal(envelope.Data, &payload) != nil {
		return "unknown", envelope.Type
	}
	return payload.ResourceID, envelope.Type
}
