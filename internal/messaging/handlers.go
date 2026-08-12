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
	// Extraction must happen before the defer so the recover closure below
	// can reference resourceID/ceID/ceType (a := declared after a defer
	// statement is not visible inside a closure written earlier).
	resourceID, ceID, ceType := extractLogFields(msg.Data())
	defer func() {
		if r := recover(); r != nil {
			nakErr := msg.NakWithDelay(c.nakDelay())
			attrs := []any{
				"panic", r, "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType,
				"subject", msg.Subject(), "stack", string(debug.Stack()),
			}
			if nakErr != nil {
				attrs = append(attrs, "nak_error", nakErr)
			}
			c.logger.Error("panic in cancel message handler", attrs...)
		}
	}()

	c.logMessageReceived(msg, resourceID, ceID, ceType)

	ctx, cancel := handlerContext(c.cfg.CancelHandlerTimeout)
	defer cancel()

	if err := c.cancelHandler(ctx, msg.Data()); err != nil {
		if nakErr := msg.NakWithDelay(c.nakDelay()); nakErr != nil {
			c.logMessageResolutionFailure("failed to nak cancel message", msg, nakErr)
			return
		}
		c.logger.Warn("cancel message nacked, handler failed", "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject(), "error", err)
		return
	}
	if err := msg.Ack(); err != nil {
		c.logMessageResolutionFailure("failed to ack cancel message, may be redelivered", msg, err)
		return
	}
	c.logger.Info("cancel message acked", "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject())
}

func (c *Client) handleMainMessage(msg jetstream.Msg) {
	// Extraction must happen before the defer so the recover closure below
	// can reference resourceID/ceID/ceType (a := declared after a defer
	// statement is not visible inside a closure written earlier).
	resourceID, ceID, ceType := extractLogFields(msg.Data())
	defer func() {
		if r := recover(); r != nil {
			nakErr := msg.NakWithDelay(c.nakDelay())
			attrs := []any{
				"panic", r, "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType,
				"subject", msg.Subject(), "stack", string(debug.Stack()),
			}
			if nakErr != nil {
				attrs = append(attrs, "nak_error", nakErr)
			}
			c.logger.Error("panic in main message handler", attrs...)
		}
	}()

	c.logMessageReceived(msg, resourceID, ceID, ceType)

	if c.cfg.MaxDeliver > 0 {
		meta, err := msg.Metadata()
		if err != nil {
			nakErr := msg.Nak()
			c.logger.Warn("failed to get message metadata for MaxDeliver guard",
				"error", err, "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "nak_error", nakErr)
			return
		}
		if meta.NumDelivered >= uint64(c.cfg.MaxDeliver) {
			c.publishMaxDeliverError(msg)
			// If Term fails, JetStream redelivers and this guard fires again,
			// publishing a duplicate terminal error CE (IT-RCM-080). Log
			// rather than retry Term, since it would race the same issue.
			if err := msg.Term(); err != nil {
				c.logMessageResolutionFailure("failed to terminate max-delivery message, may be redelivered", msg, err)
			}
			return
		}
	}

	ctx, cancel := handlerContext(c.cfg.HandlerTimeout)
	defer cancel()

	if err := c.mainHandler(ctx, msg.Data()); err != nil {
		if nakErr := msg.NakWithDelay(c.nakDelay()); nakErr != nil {
			c.logMessageResolutionFailure("failed to nak main message", msg, nakErr)
			return
		}
		c.logger.Warn("main message nacked, handler failed", "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject(), "error", err)
		return
	}
	if err := msg.Ack(); err != nil {
		c.logMessageResolutionFailure("failed to ack main message, may be redelivered", msg, err)
		return
	}
	c.logger.Info("main message acked", "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject())
}

// handlerContext bounds handler execution so a hung SP/downstream call can't
// block a consumer indefinitely (REQ-RCM-180); falls back to a cancellable,
// deadline-free context when timeout is unset.
func handlerContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithCancel(context.Background())
}

func (c *Client) nakDelay() time.Duration {
	if c.cfg.NakDelay > 0 {
		return c.cfg.NakDelay
	}
	return routing.DefaultNakDelay
}

// logMessageReceived logs the receipt of a message before any handling is
// attempted, so a message's arrival is auditable even if the handler later
// panics or the process crashes before resolution.
func (c *Client) logMessageReceived(msg jetstream.Msg, resourceID, ceID, ceType string) {
	attrs := []any{"resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject()}
	if meta, err := msg.Metadata(); err == nil {
		attrs = append(attrs,
			"stream_seq", meta.Sequence.Stream,
			"consumer_seq", meta.Sequence.Consumer,
			"num_delivered", meta.NumDelivered,
		)
	} else {
		attrs = append(attrs, "meta_error", err)
	}
	c.logger.Info("message received", attrs...)
}

func (c *Client) publishMaxDeliverError(msg jetstream.Msg) {
	resourceID, ceID, ceType := extractLogFields(msg.Data())
	// Log the CE id and stream/consumer sequence so an operator can still
	// correlate the incident even when resourceID is empty and the CP drops
	// the error CE published below (it requires a non-empty resource_id).
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
		c.logger.Warn("failed to publish max-deliver error CE", "error", err, "resource_id", resourceID, "ce_id", ceID, "published_ce_type", cloudevent.TypeError)
		return
	}
	c.logger.Info("published max-deliver error CE", "resource_id", resourceID, "ce_id", ceID, "published_ce_type", cloudevent.TypeError)
}

func (c *Client) logMessageResolutionFailure(msgText string, msg jetstream.Msg, err error) {
	resourceID, ceID, ceType := extractLogFields(msg.Data())
	attrs := make([]any, 0, 14)
	attrs = append(attrs, "error", err, "resource_id", resourceID, "ce_id", ceID, "ce_type", ceType, "subject", msg.Subject())
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

// extractLogFields performs a single unmarshal of a CloudEvent envelope to
// recover the fields needed for correlation in log entries. It never returns
// an error: ce_id/ce_type fall back to "unknown" on any parse failure so a
// malformed message can still be logged and traced. resource_id is only
// ever populated from a successfully-parsed payload; it is "" whenever it's
// unavailable for any reason (malformed envelope, malformed data, or a
// genuinely absent field) — never "unknown" — since downstream consumers
// (e.g. publishMaxDeliverError's outbound ErrorData.ResourceID) treat
// resource_id="" as "no resource identified", not as an error marker.
func extractLogFields(data []byte) (resourceID, ceID, ceType string) {
	var envelope struct {
		ID   string          `json:"id"`
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return "", "unknown", "unknown"
	}
	ceID, ceType = envelope.ID, envelope.Type
	if ceID == "" {
		ceID = "unknown"
	}
	if ceType == "" {
		ceType = "unknown"
	}
	var payload struct {
		ResourceID string `json:"resource_id"`
	}
	_ = json.Unmarshal(envelope.Data, &payload)
	return payload.ResourceID, ceID, ceType
}
