package messaging

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const nakDelay = 500 * time.Millisecond

func (c *Client) handleCancelMessage(msg jetstream.Msg) {
	if err := c.cancelHandler(context.Background(), msg.Data()); err != nil {
		if nakErr := msg.NakWithDelay(nakDelay); nakErr != nil {
			c.logMessageResolutionFailure("failed to nak cancel message", msg, nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		c.logMessageResolutionFailure("failed to ack cancel message, may be redelivered", msg, err)
	}
}

func (c *Client) handleMainMessage(msg jetstream.Msg) {
	if err := c.mainHandler(context.Background(), msg.Data()); err != nil {
		if nakErr := msg.NakWithDelay(nakDelay); nakErr != nil {
			c.logMessageResolutionFailure("failed to nak main message", msg, nakErr)
		}
		return
	}
	if err := msg.Ack(); err != nil {
		c.logMessageResolutionFailure("failed to ack main message, may be redelivered", msg, err)
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
