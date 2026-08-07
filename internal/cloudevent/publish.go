package cloudevent

import (
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// PublishCE constructs a CloudEvent and publishes it via the provided function,
// passing the CE's own id as the NATS message ID (JetStream dedup, F34). The
// publishFn parameter avoids defining a Publisher interface in this package.
//
// NOTE: each call generates a fresh CE id (see NewCloudEvent), so calling
// PublishCE again after a failed attempt does NOT dedupe against the first
// attempt — it produces a distinct Nats-Msg-Id. There is no publish-retry
// today; if one is added, it must retry PublishWithMsgID directly with the
// already-built (subject, id, bytes) rather than re-invoking PublishCE, or
// dedup will silently not apply.
func PublishCE(ctx context.Context, publishFn func(context.Context, string, string, []byte) error, subject, agentName, ceType string, data any) error {
	event, err := NewCloudEvent(agentName, ceType)
	if err != nil {
		return fmt.Errorf("failed to create CE: %w", err)
	}
	// Set subject to the NATS subject being published to, matching the
	// control-plane's own outbound CE envelope shape (item #5 of the
	// CP/agent alignment review).
	event.SetSubject(subject)
	if err := event.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return fmt.Errorf("failed to set CE data: %w", err)
	}
	bytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal CE: %w", err)
	}
	return publishFn(ctx, subject, event.ID(), bytes)
}
