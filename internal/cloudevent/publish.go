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
// Each call generates a fresh CE id, so a caller retrying a failed publish
// must retry PublishWithMsgID directly with the already-built id/bytes, not
// re-invoke PublishCE, or dedup won't apply.
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
