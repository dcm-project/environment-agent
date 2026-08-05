package cloudevent

import (
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// PublishCE constructs a CloudEvent and publishes it via the provided function.
// The publishFn parameter avoids defining a Publisher interface in this package.
func PublishCE(ctx context.Context, publishFn func(context.Context, string, []byte) error, subject, agentName, ceType string, data any) error {
	event, err := NewCloudEvent(agentName, ceType)
	if err != nil {
		return fmt.Errorf("failed to create CE: %w", err)
	}
	if err := event.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return fmt.Errorf("failed to set CE data: %w", err)
	}
	bytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal CE: %w", err)
	}
	return publishFn(ctx, subject, bytes)
}
