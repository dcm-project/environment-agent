// Package cloudevent provides CloudEvent v1.0 construction utilities.
package cloudevent

import (
	"errors"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
)

// NewCloudEvent creates a CloudEvents v1.0 event with standard agent attributes.
func NewCloudEvent(agentID, eventType string) (cloudevents.Event, error) {
	source, err := FormatSource(agentID)
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("failed to format source: %w", err)
	}
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource(source)
	event.SetType(eventType)
	event.SetTime(time.Now().UTC())
	return event, nil
}

// FormatSource formats the CloudEvent source attribute for an agent.
func FormatSource(agentID string) (string, error) {
	if agentID == "" {
		return "", errors.New("agentID must not be empty")
	}
	return fmt.Sprintf("dcm/agents/%s", agentID), nil
}
