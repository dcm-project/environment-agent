package monitoring

import (
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/google/uuid"
)

// statusEventData is the CloudEvent data payload for network status updates
// (matches the enhancement NetworkStatus shape).
type statusEventData struct {
	ID         string         `json:"id"`
	Status     string         `json:"status"`
	Message    string         `json:"message"`
	OutputSpec map[string]any `json:"output_spec,omitempty"`
}

// NewStatusCloudEvent constructs a CloudEvents v1.0 JSON payload for a status
// change notification using the CloudEvents SDK.
func NewStatusCloudEvent(subject, providerName, instanceID string, status v1alpha1.NetworkStatus, message string, outputSpec map[string]any) ([]byte, error) {
	event := cloudevents.NewEvent()
	event.SetID(uuid.NewString())
	event.SetSource("dcm/providers/" + providerName)
	event.SetType("dcm.status.network")
	event.SetSubject(subject)
	event.SetTime(time.Now().UTC())
	if err := event.SetData(cloudevents.ApplicationJSON, statusEventData{
		ID:         instanceID,
		Status:     string(status),
		Message:    message,
		OutputSpec: outputSpec,
	}); err != nil {
		return nil, fmt.Errorf("setting cloud event data: %w", err)
	}

	data, err := event.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshaling cloud event: %w", err)
	}
	return data, nil
}
