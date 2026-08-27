package monitoring

import (
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/google/uuid"
)

// statusEventData is the CloudEvent data payload for storage status updates
// (matches the enhancement StorageStatus shape).
type statusEventData struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// NewStatusCloudEvent constructs a CloudEvents v1.0 JSON payload for a status
// change notification using the CloudEvents SDK.
func NewStatusCloudEvent(subject, providerName, instanceID string, status v1alpha1.StorageStatus, message string) ([]byte, error) {
	event := cloudevents.NewEvent()
	event.SetID(uuid.NewString())
	event.SetSource("dcm/providers/" + providerName)
	event.SetType("dcm.status.storage")
	event.SetSubject(subject)
	event.SetTime(time.Now().UTC())
	if err := event.SetData(cloudevents.ApplicationJSON, statusEventData{
		ID:      instanceID,
		Status:  string(status),
		Message: message,
	}); err != nil {
		return nil, fmt.Errorf("setting cloud event data: %w", err)
	}

	data, err := event.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshaling cloud event: %w", err)
	}
	return data, nil
}
