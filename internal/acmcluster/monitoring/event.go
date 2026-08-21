package monitoring

import (
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/google/uuid"
)

// CloudEvent represents a CloudEvents v1.0 compliant event.
type CloudEvent struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	Time            string `json:"time"`
	Subject         string `json:"subject"`
	DataContentType string `json:"datacontenttype"`
	Data            any    `json:"data"`
}

// StatusPayload is the data portion of a status CloudEvent.
type StatusPayload struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// NewStatusCloudEvent creates a CloudEvent for a status change.
func NewStatusCloudEvent(providerName, instanceID string, status v1alpha1.ClusterStatus, message string) (*CloudEvent, error) {
	return &CloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.New().String(),
		Source:          fmt.Sprintf("dcm/providers/%s", providerName),
		Type:            "dcm.status.cluster",
		Subject:         "dcm.cluster",
		Time:            time.Now().UTC().Format(time.RFC3339),
		DataContentType: "application/json",
		Data:            StatusPayload{ID: instanceID, Status: string(status), Message: message},
	}, nil
}
