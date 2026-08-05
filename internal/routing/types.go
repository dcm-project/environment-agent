package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// ErrNotImplemented is returned by stubs during RED phase.
var ErrNotImplemented = errors.New("not implemented")

// CE error string constants for deterministic test assertions.
const (
	ErrorUnsupportedServiceType = "UNSUPPORTED_SERVICE_TYPE"
	ErrorSPUnavailable          = "SP_UNAVAILABLE"
	ErrorRetryExhausted         = "RETRY_EXHAUSTED"
	ErrorNonRetryable           = "NON_RETRYABLE_SP_ERROR"
	ErrorInvalidPayload         = "INVALID_PAYLOAD"
)

// SPForwarder abstracts SP dispatch with typed operation contracts.
type SPForwarder interface {
	CreateResource(ctx context.Context, endpoint string, embedded bool, req CreateResourceRequest) error
	DeleteResource(ctx context.Context, endpoint string, embedded bool, resourceID string) error
}

// Publisher abstracts NATS publish for response/retry CEs.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// RetryTopicConsumer abstracts retry topic operations for cancel handling.
type RetryTopicConsumer interface {
	FetchRetryMessages(ctx context.Context) ([]RetryMessage, error)
	RepublishToRetry(ctx context.Context, data []byte) error
}

// RetryMessage wraps a message fetched from the retry topic.
type RetryMessage struct {
	Data        []byte
	ResourceID  string
	ServiceType string
	AckFunc     func() error
}

// CreateResourceRequest is the typed payload for creation forwarding.
type CreateResourceRequest struct {
	ResourceID  string
	ServiceType string
	Spec        json.RawMessage
}

// SPResponseError represents an error response from a service provider.
type SPResponseError struct {
	StatusCode int
	Message    string
}

func (e *SPResponseError) Error() string {
	return fmt.Sprintf("%d %s", e.StatusCode, e.Message)
}

// IsRetryable returns true if the error should trigger a retry.
// Plain errors (connection failures) are retryable. SPResponseError with 5xx/429 are retryable.
// HTTP 408 is NOT retryable per REQ-RTE-111 (4xx except 429).
func IsRetryable(err error) bool {
	var spErr *SPResponseError
	if !errors.As(err, &spErr) {
		return true
	}
	return spErr.StatusCode >= 500 || spErr.StatusCode == http.StatusTooManyRequests
}

// CreationAckData is the CE payload for creation-acknowledged events.
type CreationAckData struct {
	ResourceID string `json:"resourceId"`
	AgentName  string `json:"agentName"`
	TopicName  string `json:"topicName"`
	Status     string `json:"status"`
}

// DeletionAckData is the CE payload for deletion-acknowledged events.
type DeletionAckData struct {
	ResourceID string `json:"resourceId"`
	AgentName  string `json:"agentName"`
	TopicName  string `json:"topicName"`
	Status     string `json:"status"`
}

// RequestQueuedData is the CE payload for request-queued events.
type RequestQueuedData struct {
	ResourceID  string `json:"resourceId"`
	AgentName   string `json:"agentName"`
	TopicName   string `json:"topicName"`
	ServiceType string `json:"serviceType"`
	Status      string `json:"status"`
}

// ErrorData is the CE payload for error events.
type ErrorData struct {
	ResourceID string `json:"resourceId"`
	AgentName  string `json:"agentName"`
	TopicName  string `json:"topicName"`
	Error      string `json:"error"`
	Details    string `json:"details"`
}

// CancelAckData is the CE payload for cancel-acknowledged events.
type CancelAckData struct {
	ResourceID  string `json:"resourceId"`
	AgentName   string `json:"agentName"`
	TopicName   string `json:"topicName"`
	ServiceType string `json:"serviceType"`
}

// CancelRejectedData is the CE payload for cancel-rejected events.
type CancelRejectedData struct {
	ResourceID string `json:"resourceId"`
	AgentName  string `json:"agentName"`
	TopicName  string `json:"topicName"`
	Reason     string `json:"reason"`
}

type inboundPayload struct {
	ResourceID  string          `json:"resourceId"`
	ServiceType string          `json:"serviceType"`
	Spec        json.RawMessage `json:"spec,omitempty"`
}
