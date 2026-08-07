package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// DefaultNakDelay is the fallback NakWithDelay duration when no explicit
// NakDelay is configured. Used by both the messaging client and retry processor.
const DefaultNakDelay = 500 * time.Millisecond

// CE error string constants for deterministic test assertions.
const (
	ErrorUnsupportedServiceType = "UNSUPPORTED_SERVICE_TYPE"
	ErrorSPUnavailable          = "SP_UNAVAILABLE"
	ErrorRetryExhausted         = "RETRY_EXHAUSTED"
	ErrorNonRetryable           = "NON_RETRYABLE_SP_ERROR"
	ErrorInvalidPayload         = "INVALID_PAYLOAD"
	ErrorMaxDeliveryExceeded    = "MAX_DELIVERY_EXCEEDED"
)

// SPForwarder abstracts SP dispatch with typed operation contracts.
type SPForwarder interface {
	CreateResource(ctx context.Context, endpoint string, embedded bool, req CreateResourceRequest) error
	DeleteResource(ctx context.Context, endpoint string, embedded bool, req DeleteResourceRequest) error
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
	EventID     string // CE id, forwarded as Idempotency-Key (REQ-RCM-210)
}

// DeleteResourceRequest is the typed payload for deletion forwarding.
type DeleteResourceRequest struct {
	ResourceID  string
	ServiceType string
	EventID     string // CE id, forwarded as Idempotency-Key (REQ-RCM-210)
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

// ResponseContext holds the common fields shared by all response CE payloads.
type ResponseContext struct {
	ResourceID string `json:"resourceId"`
	AgentName  string `json:"agentName"`
	TopicName  string `json:"topicName"`
}

// CreationAckData is the CE payload for creation-acknowledged events.
type CreationAckData struct {
	ResponseContext
	Status string `json:"status"`
}

// DeletionAckData is the CE payload for deletion-acknowledged events.
type DeletionAckData struct {
	ResponseContext
	Status string `json:"status"`
}

// RequestQueuedData is the CE payload for request-queued events.
type RequestQueuedData struct {
	ResponseContext
	ServiceType string `json:"serviceType"`
	Status      string `json:"status"`
}

// ErrorData is the CE payload for error events.
type ErrorData struct {
	ResponseContext
	Error   string `json:"error"`
	Details string `json:"details"`
}

// CancelAckData is the CE payload for cancel-acknowledged events.
type CancelAckData struct {
	ResponseContext
	ServiceType string `json:"serviceType"`
}

// CancelRejectedData is the CE payload for cancel-rejected events.
type CancelRejectedData struct {
	ResponseContext
	Reason string `json:"reason"`
}

// HealthEventData is the CE payload for health degraded/unavailable events (REQ-HMN-120, REQ-HMN-145).
type HealthEventData struct {
	AgentID          string `json:"agentId"`
	AgentName        string `json:"agentName"`
	TopicName        string `json:"topicName"`
	ServiceType      string `json:"serviceType"`
	Reason           string `json:"reason"`
	AffectedProvider string `json:"affectedProvider"`
}

type inboundPayload struct {
	ResourceID  string          `json:"resourceId"`
	ServiceType string          `json:"serviceType"`
	Spec        json.RawMessage `json:"spec,omitempty"`
	EventID     string
}
