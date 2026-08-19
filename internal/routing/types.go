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
// NakDelay is configured. Used by messaging.Client for the main and cancel
// subjects; the retry processor uses its own fixed storeErrorRetryDelay.
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
	// PublishWithMsgID publishes with a JetStream Nats-Msg-Id header for
	// server-side dedup, using the CE's own id. Used for response CEs so that
	// a future publish-retry mechanism can safely re-publish without risking
	// duplicate delivery to the control-plane (REQ-MSG-135).
	PublishWithMsgID(ctx context.Context, subject, msgID string, data []byte) error
}

// RetryTopicConsumer abstracts retry topic operations for cancel handling.
type RetryTopicConsumer interface {
	FetchRetryMessages(ctx context.Context) ([]RetryMessage, error)
}

// RetryMessage wraps a message fetched from the retry topic.
type RetryMessage struct {
	Data        []byte
	ResourceID  string
	ServiceType string
	AckFunc     func() error
	// NakFunc negatively acknowledges this message in place (same JetStream
	// message) so it's redelivered later. Used instead of ack+republish for
	// non-cancelled messages, since they already live on this stream and
	// don't need to move. The retry-subject consumer has no MaxDeliver limit
	// (DD-410), so this choice is about simplicity, not delivery-count
	// preservation.
	NakFunc func() error
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

// SafeErrorAttrs returns slog key-value attributes describing err without
// leaking a wrapped SP response body: an *SPResponseError contributes only
// its HTTP status code, any other error is logged as-is.
func SafeErrorAttrs(err error) []any {
	var spErr *SPResponseError
	if errors.As(err, &spErr) {
		return []any{"http_status", spErr.StatusCode}
	}
	return []any{"error", err}
}

// ResponseContext holds the common fields shared by all response CE payloads.
// Field names use snake_case (AEP convention) to match the control-plane's
// CE data structs. Only ResourceID is consumed by the control-plane today —
// AgentName/TopicName are informational/diagnostic.
type ResponseContext struct {
	ResourceID string `json:"resource_id"`
	AgentName  string `json:"agent_name"`
	TopicName  string `json:"topic_name"`
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
	ServiceType string `json:"service_type"`
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
	ServiceType string `json:"service_type"`
}

// CancelRejectedData is the CE payload for cancel-rejected events.
type CancelRejectedData struct {
	ResponseContext
	Reason string `json:"reason"`
}

// HealthEventData is the CE payload for health degraded/unavailable events (REQ-HMN-120, REQ-HMN-145).
type HealthEventData struct {
	AgentID          string `json:"agent_id"`
	AgentName        string `json:"agent_name"`
	TopicName        string `json:"topic_name"`
	ServiceType      string `json:"service_type"`
	Reason           string `json:"reason"`
	AffectedProvider string `json:"affected_provider"`
}

// inboundPayload mirrors the control-plane's CreatePayload/DeletePayload/CancelPayload
// (snake_case, AEP convention). Go's encoding/json does not fold underscores,
// so these tags must match the control-plane's wire format exactly.
type inboundPayload struct {
	ResourceID  string          `json:"resource_id"`
	ServiceType string          `json:"service_type"`
	Spec        json.RawMessage `json:"spec,omitempty"`
	EventID     string
}
