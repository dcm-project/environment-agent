package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

// SPResponseError carries the status and message returned by a provider.
type SPResponseError struct {
	StatusCode int
	Message    string
}

func (e *SPResponseError) Error() string {
	return fmt.Sprintf("%d %s", e.StatusCode, e.Message)
}

func errorStatusCode(err error) (int, bool) {
	var spErr *SPResponseError
	if errors.As(err, &spErr) {
		return spErr.StatusCode, true
	}
	var fwdErr *forwarderError
	if errors.As(err, &fwdErr) {
		return fwdErr.statusCode, true
	}
	return 0, false
}

// IsRetryable returns true if the error should trigger a retry.
// Plain errors (connection failures) are retryable. Status errors with 5xx/429 are retryable.
// HTTP 408 is NOT retryable per REQ-RTE-111 (4xx except 429).
func IsRetryable(err error) bool {
	statusCode, hasStatus := errorStatusCode(err)
	if !hasStatus {
		return true
	}
	return statusCode >= 500 || statusCode == http.StatusTooManyRequests
}

// SafeErrorAttrs returns slog key-value attributes describing err without
// leaking provider response bodies or request URLs. Status-bearing errors
// contribute only their HTTP status code, URL errors use a generic message,
// and other errors are logged as-is.
func SafeErrorAttrs(err error) []any {
	if statusCode, hasStatus := errorStatusCode(err); hasStatus {
		return []any{"http_status", statusCode}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return []any{"error", "provider HTTP request failed"}
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

// ProviderErrorData contains structured details from an SP response.
type ProviderErrorData struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"message"`
}

// ErrorDetails is the details object in an error CE.
type ErrorDetails struct {
	Message       string             `json:"message"`
	ProviderError *ProviderErrorData `json:"provider_error,omitempty"`
}

// ErrorData is the CE payload for error events.
type ErrorData struct {
	ResponseContext
	Error   string       `json:"error"`
	Details ErrorDetails `json:"details"`
}

// TerminalProviderErrorData builds the error payload used after forwarding
// stops because the provider rejected the request or retries were exhausted.
func TerminalProviderErrorData(err error, serviceType string, responseContext ResponseContext) ErrorData {
	var providerError *ProviderErrorData
	var spErr *SPResponseError
	if errors.As(err, &spErr) {
		providerError = &ProviderErrorData{StatusCode: spErr.StatusCode, Message: spErr.Message}
	}

	if IsRetryable(err) {
		return ErrorData{
			ResponseContext: responseContext,
			Error:           ErrorRetryExhausted,
			Details: ErrorDetails{
				Message:       "service provider error after retry exhaustion for service type: " + serviceType,
				ProviderError: providerError,
			},
		}
	}
	return ErrorData{
		ResponseContext: responseContext,
		Error:           ErrorNonRetryable,
		Details: ErrorDetails{
			Message:       "service provider returned non-retryable error for service type: " + serviceType,
			ProviderError: providerError,
		},
	}
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
