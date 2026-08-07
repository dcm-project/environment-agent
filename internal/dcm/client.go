package dcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/dcm-project/environment-agent/api/v1alpha1"
)

var ErrRateLimited = errors.New("dcm rate limited")

// RateLimitError is returned on HTTP 429 responses.
type RateLimitError struct {
	RetryAfter    time.Duration
	HasRetryAfter bool
}

func (e *RateLimitError) Error() string { return "dcm rate limited" }
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

type registrationPayload struct {
	Name         string   `json:"name"`
	Environment  string   `json:"environment"`
	Cost         string   `json:"cost"`
	TopicName    string   `json:"topic_name"`
	ServiceTypes []string `json:"service_types"`
	// ResourcesAvailable is sent for forward-compatibility, but currently
	// ignored by the control plane (its OpenAPI schema doesn't model this
	// field). Kept omitempty so absence is silent, not an error.
	ResourcesAvailable *v1alpha1.ResourceCapacity `json:"resources_available,omitempty"`
}

type heartbeatPayload struct {
	Timestamp   time.Time `json:"timestamp"`
	ConsumerLag int64     `json:"consumer_lag"`
}

type registrationResponse struct {
	AgentID string `json:"agent_id"`
}

// ponytail: hand-rolled HTTP client — replace with generated control-plane client
// once the DCM agent registration OpenAPI spec is published and a Go client is generated.
type dcmClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

func newDCMClient(registrationURL string) (*dcmClient, error) {
	u, err := url.Parse(registrationURL)
	if err != nil {
		return nil, fmt.Errorf("invalid registration URL: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid registration URL: missing host")
	}
	return &dcmClient{
		baseURL:    u,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (c *dcmClient) register(ctx context.Context, payload registrationPayload) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal registration payload: %w", err)
	}

	endpoint := c.baseURL.JoinPath("api", "v1alpha1", "agents").String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("registration request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on HTTP response

	return c.handleRegistrationResponse(resp)
}

func (c *dcmClient) handleRegistrationResponse(resp *http.Response) (string, error) {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var result registrationResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", fmt.Errorf("decode registration response: %w", err)
		}
		if result.AgentID == "" {
			return "", fmt.Errorf("DCM returned empty agent_id")
		}
		return result.AgentID, nil
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		rle := &RateLimitError{}
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if d, ok := ParseRetryAfter(ra, time.Now()); ok {
				rle.RetryAfter = d
				rle.HasRetryAfter = true
			}
		}
		return "", rle
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return "", fmt.Errorf("registration rejected (HTTP %d): %w", resp.StatusCode, ErrNonRetryable)
	}

	return "", fmt.Errorf("registration failed (HTTP %d)", resp.StatusCode)
}

func (c *dcmClient) heartbeat(ctx context.Context, agentID string, payload heartbeatPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal heartbeat payload: %w", err)
	}

	// ponytail: agentID from trusted DCM control plane — no path-traversal sanitization needed beyond empty check above
	endpoint := c.baseURL.JoinPath("api", "v1alpha1", "agents", agentID, "heartbeat").String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create heartbeat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("heartbeat request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on HTTP response

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("heartbeat failed (HTTP %d)", resp.StatusCode)
}
