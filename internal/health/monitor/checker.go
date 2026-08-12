package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// Checker performs a health check for a single provider.
type Checker interface {
	Check(ctx context.Context) HealthCheckResult
}

// ExternalChecker polls an external SP's /health endpoint via HTTP.
type ExternalChecker struct {
	endpoint string
	client   *http.Client
}

// NewExternalChecker creates an ExternalChecker for the given SP endpoint.
func NewExternalChecker(endpoint string) *ExternalChecker {
	return &ExternalChecker{endpoint: strings.TrimRight(endpoint, "/"), client: &http.Client{}}
}

func (c *ExternalChecker) Check(ctx context.Context) HealthCheckResult {
	healthURL, err := url.JoinPath(c.endpoint, "health")
	if err != nil {
		return CheckFailed
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return CheckFailed
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return CheckFailed
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return CheckFailed
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return CheckFailed
	}
	switch strings.ToLower(body.Status) {
	case "healthy":
		return CheckHealthy
	case "unhealthy":
		return CheckUnhealthy
	default:
		return CheckFailed
	}
}

// EmbeddedChecker performs an in-process health check.
type EmbeddedChecker struct {
	checkFn func() HealthCheckResult
}

// NewEmbeddedChecker creates an EmbeddedChecker backed by checkFn.
func NewEmbeddedChecker(checkFn func() HealthCheckResult) *EmbeddedChecker {
	return &EmbeddedChecker{checkFn: checkFn}
}

func (c *EmbeddedChecker) Check(_ context.Context) HealthCheckResult {
	return c.checkFn()
}

// DefaultEmbeddedCheckFn returns a check function that reads health from
// AGENT_EMBEDDED_SP_{TYPE}_HEALTH. Unset/empty → CheckHealthy.
func DefaultEmbeddedCheckFn(serviceType string) func() HealthCheckResult {
	key := "AGENT_EMBEDDED_SP_" + strings.ToUpper(serviceType) + "_HEALTH"
	return func() HealthCheckResult {
		val := strings.ToLower(os.Getenv(key))
		switch val {
		case "", "healthy":
			return CheckHealthy
		case "unhealthy":
			return CheckUnhealthy
		default:
			return CheckUnhealthy
		}
	}
}
