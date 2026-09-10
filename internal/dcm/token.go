package dcm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TokenSource provides bearer tokens for outbound HTTP requests to the
// control plane. Implementations must be safe for concurrent use.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// StaticTokenSource returns a fixed token on every call. Suitable for
// dev/simple deployments where a pre-obtained JWT is configured via
// DCM_AUTH_TOKEN.
type StaticTokenSource struct {
	token string
}

// NewStaticTokenSource creates a TokenSource that always returns the
// given token.
func NewStaticTokenSource(token string) *StaticTokenSource {
	return &StaticTokenSource{token: token}
}

func (s *StaticTokenSource) Token(_ context.Context) (string, error) {
	return s.token, nil
}

// defaultExpiryDelta is the safety buffer subtracted from expires_in to
// trigger proactive token refresh before the actual expiry.
const defaultExpiryDelta = 10 * time.Second

// ClientCredentialsTokenSource obtains tokens via the OAuth2
// client_credentials grant. Tokens are cached and refreshed proactively
// before expiry.
type ClientCredentialsTokenSource struct {
	tokenEndpoint string
	clientID      string
	clientSecret  string
	httpClient    *http.Client
	expiryDelta   time.Duration

	mu          sync.Mutex
	accessToken string
	expiry      time.Time
}

// NewClientCredentialsTokenSource creates a TokenSource that fetches
// tokens from the given OIDC token endpoint using client_credentials.
// If httpClient is nil, a default client with a 10s timeout is used.
func NewClientCredentialsTokenSource(tokenEndpoint, clientID, clientSecret string, httpClient *http.Client) *ClientCredentialsTokenSource {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &ClientCredentialsTokenSource{
		tokenEndpoint: tokenEndpoint,
		clientID:      clientID,
		clientSecret:  clientSecret,
		httpClient:    httpClient,
		expiryDelta:   defaultExpiryDelta,
	}
}

func (c *ClientCredentialsTokenSource) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.accessToken != "" && time.Now().Before(c.expiry.Add(-c.expiryDelta)) {
		return c.accessToken, nil
	}

	token, expiry, err := c.fetchToken(ctx)
	if err != nil {
		return "", fmt.Errorf("fetching client-credentials token: %w", err)
	}
	c.accessToken = token
	c.expiry = expiry
	return token, nil
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

func (c *ClientCredentialsTokenSource) fetchToken(ctx context.Context) (string, time.Time, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort close on HTTP response

	if resp.StatusCode != http.StatusOK {
		// Drain a bounded amount of the body before the deferred close so the
		// underlying connection can be reused (keep-alive) by the next retry
		// instead of forcing a fresh connection + TLS handshake on every
		// authentication failure.
		_, _ = io.CopyN(io.Discard, resp.Body, maxDrainBytes)
		return "", time.Time{}, fmt.Errorf("token endpoint returned HTTP %d", resp.StatusCode)
	}

	var tokenResp tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", time.Time{}, fmt.Errorf("decode token response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("token endpoint returned empty access_token")
	}

	ttl, err := validateExpiresIn(tokenResp.ExpiresIn)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("token endpoint returned invalid expires_in: %w", err)
	}

	expiry := time.Now().Add(ttl)
	return tokenResp.AccessToken, expiry, nil
}

// maxDrainBytes bounds how much of a non-200 token response body is read
// before close, purely to allow HTTP keep-alive connection reuse — not a
// diagnostic budget, so it can stay small.
const maxDrainBytes = 4096

// maxExpiresInSeconds is the largest expires_in (in whole seconds) that can be
// multiplied by time.Second and added to a time.Time without overflowing
// time.Duration's underlying int64 nanoseconds (REQ-DCM-221).
const maxExpiresInSeconds = int64(math.MaxInt64) / int64(time.Second)

// validateExpiresIn rejects a token response's expires_in value that would
// defeat caching (missing/zero/negative — the token would already be treated
// as expired) or overflow time.Duration when converted (REQ-DCM-221).
func validateExpiresIn(expiresIn int64) (time.Duration, error) {
	if expiresIn <= 0 {
		return 0, fmt.Errorf("expires_in must be positive, got %d", expiresIn)
	}
	if expiresIn > maxExpiresInSeconds {
		return 0, fmt.Errorf("expires_in %d seconds overflows time.Duration (max %d)", expiresIn, maxExpiresInSeconds)
	}
	return time.Duration(expiresIn) * time.Second, nil
}
