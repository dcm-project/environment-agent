package dcm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
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
	conf        *clientcredentials.Config
	httpClient  *http.Client
	expiryDelta time.Duration

	mu          sync.Mutex
	accessToken string
	expiry      time.Time
}

// NewClientCredentialsTokenSource creates a TokenSource that fetches
// tokens from the given OIDC token endpoint using client_credentials.
// If httpClient is nil, a default client with a 10s timeout is used.
func NewClientCredentialsTokenSource(tokenEndpoint, clientID, clientSecret string, httpClient *http.Client) *ClientCredentialsTokenSource {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
			// Refuse redirects: the POST body carries clientSecret, which
			// must not be resent to another origin (REQ-DCM-261).
			CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				return fmt.Errorf("refusing to follow redirect to %s: client credentials must not be resent to another origin", req.URL)
			},
		}
	}
	return &ClientCredentialsTokenSource{
		conf: &clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     tokenEndpoint,
			// client_id/client_secret as POST body params, not a Basic auth header.
			AuthStyle: oauth2.AuthStyleInParams,
		},
		httpClient:  httpClient,
		expiryDelta: defaultExpiryDelta,
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

// fetchToken calls c.conf.Token(ctx) (not c.conf.TokenSource(ctx)) on every
// invocation so this call's ctx is honored per-fetch, preserving
// attemptRegister/sendHeartbeat's per-attempt cancellation/timeout (DD-550).
func (c *ClientCredentialsTokenSource) fetchToken(ctx context.Context) (string, time.Time, error) {
	// Inject the redirect-refusing http.Client (REQ-DCM-261, DD-580).
	ctx = context.WithValue(ctx, oauth2.HTTPClient, c.httpClient)

	tok, err := c.conf.Token(ctx)
	if err != nil {
		var retrieveErr *oauth2.RetrieveError
		if errors.As(err, &retrieveErr) {
			status := 0
			if retrieveErr.Response != nil {
				status = retrieveErr.Response.StatusCode
			}
			switch {
			case retrieveErr.ErrorCode != "" && retrieveErr.ErrorDescription != "":
				return "", time.Time{}, fmt.Errorf("token endpoint returned HTTP %d: %s: %s", status, retrieveErr.ErrorCode, retrieveErr.ErrorDescription)
			case retrieveErr.ErrorCode != "":
				return "", time.Time{}, fmt.Errorf("token endpoint returned HTTP %d: %s", status, retrieveErr.ErrorCode)
			default:
				return "", time.Time{}, fmt.Errorf("token endpoint returned HTTP %d", status)
			}
		}
		// Covers network errors and CheckRedirect's redirect-refusal error.
		return "", time.Time{}, fmt.Errorf("token request failed: %w", err)
	}

	// Reject missing/zero/negative expires_in (REQ-DCM-221). An oversized
	// expires_in is not rejected: the library clamps it to math.MaxInt32
	// seconds (~68 years, AC-DCM-226) instead of erroring.
	if tok.Expiry.IsZero() || !tok.Expiry.After(time.Now()) {
		return "", time.Time{}, fmt.Errorf("token endpoint returned invalid expires_in")
	}

	return tok.AccessToken, tok.Expiry, nil
}
