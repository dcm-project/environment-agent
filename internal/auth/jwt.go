// Package auth provides JWT authentication middleware for the environment agent API.
package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Default bounds applied by NewOIDCValidator when the caller passes a nil
// httpClient / zero discoveryTimeout. Kept as package constants rather than
// env-configurable knobs (no operator-tunable requirement exists yet; see
// internal/dcm/client.go for the same fixed-constant precedent).
const (
	// defaultOIDCDiscoveryTimeout bounds the one-time startup discovery
	// call (the single GET to the issuer's well-known document). It is
	// kept short because it runs before net.Listen — a stalled issuer
	// must not be allowed to delay the health endpoint coming up.
	defaultOIDCDiscoveryTimeout = 5 * time.Second
	// defaultOIDCHTTPTimeout bounds every individual HTTP round trip made
	// with the client injected into go-oidc, i.e. both the discovery GET
	// and, because oidc.Provider retains and reuses this same client for
	// the life of the process, every later JWKS refresh triggered from
	// the request path. It gets a bit more slack than the discovery
	// timeout since JWKS refreshes happen on the hot request path, but
	// it stays well inside AGENT_SERVER_REQUEST_TIMEOUT's 30s default.
	defaultOIDCHTTPTimeout = 10 * time.Second
)

// JWTValidator validates a raw JWT token and returns the extracted claims.
type JWTValidator interface {
	Validate(ctx context.Context, rawToken string) (*JWTClaims, error)
}

// JWTClaims holds the identity claims extracted from a validated JWT token.
type JWTClaims struct {
	Subject           string
	PreferredUsername string
}

// OIDCValidator validates JWT tokens using OIDC discovery.
type OIDCValidator struct {
	verifier *oidc.IDTokenVerifier
}

// NewOIDCValidator creates a validator that uses OIDC discovery to obtain
// JWKS keys from the issuer. When audience is empty, audience validation
// is skipped (a warning should have been logged at startup per REQ-AUTH-110).
//
// httpClient and discoveryTimeout bound two related but distinct things:
//
//   - discoveryTimeout wraps only the one-time startup oidc.NewProvider
//     call in context.WithTimeout, giving an explicit, attributable
//     "OIDC discovery timed out" startup failure independent of ctx's own
//     (possibly infinite) lifetime. A nil/zero value defaults to
//     defaultOIDCDiscoveryTimeout.
//   - httpClient is injected into the discovery context via
//     oidc.ClientContext before calling oidc.NewProvider. go-oidc's
//     Provider captures and reuses whatever client it was given at
//     construction time for every later JWKS refresh (Provider.remoteKeySet
//     lazily builds a RemoteKeySet against context.Background(), not the
//     caller's per-Validate context, and its keysFromRemote spawns a
//     detached, uncancellable goroutine to do the actual fetch). Without a
//     bounded client here, a stalled/black-holed issuer leaks one goroutine
//     per unknown/rotated kid, forever, in addition to hanging the request
//     that triggered the refresh. Injecting a client with its own Timeout
//     fixes both the discovery call and every subsequent JWKS refresh in
//     one place. A nil value defaults to a client with
//     defaultOIDCHTTPTimeout.
func NewOIDCValidator(ctx context.Context, issuerURL, audience string, httpClient *http.Client, discoveryTimeout time.Duration) (*OIDCValidator, error) {
	if ctx == nil {
		panic("auth: NewOIDCValidator context must not be nil")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultOIDCHTTPTimeout}
	}
	if discoveryTimeout <= 0 {
		discoveryTimeout = defaultOIDCDiscoveryTimeout
	}

	discoveryCtx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	discoveryCtx = oidc.ClientContext(discoveryCtx, httpClient)

	provider, err := oidc.NewProvider(discoveryCtx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery for %q: %w", issuerURL, err)
	}
	cfg := &oidc.Config{
		ClientID: audience,
	}
	if audience == "" {
		cfg.SkipClientIDCheck = true
	}
	return &OIDCValidator{
		verifier: provider.Verifier(cfg),
	}, nil
}

// Validate verifies the token signature, expiry, issuer, and audience,
// then extracts the subject and preferred_username claims.
func (v *OIDCValidator) Validate(ctx context.Context, rawToken string) (*JWTClaims, error) {
	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return nil, fmt.Errorf("verifying token: %w", err)
	}
	var extra struct {
		PreferredUsername string `json:"preferred_username"`
	}
	if err := idToken.Claims(&extra); err != nil {
		return nil, fmt.Errorf("extracting claims: %w", err)
	}
	return &JWTClaims{
		Subject:           idToken.Subject,
		PreferredUsername: extra.PreferredUsername,
	}, nil
}

// ExtractBearerToken extracts the raw token from an HTTP request's
// Authorization header. The scheme comparison is case-insensitive per RFC 7235.
func ExtractBearerToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing Authorization header")
	}
	const bearerPrefix = "bearer "
	if len(authHeader) < len(bearerPrefix) || !strings.EqualFold(authHeader[:len(bearerPrefix)], bearerPrefix) {
		return "", fmt.Errorf("authorization scheme is not Bearer")
	}
	token := authHeader[len(bearerPrefix):]
	if token == "" {
		return "", fmt.Errorf("empty Bearer token")
	}
	return token, nil
}
