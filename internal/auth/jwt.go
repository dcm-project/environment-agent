// Package auth provides JWT authentication middleware for the environment agent API.
package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
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
func NewOIDCValidator(ctx context.Context, issuerURL, audience string) (*OIDCValidator, error) {
	if ctx == nil {
		panic("auth: NewOIDCValidator context must not be nil")
	}
	provider, err := oidc.NewProvider(ctx, issuerURL)
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
