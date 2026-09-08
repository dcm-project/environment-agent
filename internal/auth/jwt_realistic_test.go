package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	josejwt "github.com/go-jose/go-jose/v4/jwt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/auth"
)

// This file provides a self-contained, independent proof that
// auth.OIDCValidator works against a real OIDC discovery flow, real JWKS,
// and a real RS256-signed JWT shaped like a Keycloak access token. Unlike
// jwt_test.go (which only exercises the fully-mocked auth.JWTValidator
// interface), these tests spin up an in-process httptest.Server that serves
// both a "/.well-known/openid-configuration" document and a JWKS endpoint,
// so the full oidc.NewProvider -> provider.Verifier -> verifier.Verify path
// (signature, issuer, expiry, and audience checks) is genuinely exercised.
//
// This mirrors the structural pattern proven in control-plane's
// internal/auth/jwt.go (nearly identical NewOIDCValidator/Validate using
// coreos/go-oidc/v3), but does NOT depend on control-plane's tests or its
// real-Keycloak subsystem test in any way: the RSA keypair, discovery
// document, JWKS, and signed tokens below are all generated locally.
//
// NOTE on the JOSE "typ" header: real Keycloak access tokens do NOT set
// `typ: "Bearer"` in the JOSE header (that string only ever appears as the
// OAuth2 token_type in the token response and in the HTTP Authorization
// header). By default, Keycloak sets the access token's JOSE header to
// `typ: "JWT"` (RFC 9068's `at+jwt` is opt-in per-client, off by default).
// This was verified against Keycloak's DefaultTokenManager source rather
// than assumed; see the final report for this task for citations.

// realisticKeycloakClaims mirrors the private/custom claims a real Keycloak
// access token carries, beyond the standard RFC 7519 claims: `azp`, `scope`,
// `realm_access`, `resource_access`, and `preferred_username`.
type realisticKeycloakClaims struct {
	PreferredUsername string                 `json:"preferred_username,omitempty"`
	AZP               string                 `json:"azp,omitempty"`
	Scope             string                 `json:"scope,omitempty"`
	RealmAccess       map[string]interface{} `json:"realm_access,omitempty"`
	ResourceAccess    map[string]interface{} `json:"resource_access,omitempty"`
}

// realisticOIDCTestServer bundles an httptest.Server that serves an OIDC
// discovery document and a JWKS endpoint backed by a freshly generated RSA
// keypair, plus a helper to sign realistic Keycloak-shaped access tokens
// against that keypair.
type realisticOIDCTestServer struct {
	Server *httptest.Server
	Issuer string

	privateKey *rsa.PrivateKey
	kid        string

	// slowDiscovery/slowJWKS, when set via SetSlowDiscovery/SetSlowJWKS,
	// make the corresponding endpoint hang (per hangUntilClientGivesUp)
	// instead of responding, to prove client-side timeout enforcement
	// against a stalled/black-holed issuer.
	slowDiscovery atomic.Bool
	slowJWKS      atomic.Bool
}

// newRealisticOIDCTestServer generates a local RSA keypair and starts an
// httptest.Server serving OIDC discovery + JWKS for it. The discovery
// document's "issuer" field is set to the server's own base URL, which is
// required: go-oidc's provider.NewProvider strictly compares the issuer it
// was given against the "issuer" field returned by discovery.
func newRealisticOIDCTestServer() *realisticOIDCTestServer {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	Expect(err).NotTo(HaveOccurred())

	s := &realisticOIDCTestServer{
		privateKey: privateKey,
		kid:        "test-signing-key-1",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", s.serveDiscovery)
	mux.HandleFunc("/protocol/openid-connect/certs", s.serveJWKS)

	s.Server = httptest.NewServer(mux)
	s.Issuer = s.Server.URL
	return s
}

func (s *realisticOIDCTestServer) Close() {
	s.Server.Close()
}

// SetSlowDiscovery toggles whether the discovery endpoint hangs (per
// hangUntilClientGivesUp) instead of responding.
func (s *realisticOIDCTestServer) SetSlowDiscovery(slow bool) {
	s.slowDiscovery.Store(slow)
}

// SetSlowJWKS toggles whether the JWKS endpoint hangs (per
// hangUntilClientGivesUp) instead of responding.
func (s *realisticOIDCTestServer) SetSlowJWKS(slow bool) {
	s.slowJWKS.Store(slow)
}

// hangUntilClientGivesUp models a stalled/black-holed issuer that never
// responds: it blocks until the client aborts the request (which
// net/http.Server observes as the request context being cancelled), with a
// generous fixed fallback purely so httptest.Server.Close() in AfterEach can
// never hang the suite if context-cancellation propagation doesn't fire for
// some environment-specific reason. The fallback is not part of any timing
// assertion in the tests below.
func (s *realisticOIDCTestServer) hangUntilClientGivesUp(_ http.ResponseWriter, r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-time.After(time.Second):
	}
}

func (s *realisticOIDCTestServer) serveDiscovery(w http.ResponseWriter, r *http.Request) {
	if s.slowDiscovery.Load() {
		s.hangUntilClientGivesUp(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]string{
		"issuer":                 s.Issuer,
		"authorization_endpoint": s.Issuer + "/protocol/openid-connect/auth",
		"token_endpoint":         s.Issuer + "/protocol/openid-connect/token",
		"jwks_uri":               s.Issuer + "/protocol/openid-connect/certs",
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *realisticOIDCTestServer) serveJWKS(w http.ResponseWriter, r *http.Request) {
	if s.slowJWKS.Load() {
		s.hangUntilClientGivesUp(w, r)
		return
	}
	pub := jose.JSONWebKey{
		Key:       &s.privateKey.PublicKey,
		KeyID:     s.kid,
		Algorithm: "RS256",
		Use:       "sig",
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{pub}}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// signAccessTokenOpts controls the "aud" claim of a signed test token, so
// each test case can simulate a different Keycloak client audience-mapper
// configuration. A nil Audience means the "aud" claim is entirely absent
// from the token, simulating a Keycloak client with no audience mapper
// configured (the default state).
type signAccessTokenOpts struct {
	Audience []string
}

// signAccessToken signs an RS256 JWT shaped like a real Keycloak access
// token: JOSE header `kid` (matching an entry in the server's JWKS) and
// `typ: "JWT"` (Keycloak's default access-token header type, see NOTE
// above), plus standard claims (`sub`, `iss`, `exp`, `iat`) and the private
// claims Keycloak embeds in access tokens (`azp`, `scope`, `realm_access`,
// `resource_access`, `preferred_username`).
func (s *realisticOIDCTestServer) signAccessToken(opts signAccessTokenOpts) string {
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key: jose.JSONWebKey{
			Key:       s.privateKey,
			KeyID:     s.kid,
			Algorithm: "RS256",
			Use:       "sig",
		},
	}, (&jose.SignerOptions{}).WithType("JWT"))
	Expect(err).NotTo(HaveOccurred())

	now := time.Now()
	standardClaims := josejwt.Claims{
		Issuer:   s.Issuer,
		Subject:  "f:1234abcd-5678-ef90-1234-abcdef567890:jdoe",
		IssuedAt: josejwt.NewNumericDate(now),
		Expiry:   josejwt.NewNumericDate(now.Add(time.Hour)),
	}
	if opts.Audience != nil {
		standardClaims.Audience = josejwt.Audience(opts.Audience)
	}

	privateClaims := realisticKeycloakClaims{
		PreferredUsername: "jdoe",
		AZP:               "dcm-api",
		Scope:             "openid profile email",
		RealmAccess: map[string]interface{}{
			"roles": []string{"offline_access", "uma_authorization", "default-roles-dcm"},
		},
		ResourceAccess: map[string]interface{}{
			"dcm-api": map[string]interface{}{
				"roles": []string{"agent"},
			},
			"account": map[string]interface{}{
				"roles": []string{"manage-account", "view-profile"},
			},
		},
	}

	raw, err := josejwt.Signed(signer).Claims(standardClaims).Claims(privateClaims).Serialize()
	Expect(err).NotTo(HaveOccurred())
	return raw
}

var _ = Describe("OIDCValidator against a realistic self-signed OIDC server", Label("unit"), func() {
	var oidcSrv *realisticOIDCTestServer

	BeforeEach(func() {
		oidcSrv = newRealisticOIDCTestServer()
	})

	AfterEach(func() {
		oidcSrv.Close()
	})

	It("accepts a real signed Keycloak-shaped access token when the configured audience matches (UT-AUTH-110)", func() {
		token := oidcSrv.signAccessToken(signAccessTokenOpts{Audience: []string{"dcm-api"}})

		validator, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "dcm-api", nil, 0)
		Expect(err).NotTo(HaveOccurred())

		claims, err := validator.Validate(context.Background(), token)
		Expect(err).NotTo(HaveOccurred())
		Expect(claims.Subject).To(Equal("f:1234abcd-5678-ef90-1234-abcdef567890:jdoe"))
		Expect(claims.PreferredUsername).To(Equal("jdoe"))
	})

	It("rejects the same token when the configured audience does not match the token's aud claim (UT-AUTH-111)", func() {
		token := oidcSrv.signAccessToken(signAccessTokenOpts{Audience: []string{"dcm-api"}})

		validator, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "some-other-client", nil, 0)
		Expect(err).NotTo(HaveOccurred())

		_, err = validator.Validate(context.Background(), token)
		Expect(err).To(HaveOccurred())
	})

	It("accepts a token with no aud claim at all when no audience is configured (UT-AUTH-112)", func() {
		token := oidcSrv.signAccessToken(signAccessTokenOpts{Audience: nil})

		validator, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "", nil, 0)
		Expect(err).NotTo(HaveOccurred())

		claims, err := validator.Validate(context.Background(), token)
		Expect(err).NotTo(HaveOccurred())
		Expect(claims.Subject).To(Equal("f:1234abcd-5678-ef90-1234-abcdef567890:jdoe"))
		Expect(claims.PreferredUsername).To(Equal("jdoe"))
	})

	// Bonus (not required by the test plan): confirms SkipClientIDCheck
	// truly ignores the aud claim's presence/value entirely, rather than
	// merely tolerating its absence.
	It("accepts a token WITH an aud claim even when no audience is configured", func() {
		token := oidcSrv.signAccessToken(signAccessTokenOpts{Audience: []string{"dcm-api"}})

		validator, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "", nil, 0)
		Expect(err).NotTo(HaveOccurred())

		_, err = validator.Validate(context.Background(), token)
		Expect(err).NotTo(HaveOccurred())
	})

	// The three tests below use sub-200ms injected timeouts (per
	// hangUntilClientGivesUp) so the added wall-clock cost to the suite is
	// negligible, and assert a generous ceiling (well under the 1s fallback
	// above) rather than hanging indefinitely.

	It("bounds OIDC discovery by an explicit discoveryTimeout against a stalled issuer", func() {
		oidcSrv.SetSlowDiscovery(true)

		start := time.Now()
		_, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "dcm-api", nil, 40*time.Millisecond)
		elapsed := time.Since(start)

		Expect(err).To(HaveOccurred())
		Expect(elapsed).To(BeNumerically("<", 500*time.Millisecond),
			"discovery must be bounded by discoveryTimeout instead of hanging on a stalled issuer")
	})

	It("bounds OIDC discovery by an injected httpClient.Timeout even with discoveryTimeout=0 (default)", func() {
		oidcSrv.SetSlowDiscovery(true)

		client := &http.Client{Timeout: 40 * time.Millisecond}
		start := time.Now()
		_, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "dcm-api", client, 0)
		elapsed := time.Since(start)

		Expect(err).To(HaveOccurred())
		Expect(elapsed).To(BeNumerically("<", 500*time.Millisecond),
			"discovery must be bounded by httpClient.Timeout on its own, independent of the discovery-specific context wrap")
	})

	It("bounds a JWKS refresh by httpClient.Timeout, not by the caller's Validate context", func() {
		client := &http.Client{Timeout: 50 * time.Millisecond}
		validator, err := auth.NewOIDCValidator(context.Background(), oidcSrv.Issuer, "dcm-api", client, 200*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())

		// Discovery already succeeded above; only now make JWKS hang, so
		// the forced refresh below (triggered by a kid not yet cached)
		// exercises the bounded-client path rather than discovery's.
		oidcSrv.SetSlowJWKS(true)
		token := oidcSrv.signAccessToken(signAccessTokenOpts{Audience: []string{"dcm-api"}})

		start := time.Now()
		// Deliberately context.Background(): no caller-side deadline, to
		// prove the bound comes from the client injected at construction
		// time (retained by go-oidc's Provider for the JWKS refresh path),
		// not from request-scoped cancellation.
		_, err = validator.Validate(context.Background(), token)
		elapsed := time.Since(start)

		Expect(err).To(HaveOccurred())
		Expect(elapsed).To(BeNumerically("<", 500*time.Millisecond),
			"a stalled JWKS refresh must not be allowed to hang Validate forever, even with an undeadlined caller context")
	})
})
