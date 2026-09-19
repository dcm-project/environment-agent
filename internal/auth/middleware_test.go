package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/auth"
	"github.com/dcm-project/environment-agent/internal/requestctx"
)

// requestChain rebuilds, in-memory, the same middleware nesting server.go's
// Run wires in production (requestctx.Middleware -> RequestTimeout -> auth
// -> RequestLogger -> handler, omitting only PanicRecovery). Since DD-510
// centralizes audit-log emission in RequestTimeout, tests asserting on the
// "request" log entry must go through this chain rather than invoking mw in
// isolation. timeout<=0 disables deadline enforcement, which is fine since
// none of these tests are about timeout behavior.
func requestChain(logger *slog.Logger, mw func(http.Handler) http.Handler, handler http.Handler) http.Handler {
	withRequestLogger := apiserver.RequestLogger()(handler)
	withAuth := mw(withRequestLogger)
	withTimeout := apiserver.RequestTimeout(0, logger)(withAuth)
	return requestctx.Middleware(withTimeout)
}

// mockValidator implements auth.JWTValidator for testing.
type mockValidator struct {
	claims *auth.JWTClaims
	err    error
}

func (m *mockValidator) Validate(_ context.Context, _ string) (*auth.JWTClaims, error) {
	return m.claims, m.err
}

// okHandler is a simple handler that returns 200 and stores claims from context.
func okHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
}

var _ = Describe("Auth Middleware", Label("unit"), func() {
	var (
		logBuf bytes.Buffer
		logger *slog.Logger
	)

	BeforeEach(func() {
		logBuf.Reset()
		logger = slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})

	Describe("Middleware", func() {
		It("bypasses /api/v1alpha1/health (UT-AUTH-020)", func() {
			validator := &mockValidator{err: nil}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/health", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
		})

		It("does not bypass non-GET methods at the health path (UT-AUTH-021)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodPost, "/api/v1alpha1/health", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))
			Expect(rr.Header().Get("WWW-Authenticate")).To(Equal("Bearer"))
		})

		It("rejects request without Authorization header with 401 (UT-AUTH-030)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))
			Expect(rr.Header().Get("Content-Type")).To(Equal("application/problem+json"))
			Expect(rr.Header().Get("WWW-Authenticate")).To(Equal("Bearer"))

			var body map[string]any
			Expect(json.Unmarshal(rr.Body.Bytes(), &body)).To(Succeed())
			Expect(body["type"]).To(Equal("UNAUTHORIZED"))
			Expect(body["detail"]).To(Equal("invalid Bearer token"))
		})

		It("rejects wrong scheme (Basic) with 401 (UT-AUTH-031)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Basic abc")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))
			Expect(rr.Header().Get("WWW-Authenticate")).To(Equal("Bearer"))
		})

		It("rejects invalid token with 401 (UT-AUTH-040)", func() {
			validator := &mockValidator{
				err: fmt.Errorf("token is expired"),
			}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Bearer invalid-token")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))

			var body map[string]any
			Expect(json.Unmarshal(rr.Body.Bytes(), &body)).To(Succeed())
			Expect(body["type"]).To(Equal("UNAUTHORIZED"))
		})

		It("never leaks internal validator error details in the response body (UT-AUTH-041)", func() {
			internalErr := fmt.Errorf("verifying token: oidc: token is expired (Token Expiry: 2020-01-01)")
			validator := &mockValidator{err: internalErr}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Bearer invalid-token")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))

			var body map[string]any
			Expect(json.Unmarshal(rr.Body.Bytes(), &body)).To(Succeed())
			Expect(body["detail"]).To(Equal("invalid Bearer token"))
			Expect(rr.Body.String()).NotTo(ContainSubstring("oidc:"))
			Expect(rr.Body.String()).NotTo(ContainSubstring("Token Expiry"))

			Expect(logBuf.String()).To(ContainSubstring("oidc: token is expired"))
			Expect(logBuf.String()).To(ContainSubstring("/api/v1alpha1/providers"))
		})

		It("never leaks bearer-extraction error details in the response body (UT-AUTH-042)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Basic abc")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))

			var body map[string]any
			Expect(json.Unmarshal(rr.Body.Bytes(), &body)).To(Succeed())
			Expect(body["detail"]).To(Equal("invalid Bearer token"))
		})

		It("passes valid token and sets context claims (UT-AUTH-050)", func() {
			validator := &mockValidator{
				claims: &auth.JWTClaims{
					Subject:           "user-123",
					PreferredUsername: "jdoe",
				},
			}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			var gotClaims *auth.JWTClaims
			var gotOK bool
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotClaims, gotOK = auth.ClaimsFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			})
			handler := mw(inner)
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
			Expect(gotOK).To(BeTrue())
			Expect(gotClaims).NotTo(BeNil())
			Expect(gotClaims.Subject).To(Equal("user-123"))
			Expect(gotClaims.PreferredUsername).To(Equal("jdoe"))
		})

		It("logs the authenticated identity at DEBUG level with sub, preferred_username, method, and path (UT-AUTH-051)", func() {
			validator := &mockValidator{
				claims: &auth.JWTClaims{
					Subject:           "user-123",
					PreferredUsername: "jdoe",
				},
			}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))

			var debugRecord map[string]any
			for _, line := range bytes.Split(logBuf.Bytes(), []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				var rec map[string]any
				Expect(json.Unmarshal(line, &rec)).To(Succeed())
				if rec["msg"] == "authenticated request" {
					debugRecord = rec
				}
			}
			Expect(debugRecord).NotTo(BeNil(), "expected a DEBUG 'authenticated request' log entry")
			Expect(debugRecord["level"]).To(Equal("DEBUG"))
			Expect(debugRecord["sub"]).To(Equal("user-123"))
			Expect(debugRecord["preferred_username"]).To(Equal("jdoe"))
			Expect(debugRecord["method"]).To(Equal(http.MethodGet))
			Expect(debugRecord["path"]).To(Equal("/api/v1alpha1/providers"))
		})
	})

	Describe("Request audit logging on rejection", func() {
		// requestLogLines returns parsed JSON log records with msg="request".
		requestLogLines := func(buf *bytes.Buffer) []map[string]any {
			var records []map[string]any
			for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				var rec map[string]any
				Expect(json.Unmarshal(line, &rec)).To(Succeed())
				if rec["msg"] == "request" {
					records = append(records, rec)
				}
			}
			return records
		}

		// UT-AUTH-100/101/102 assert on the "request" audit log entry, which
		// DD-510 centralizes in apiserver.RequestTimeout, so these tests drive
		// the middleware through requestChain rather than in isolation.

		It("emits exactly one request audit log entry for missing credentials (UT-AUTH-100)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := requestChain(logger, mw, okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))

			records := requestLogLines(&logBuf)
			Expect(records).To(HaveLen(1), "expected exactly one request audit log entry")
			Expect(records[0]["level"]).To(Equal("INFO"))
			Expect(records[0]["method"]).To(Equal(http.MethodGet))
			Expect(records[0]["path"]).To(Equal("/api/v1alpha1/providers"))
			Expect(records[0]["status"]).To(BeNumerically("==", http.StatusUnauthorized))
			Expect(records[0]["duration"]).NotTo(BeEmpty())
		})

		It("emits exactly one request audit log entry for invalid credentials (UT-AUTH-101)", func() {
			validator := &mockValidator{err: fmt.Errorf("token is expired")}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := requestChain(logger, mw, okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Bearer invalid-token")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))

			records := requestLogLines(&logBuf)
			Expect(records).To(HaveLen(1), "expected exactly one request audit log entry")
			Expect(records[0]["level"]).To(Equal("INFO"))
			Expect(records[0]["method"]).To(Equal(http.MethodGet))
			Expect(records[0]["path"]).To(Equal("/api/v1alpha1/providers"))
			Expect(records[0]["status"]).To(BeNumerically("==", http.StatusUnauthorized))
			Expect(records[0]["duration"]).NotTo(BeEmpty())
		})

		It("does not emit a request audit log entry twice for a wrong-scheme rejection (UT-AUTH-102)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := requestChain(logger, mw, okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Basic abc")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))
			Expect(requestLogLines(&logBuf)).To(HaveLen(1))
		})

		It("emits exactly one request audit log entry (not zero) for the bypassed health endpoint when driven through the full chain (UT-AUTH-103)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := requestChain(logger, mw, okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/health", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
			records := requestLogLines(&logBuf)
			Expect(records).To(HaveLen(1),
				"RequestTimeout (DD-510) must still emit exactly one audit log entry for a bypassed request, not zero and not two")
			Expect(records[0]["status"]).To(BeNumerically("==", http.StatusOK))
		})

		It("auth.Middleware invoked in isolation never itself emits a request audit log entry (UT-AUTH-104)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/health", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
			Expect(requestLogLines(&logBuf)).To(BeEmpty(),
				"per DD-510, only apiserver.RequestTimeout ever emits the audit log entry; "+
					"auth.Middleware called on its own (not exercised here) only records outcome fields")
		})
	})

	Describe("DisabledMiddleware", func() {
		It("passes all requests through (UT-AUTH-060)", func() {
			mw := auth.DisabledMiddleware(logger)

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
		})

		It("passes requests with Bearer token through without validation (UT-AUTH-061)", func() {
			mw := auth.DisabledMiddleware(logger)

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			req.Header.Set("Authorization", "Bearer some-token")
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusOK))
		})

		It("emits WARN log at construction time (UT-AUTH-090)", func() {
			logBuf.Reset()
			_ = auth.DisabledMiddleware(logger)

			Expect(logBuf.String()).To(ContainSubstring("authentication is disabled"))
		})
	})

	Describe("writeAuthError", func() {
		It("produces RFC 7807 JSON with required fields (UT-AUTH-070)", func() {
			validator := &mockValidator{}
			mw := auth.Middleware(auth.MiddlewareConfig{
				JWTValidator: validator,
				Logger:       logger,
			})

			handler := mw(okHandler())
			req := httptest.NewRequest(http.MethodGet, "/api/v1alpha1/providers", nil)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			Expect(rr.Code).To(Equal(http.StatusUnauthorized))
			Expect(rr.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var body map[string]any
			Expect(json.Unmarshal(rr.Body.Bytes(), &body)).To(Succeed())
			Expect(body).To(HaveKey("type"))
			Expect(body).To(HaveKey("title"))
			Expect(body).To(HaveKey("status"))
			Expect(body).To(HaveKey("detail"))
			Expect(body["type"]).To(Equal("UNAUTHORIZED"))
			Expect(body["title"]).To(Equal("Unauthorized"))
			Expect(body["status"]).To(BeNumerically("==", 401))
		})
	})
})
