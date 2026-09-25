package apiserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/auth"
	"github.com/dcm-project/environment-agent/internal/config"
)

// mockJWTValidator implements auth.JWTValidator for tests.
type mockJWTValidator struct {
	claims *auth.JWTClaims
	err    error
}

func (m *mockJWTValidator) Validate(_ context.Context, _ string) (*auth.JWTClaims, error) {
	return m.claims, m.err
}

// slowJWTValidator implements auth.JWTValidator with a Validate that
// outlasts the configured request timeout, returning as soon as either the
// context is done or its own delay elapses (same idiom as IT-HTTP-120's slow
// handler), so it never blocks the test suite even if the timeout wiring is
// broken.
type slowJWTValidator struct {
	delay  time.Duration
	claims *auth.JWTClaims
	err    error
}

func (v *slowJWTValidator) Validate(ctx context.Context, _ string) (*auth.JWTClaims, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(v.delay):
		return v.claims, v.err
	}
}

// stubHandler implements server.ServerInterface with controllable behavior.
type stubHandler struct {
	getHealthFunc      func(w http.ResponseWriter, r *http.Request)
	listProvidersFunc  func(w http.ResponseWriter, r *http.Request, params v1alpha1.ListProvidersParams)
	createProviderFunc func(w http.ResponseWriter, r *http.Request, params v1alpha1.CreateProviderParams)
	getProviderFunc    func(w http.ResponseWriter, r *http.Request, providerId string)
}

func (h *stubHandler) GetHealth(w http.ResponseWriter, r *http.Request) {
	if h.getHealthFunc != nil {
		h.getHealthFunc(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *stubHandler) ListProviders(w http.ResponseWriter, r *http.Request, params v1alpha1.ListProvidersParams) {
	if h.listProvidersFunc != nil {
		h.listProvidersFunc(w, r, params)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *stubHandler) CreateProvider(w http.ResponseWriter, r *http.Request, params v1alpha1.CreateProviderParams) {
	if h.createProviderFunc != nil {
		h.createProviderFunc(w, r, params)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *stubHandler) GetProvider(w http.ResponseWriter, r *http.Request, providerId string) {
	if h.getProviderFunc != nil {
		h.getProviderFunc(w, r, providerId)
		return
	}
	w.WriteHeader(http.StatusOK)
}

var _ server.ServerInterface = (*stubHandler)(nil)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (sb *syncBuffer) Write(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Write(p)
}

func (sb *syncBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.String()
}

func defaultConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Address:         "127.0.0.1:0",
			ShutdownTimeout: 15 * time.Second,
			RequestTimeout:  30 * time.Second,
		},
	}
}

func httpClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Second}
}

var _ = Describe("HTTP Server Integration", Label("integration"), func() {
	var (
		cfg    *config.Config
		logBuf *syncBuffer
		logger *slog.Logger
		ln     net.Listener
		srv    *apiserver.Server
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		cfg = defaultConfig()
		logBuf = &syncBuffer{}
		logger = slog.New(slog.NewJSONHandler(logBuf, nil))

		var err error
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = ln.Close() })

		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)
	})

	// runAndAwaitReady starts srv and blocks until ready to serve HTTP.
	runAndAwaitReady := func(opts ...time.Duration) {
		probeTimeout := 200 * time.Millisecond
		if len(opts) > 0 {
			probeTimeout = opts[0]
		}

		runErrCh := make(chan error, 1)
		go func() { runErrCh <- srv.Run(ctx, ln) }()

		// Run() erroring and the server starting to serve race each other, so
		// poll with an HTTP readiness probe — TCP connect alone isn't enough
		// since the listener is open but no HTTP server may be attached yet.
		ready := make(chan struct{})
		go func() {
			for {
				client := &http.Client{Timeout: probeTimeout}
				resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
				if err == nil {
					_ = resp.Body.Close()
					close(ready)
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
		}()

		select {
		case err := <-runErrCh:
			Fail(fmt.Sprintf("server failed to start: %v", err))
		case <-ready:
			// Server is serving HTTP — proceed to assertions
		case <-time.After(3 * time.Second):
			Fail("timed out waiting for server readiness")
		}
	}

	startServer := func(handler server.ServerInterface, opts ...time.Duration) {
		srv = apiserver.New(cfg, logger, handler, nil)
		runAndAwaitReady(opts...)
	}

	// startServerWithAuth wires the real auth middleware into the full server
	// chain, instead of the identity passthrough used elsewhere in this suite.
	startServerWithAuth := func(handler server.ServerInterface, validator auth.JWTValidator) {
		authMW := auth.Middleware(auth.MiddlewareConfig{
			JWTValidator: validator,
			Logger:       logger,
		})
		srv = apiserver.New(cfg, logger, handler, authMW)
		runAndAwaitReady()
	}

	Describe("Server Lifecycle", func() {
		It("starts and accepts connections on the configured address (IT-HTTP-010)", func() {
			handler := &stubHandler{}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
		})

		It("registers all OpenAPI routes (IT-HTTP-020)", func() {
			handler := &stubHandler{}
			startServer(handler)

			client := httpClient()
			baseURL := fmt.Sprintf("http://%s/api/v1alpha1", ln.Addr().String())

			routes := []struct {
				method string
				path   string
			}{
				{"GET", "/health"},
				{"GET", "/providers"},
				{"POST", "/providers"},
				{"GET", "/providers/test-id"},
			}

			for _, route := range routes {
				var resp *http.Response
				var err error

				switch route.method {
				case "GET":
					resp, err = client.Get(baseURL + route.path)
				case "POST":
					resp, err = client.Post(baseURL+route.path, "application/json",
						strings.NewReader(`{"name":"test","endpoint":"http://test","service_type":"test","schema_version":"1.0"}`))
				}

				Expect(err).NotTo(HaveOccurred(), "route %s %s", route.method, route.path)
				_ = resp.Body.Close()
				Expect(resp.StatusCode).NotTo(Equal(http.StatusNotFound),
					"route %s %s must not return 404", route.method, route.path)
			}
		})

		It("loads config from environment and listens on configured port (IT-HTTP-060)", func() {
			handler := &stubHandler{}
			startServer(handler)

			Expect(srv.Addr()).NotTo(BeEmpty())
			Expect(srv.Addr()).To(Equal(ln.Addr().String()))
		})

		It("logs lifecycle events on startup and shutdown (IT-HTTP-090)", func() {
			handler := &stubHandler{}
			startServer(handler)

			Expect(logBuf.String()).To(ContainSubstring(ln.Addr().String()),
				"startup log must contain listen address")

			cancel()
			Eventually(func() string {
				return logBuf.String()
			}).WithTimeout(2*time.Second).Should(ContainSubstring("shutdown"),
				"shutdown log must contain shutdown message")
		})
	})

	Describe("Request Logging", func() {
		It("logs each request with method, path, status, and duration (IT-HTTP-070)", func() {
			handler := &stubHandler{}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			Eventually(func() string {
				return logBuf.String()
			}).WithTimeout(2*time.Second).Should(And(
				ContainSubstring("GET"),
				ContainSubstring("/api/v1alpha1/health"),
				ContainSubstring("200"),
			), "request log must contain method, path, and status")

			Expect(logBuf.String()).To(MatchRegexp(`"level"\s*:\s*"INFO"`),
				"request log must be at INFO level")
			Expect(logBuf.String()).To(MatchRegexp(`duration|elapsed|latency`),
				"request log must contain duration")
		})
	})

	Describe("Auth Rejection Logging", func() {
		const providersPath = "/api/v1alpha1/providers"

		// requestLogCountForPath counts per-request audit log lines (the ones
		// RequestLogger — or auth's rejection-path log call — emits with
		// msg="request") for a specific path, so the readiness probe's own
		// /health request log isn't counted alongside the request under test.
		requestLogCountForPath := func(path string) int {
			count := 0
			for _, line := range strings.Split(logBuf.String(), "\n") {
				if strings.Contains(line, `"msg":"request"`) && strings.Contains(line, `"path":"`+path+`"`) {
					count++
				}
			}
			return count
		}

		It("logs exactly one INFO request audit entry when credentials are missing, without moving RequestLogger ahead of auth (IT-AUTH-120)", func() {
			handler := &stubHandler{}
			startServerWithAuth(handler, &mockJWTValidator{})

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s%s", ln.Addr().String(), providersPath))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

			Eventually(func() int { return requestLogCountForPath(providersPath) }).
				WithTimeout(2 * time.Second).Should(Equal(1))
			Expect(logBuf.String()).To(And(
				MatchRegexp(`"level"\s*:\s*"INFO"`),
				ContainSubstring(`"method":"GET"`),
				ContainSubstring(`"path":"`+providersPath+`"`),
				ContainSubstring(`"status":401`),
			), "the audit log for a rejected request must match the shape of a normal request log")
		})

		It("logs exactly one INFO request audit entry when credentials are invalid (IT-AUTH-121)", func() {
			handler := &stubHandler{}
			startServerWithAuth(handler, &mockJWTValidator{err: fmt.Errorf("token is expired")})

			req, err := http.NewRequest(http.MethodGet,
				fmt.Sprintf("http://%s%s", ln.Addr().String(), providersPath), nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Bearer invalid-token")

			resp, err := httpClient().Do(req)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

			Eventually(func() int { return requestLogCountForPath(providersPath) }).
				WithTimeout(2 * time.Second).Should(Equal(1))
			Expect(logBuf.String()).To(ContainSubstring(`"status":401`))
		})

		It("retains sub and preferred_username on successful authenticated requests (IT-AUTH-122)", func() {
			handler := &stubHandler{}
			startServerWithAuth(handler, &mockJWTValidator{
				claims: &auth.JWTClaims{Subject: "user-123", PreferredUsername: "jdoe"},
			})

			req, err := http.NewRequest(http.MethodGet,
				fmt.Sprintf("http://%s%s", ln.Addr().String(), providersPath), nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Bearer valid-token")

			resp, err := httpClient().Do(req)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			Eventually(func() int { return requestLogCountForPath(providersPath) }).
				WithTimeout(2 * time.Second).Should(Equal(1))
			Expect(logBuf.String()).To(And(
				ContainSubstring(`"status":200`),
				ContainSubstring(`"sub":"user-123"`),
				ContainSubstring(`"preferred_username":"jdoe"`),
			), "successful requests must keep JWT identity attributes in the audit log")
		})
	})

	Describe("Auth Middleware Ordering", func() {
		It("a hanging JWTValidator does not hang the request past the configured timeout, and RequestTimeout's 503 always wins (IT-AUTH-130)", func() {
			// AC-AUTH-090: auth runs inside RequestTimeout's boundary (see the
			// real chain order in server.go's Run). Proves that placement is
			// load-bearing: a validator hanging past the deadline does not hang
			// the request past it, and the client-visible status is
			// deterministically 503 — never a buffered 401/200 — because
			// RequestTimeout discards the buffer once the deadline has passed
			// (DD-170).
			cfg.Server.RequestTimeout = 1 * time.Second
			handler := &stubHandler{}
			startServerWithAuth(handler, &slowJWTValidator{
				delay: 3 * time.Second,
				err:   fmt.Errorf("token is expired"), // would be 401 if it ever reached the client
			})

			client := &http.Client{Timeout: 5 * time.Second}
			start := time.Now()
			req, err := http.NewRequest(http.MethodGet,
				fmt.Sprintf("http://%s/api/v1alpha1/providers", ln.Addr().String()), nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Bearer some-token")

			resp, err := client.Do(req)
			elapsed := time.Since(start)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			// Compare against the validator's delay, not a fixed bound, so this
			// stays meaningful if the timeouts above are ever retuned.
			Expect(elapsed).To(BeNumerically("<", 3*time.Second),
				"the hanging validator must not be allowed to hang the request past RequestTimeout's deadline")

			Expect(resp.StatusCode).To(Equal(http.StatusServiceUnavailable),
				"RequestTimeout's 503 override must win regardless of what auth would have returned")
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal(v1alpha1.ErrorTypeUNAVAILABLE))

			Eventually(func() string {
				return logBuf.String()
			}).WithTimeout(2*time.Second).Should(And(
				MatchRegexp(`"msg"\s*:\s*"request"`),
				ContainSubstring(`"status":503`),
			), "audit log must record 503, matching the client-visible status (DD-510)")
		})
	})

	Describe("Panic Recovery", func() {
		It("catches panics and returns RFC 9457 INTERNAL error (IT-HTTP-080)", func() {
			handler := &stubHandler{
				getHealthFunc: func(_ http.ResponseWriter, _ *http.Request) {
					panic("test panic for recovery")
				},
			}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal(v1alpha1.ErrorTypeINTERNAL))
			Expect(errBody.Status).To(HaveValue(Equal(500)))
			if errBody.Detail != nil {
				Expect(*errBody.Detail).NotTo(ContainSubstring("test panic"))
				Expect(*errBody.Detail).NotTo(MatchRegexp(`\.go:\d+`))
			}

			Expect(logBuf.String()).To(MatchRegexp(`"level"\s*:\s*"ERROR"`),
				"panic must be logged at ERROR level")
		})

		It("still emits one INFO request audit log entry (status 500) for a panicking request (IT-HTTP-081)", func() {
			// Verifies DD-510's centralized emission preserves the pre-existing
			// guarantee: RequestTimeout's own recover still logs status 500
			// during the panic unwind.
			handler := &stubHandler{
				getHealthFunc: func(_ http.ResponseWriter, _ *http.Request) {
					panic("test panic for recovery")
				},
			}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

			Eventually(func() string {
				return logBuf.String()
			}).WithTimeout(2*time.Second).Should(And(
				MatchRegexp(`"msg"\s*:\s*"request"`),
				MatchRegexp(`"level"\s*:\s*"INFO"`),
				ContainSubstring(`"status":500`),
			), "the panic's audit log entry must still be emitted, matching the pre-DD-510 guarantee")
		})
	})

	Describe("Error Handling", func() {
		It("returns 400 RFC 9457 for malformed requests (IT-HTTP-100)", func() {
			handler := &stubHandler{}
			startServer(handler)

			client := httpClient()
			resp, err := client.Post(
				fmt.Sprintf("http://%s/api/v1alpha1/providers", ln.Addr().String()),
				"application/json",
				strings.NewReader(`{"bad":`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal(v1alpha1.ErrorTypeINVALIDARGUMENT))
		})

		It("returns RFC 9457 for framework-layer parsing errors (IT-HTTP-110)", func() {
			handler := &stubHandler{}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(
				fmt.Sprintf("http://%s/api/v1alpha1/providers?max_page_size=not-a-number", ln.Addr().String()),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).NotTo(BeEmpty())
			Expect(errBody.Status).NotTo(BeNil())
		})
	})

	Describe("Request Timeout", func() {
		It("enforces per-request timeout with RFC 9457 response (IT-HTTP-120)", func() {
			cfg.Server.RequestTimeout = 1 * time.Second
			handler := &stubHandler{
				getHealthFunc: func(w http.ResponseWriter, r *http.Request) {
					select {
					case <-r.Context().Done():
						return
					case <-time.After(3 * time.Second):
						w.WriteHeader(http.StatusOK)
					}
				},
			}
			startServer(handler, 2*time.Second)

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusServiceUnavailable))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal(v1alpha1.ErrorTypeUNAVAILABLE))

			Eventually(func() string {
				return logBuf.String()
			}).WithTimeout(2*time.Second).Should(And(
				MatchRegexp(`"msg"\s*:\s*"request"`),
				ContainSubstring(`"status":503`),
			), "audit log must record 503, not the inner handler's buffered status (DD-510)")
		})

		It("still buffers, flushes, and logs exactly once when timeout enforcement is disabled (IT-HTTP-121)", func() {
			// DD-510 requires RequestTimeout to always wrap the request (buffering
			// + single-point audit-log emission), even when timeout <= 0 disables
			// deadline enforcement itself.
			cfg.Server.RequestTimeout = 0
			const responseBody = "disabled-timeout-response"
			const testPath = "/api/v1alpha1/providers"
			handler := &stubHandler{
				listProvidersFunc: func(w http.ResponseWriter, _ *http.Request, _ v1alpha1.ListProvidersParams) {
					w.Header().Set("X-Test-Header", "present")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(responseBody))
				},
			}
			// startServer's readiness probe only hits /health, so it can't
			// contribute a spurious log line for testPath below.
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s%s", ln.Addr().String(), testPath))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("X-Test-Header")).To(Equal("present"))
			body, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(body)).To(Equal(responseBody))

			countForPath := func() int {
				count := 0
				for _, line := range strings.Split(logBuf.String(), "\n") {
					if strings.Contains(line, `"msg":"request"`) && strings.Contains(line, `"path":"`+testPath+`"`) {
						count++
					}
				}
				return count
			}

			Eventually(countForPath).WithTimeout(2*time.Second).Should(Equal(1),
				"exactly one audit log entry, not zero (buffer never flushed/logged) and not two (double-logged)")
			Expect(logBuf.String()).To(ContainSubstring(`"status":200`))
		})
	})
})
