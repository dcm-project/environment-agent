package apiserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/config"
)

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

var _ = Describe("HTTP Server Integration", func() {
	var (
		cfg    *config.Config
		logBuf *bytes.Buffer
		logger *slog.Logger
		ln     net.Listener
		srv    *apiserver.Server
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		cfg = defaultConfig()
		logBuf = &bytes.Buffer{}
		logger = slog.New(slog.NewJSONHandler(logBuf, nil))

		var err error
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = ln.Close() })

		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)
	})

	startServer := func(handler server.ServerInterface, opts ...time.Duration) {
		probeTimeout := 200 * time.Millisecond
		if len(opts) > 0 {
			probeTimeout = opts[0]
		}

		srv = apiserver.New(cfg, logger, handler)
		runErrCh := make(chan error, 1)
		go func() { runErrCh <- srv.Run(ctx, ln) }()

		// Race: either Run() errors (RED) or server starts serving (GREEN).
		// Use HTTP readiness probe — TCP connect alone isn't enough since
		// the listener is open but no HTTP server may be attached.
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

	Describe("Panic Recovery", func() {
		It("catches panics and returns RFC 7807 INTERNAL error (IT-HTTP-080)", func() {
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
			Expect(errBody.Type).To(Equal("INTERNAL"))
			Expect(errBody.Status).To(HaveValue(Equal(500)))
			if errBody.Detail != nil {
				Expect(*errBody.Detail).NotTo(ContainSubstring("test panic"))
				Expect(*errBody.Detail).NotTo(MatchRegexp(`\.go:\d+`))
			}

			Expect(logBuf.String()).To(MatchRegexp(`"level"\s*:\s*"ERROR"`),
				"panic must be logged at ERROR level")
		})
	})

	Describe("Error Handling", func() {
		It("returns 400 RFC 7807 for malformed requests (IT-HTTP-100)", func() {
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
			Expect(errBody.Type).To(Equal("INVALID_ARGUMENT"))
		})

		It("returns RFC 7807 for framework-layer parsing errors (IT-HTTP-110)", func() {
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
		It("enforces per-request timeout with RFC 7807 response (IT-HTTP-120)", func() {
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
			Expect(errBody.Type).To(Equal("UNAVAILABLE"))
		})
	})
})
