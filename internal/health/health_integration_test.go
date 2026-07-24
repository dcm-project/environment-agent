package health_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health"
)

type mockMessagingStatus struct {
	connected bool
}

func (m *mockMessagingStatus) IsConnected() bool {
	return m.connected
}

type stubHandler struct {
	getHealthFunc      func(w http.ResponseWriter, r *http.Request)
	listProvidersFunc  func(w http.ResponseWriter, r *http.Request, params v1alpha1.ListProvidersParams)
	createProviderFunc func(w http.ResponseWriter, r *http.Request, params v1alpha1.CreateProviderParams)
	getProviderFunc    func(w http.ResponseWriter, r *http.Request, providerId string)
}

func (h *stubHandler) GetHealth(w http.ResponseWriter, r *http.Request) {
	// TODO(DD-150): Remove stubHandler when strict handler is wired.
	// REQ-HLT-010 and REQ-HLT-060 must be verified through production path.
	w.Header().Set("Content-Type", "application/json")
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

var _ = Describe("Health Service Integration", func() {
	var (
		cfg       *config.Config
		logger    *slog.Logger
		ln        net.Listener
		ctx       context.Context
		cancel    context.CancelFunc
		msgStatus *mockMessagingStatus
		svc       *health.Service
	)

	BeforeEach(func() {
		cfg = defaultConfig()
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))

		var err error
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = ln.Close() })

		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo BeforeEach requires closure variable assignment
		DeferCleanup(cancel)

		msgStatus = &mockMessagingStatus{connected: true}
		svc = health.NewService(msgStatus)
	})

	startServer := func(handler server.ServerInterface) {
		srv := apiserver.New(cfg, logger, handler)
		runErrCh := make(chan error, 1)
		go func() { runErrCh <- srv.Run(ctx, ln) }()

		ready := make(chan struct{})
		go func() {
			for {
				client := &http.Client{Timeout: 200 * time.Millisecond}
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
		case <-time.After(3 * time.Second):
			Fail("timed out waiting for server readiness")
		}
	}

	Describe("Healthy State", func() {
		It("returns 200 OK with application/json Content-Type (IT-HLT-010)", func() {
			handler := &stubHandler{getHealthFunc: func(w http.ResponseWriter, _ *http.Request) {
				result := svc.Status()
				_ = json.NewEncoder(w).Encode(result)
			}}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/json"))
		})

		It("returns healthy status and path in response body (IT-HLT-020)", func() {
			handler := &stubHandler{getHealthFunc: func(w http.ResponseWriter, _ *http.Request) {
				result := svc.Status()
				_ = json.NewEncoder(w).Encode(result)
			}}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			var body v1alpha1.Health
			Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
			Expect(body.Status).To(HaveValue(Equal("healthy")))
			Expect(body.Path).To(HaveValue(Equal("health")))
		})
	})

	Describe("Unhealthy State", func() {
		BeforeEach(func() {
			msgStatus.connected = false
		})

		It("returns unhealthy status when messaging disconnected (IT-HLT-030)", func() {
			handler := &stubHandler{getHealthFunc: func(w http.ResponseWriter, _ *http.Request) {
				result := svc.Status()
				_ = json.NewEncoder(w).Encode(result)
			}}
			startServer(handler)

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/json"))

			var body v1alpha1.Health
			Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
			Expect(body.Status).To(HaveValue(Equal("unhealthy")))
			Expect(body.Path).To(HaveValue(Equal("health")))
		})
	})

	Describe("Performance", func() {
		BeforeEach(func() {
			msgStatus.connected = false
		})

		It("responds within 5ms p99 from in-memory state (IT-HLT-040)", func() {
			handler := &stubHandler{getHealthFunc: func(w http.ResponseWriter, _ *http.Request) {
				result := svc.Status()
				_ = json.NewEncoder(w).Encode(result)
			}}
			startServer(handler)

			client := httpClient()
			baseURL := fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String())

			By("warming up connection pool")
			for i := 0; i < 10; i++ {
				resp, err := client.Get(baseURL)
				Expect(err).NotTo(HaveOccurred())
				_ = resp.Body.Close()
			}

			By("measuring p99 latency over 100 requests")
			durations := make([]time.Duration, 100)
			for i := 0; i < 100; i++ {
				start := time.Now()
				resp, err := client.Get(baseURL)
				durations[i] = time.Since(start)
				Expect(err).NotTo(HaveOccurred())
				_ = resp.Body.Close()
			}

			slices.Sort(durations)
			p99 := durations[98]
			Expect(p99).To(BeNumerically("<", 5*time.Millisecond),
				"p99 response time must be below 5ms, got %v", p99)

			By("verifying response is from in-memory state (not nil)")
			resp, err := client.Get(baseURL)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()
			var body v1alpha1.Health
			Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
			Expect(body.Status).NotTo(BeNil())
		})
	})
})
