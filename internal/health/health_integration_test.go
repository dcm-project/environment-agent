package health_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/handler"
	"github.com/dcm-project/environment-agent/internal/health"
	"github.com/dcm-project/environment-agent/internal/httperror"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/service"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

type mockMessagingStatus struct {
	connected bool
}

func (m *mockMessagingStatus) IsConnected() bool {
	return m.connected
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

var _ = Describe("Health Service Integration", Label("integration"), func() {
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

	startServer := func() {
		fileStore, err := store.NewFileStore(filepath.Join(GinkgoT().TempDir(), "registrations.json"), logger)
		Expect(err).NotTo(HaveOccurred())
		providerSvc := service.New(fileStore, provider.NewRegistry(), provider.NewInMemoryHealthTracker(), nil, logger)

		strictHandler := handler.New(svc, providerSvc)
		h := server.NewStrictHandlerWithOptions(strictHandler, nil, server.StrictHTTPServerOptions{
			RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
				httperror.WriteInvalidArgument(w, r, logger, err.Error())
			},
			ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
				httperror.WriteResponse(w, logger, http.StatusInternalServerError,
					"INTERNAL", "Internal Server Error", err.Error(), &r.RequestURI)
			},
		})

		srv := apiserver.New(cfg, logger, h)
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
			startServer()

			client := httpClient()
			resp, err := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/json"))
		})

		It("returns healthy status and path in response body (IT-HLT-020)", func() {
			startServer()

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
			startServer()

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

		It("responds within 150ms p99 from in-memory state (IT-HLT-040)", func() {
			startServer()

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
			// 150ms absorbs CI/sandbox CPU contention noise; this test
			// (AC-HLT-050) only needs to catch a real regression like an
			// accidental blocking call, not enforce a strict latency SLA.
			Expect(p99).To(BeNumerically("<", 150*time.Millisecond),
				"p99 response time must be below 150ms, got %v", p99)

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
