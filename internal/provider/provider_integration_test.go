package provider_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/handler"
	"github.com/dcm-project/environment-agent/internal/health"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/httperror"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/service"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

type noopMessaging struct{}

func (noopMessaging) IsConnected() bool { return true }

func defaultConfig() *config.Config {
	cfg, err := config.Load()
	Expect(err).NotTo(HaveOccurred())
	cfg.Server.Address = "127.0.0.1:0"
	if os.Getenv("AGENT_SP_PERSISTENCE_PATH") == "" {
		cfg.Provider.PersistencePath = filepath.Join(GinkgoT().TempDir(), "registrations.json")
	}
	return cfg
}

func startRealServer() (baseURL string, stop func()) {
	cfg := defaultConfig()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	ctx, cancel := context.WithCancel(context.Background())

	// Safety-net: ensure ctx and listener are cleaned up even if Fail fires
	// before stop() is returned/registered. DeferCleanup runs on Fail/panic.
	// Idempotent with the stop() closure below (cancel is safe to call twice;
	// ln.Close on an already-closed listener returns a harmless error).
	DeferCleanup(func() {
		cancel()
		_ = ln.Close()
	})

	fileStore, err := store.NewFileStore(cfg.Provider.PersistencePath, logger)
	Expect(err).NotTo(HaveOccurred())
	registry := provider.NewRegistry()
	healthTracker := provider.NewInMemoryHealthTracker()
	healthMonitor := monitor.New(healthTracker, cfg.Health, logger)
	providerSvc := service.New(fileStore, registry, healthTracker, healthMonitor, logger)
	Expect(providerSvc.LoadPersisted()).To(Succeed())
	providerSvc.RegisterEmbedded(cfg.Provider.EmbeddedSPs)
	healthMonitor.Start(ctx)
	DeferCleanup(healthMonitor.Stop)

	healthSvc := health.NewService(noopMessaging{})
	strictHandler := handler.New(healthSvc, providerSvc)
	h := server.NewStrictHandlerWithOptions(strictHandler, nil, server.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			httperror.WriteInvalidArgument(w, r, logger, err.Error())
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			httperror.WriteResponse(w, logger, http.StatusInternalServerError,
				"INTERNAL", "Internal Server Error",
				err.Error(), &r.RequestURI)
		},
	})
	srv := apiserver.New(cfg, logger, h)

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- srv.Run(ctx, ln) }()

	ready := make(chan struct{})
	go func() {
		for {
			client := &http.Client{Timeout: 200 * time.Millisecond}
			resp, probeErr := client.Get(fmt.Sprintf("http://%s/api/v1alpha1/health", ln.Addr().String()))
			if probeErr == nil {
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
	case <-time.After(10 * time.Second):
		Fail("timed out waiting for server readiness")
	}

	baseURL = fmt.Sprintf("http://%s", ln.Addr().String())
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-runErrCh:
			case <-time.After(cfg.Server.ShutdownTimeout + time.Second):
				_ = ln.Close()
				Fail("server did not shut down")
			}
			_ = ln.Close()
		})
	}
	return baseURL, stop
}

func validProviderBody() string {
	return `{"name":"db-provider","endpoint":"https://sp.example.com:8080","service_type":"database","schema_version":"v1alpha1"}`
}

func startWithPersistence(path string) error {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fileStore, err := store.NewFileStore(path, logger)
	if err != nil {
		return err
	}
	registry := provider.NewRegistry()
	healthTracker := provider.NewInMemoryHealthTracker()
	providerSvc := service.New(fileStore, registry, healthTracker, nil, logger)
	return providerSvc.LoadPersisted()
}

var _ = Describe("SP Registration Integration", Serial, Label("integration"), func() {
	Describe("Startup Behavior", func() {
		Context("with embedded SPs configured", func() {
			It("registers configured embedded SPs at startup (IT-SPR-010)", func() {
				GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container,cluster")
				baseURL, stop := startRealServer()
				DeferCleanup(stop)

				client := &http.Client{Timeout: 2 * time.Second}
				resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
				Expect(err).NotTo(HaveOccurred())
				defer func() { _ = resp.Body.Close() }()

				var list v1alpha1.ProviderList
				Expect(json.NewDecoder(resp.Body).Decode(&list)).To(Succeed())
				Expect(list.Results).NotTo(BeNil())
				Expect(*list.Results).To(HaveLen(2))

				serviceTypes := make([]string, 0, 2)
				for _, p := range *list.Results {
					serviceTypes = append(serviceTypes, p.ServiceType)
					Expect(p.Type).To(HaveValue(Equal(v1alpha1.Embedded)))
				}
				Expect(serviceTypes).To(ConsistOf("container", "cluster"))
			})
		})

		Context("with no embedded SPs configured", func() {
			It("returns empty provider list (IT-SPR-020)", func() {
				GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "")
				baseURL, stop := startRealServer()
				DeferCleanup(stop)

				client := &http.Client{Timeout: 2 * time.Second}
				resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
				Expect(err).NotTo(HaveOccurred())
				defer func() { _ = resp.Body.Close() }()

				var list v1alpha1.ProviderList
				Expect(json.NewDecoder(resp.Body).Decode(&list)).To(Succeed())
				Expect(list.Results).NotTo(BeNil())
				Expect(*list.Results).To(BeEmpty())
			})
		})

		Context("with persisted external SP occupying embedded slot", func() {
			It("skips embedded SP when slot occupied (IT-SPR-030)", func() {
				tmpDir := GinkgoT().TempDir()
				persistPath := filepath.Join(tmpDir, "registrations.json")
				seededData := `[{"id":"ext-container-001","name":"ext-container","endpoint":"https://container.example.com","service_type":"container","schema_version":"v1alpha1","type":"external","create_time":"2026-01-01T00:00:00Z","update_time":"2026-01-01T00:00:00Z"}]`
				Expect(os.WriteFile(persistPath, []byte(seededData), 0o644)).To(Succeed())

				GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", persistPath)
				GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
				baseURL, stop := startRealServer()
				DeferCleanup(stop)

				client := &http.Client{Timeout: 2 * time.Second}
				resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
				Expect(err).NotTo(HaveOccurred())
				defer func() { _ = resp.Body.Close() }()

				var list v1alpha1.ProviderList
				Expect(json.NewDecoder(resp.Body).Decode(&list)).To(Succeed())
				Expect(list.Results).NotTo(BeNil())

				for _, p := range *list.Results {
					if p.ServiceType == "container" {
						Expect(p.Type).To(HaveValue(Equal(v1alpha1.External)))
					}
				}

				By("verifying agent is fully operational")
				healthResp, err := client.Get(baseURL + "/api/v1alpha1/health")
				Expect(err).NotTo(HaveOccurred())
				defer func() { _ = healthResp.Body.Close() }()
				Expect(healthResp.StatusCode).To(Equal(http.StatusOK))
			})
		})
	})

	Describe("External SP Registration", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		It("creates a new external provider with server-set fields (IT-SPR-040)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp.Body).Decode(&p)).To(Succeed())
			Expect(p.Id).NotTo(BeNil())
			Expect(p.Path).NotTo(BeNil())
			Expect(p.CreateTime).NotTo(BeNil())
			Expect(p.UpdateTime).NotTo(BeNil())
			Expect(p.Type).To(HaveValue(Equal(v1alpha1.External)))
		})

		It("re-registration returns 200 with refreshed update_time (IT-SPR-050)", func() {
			client := &http.Client{Timeout: 2 * time.Second}

			resp1, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			var p1 v1alpha1.Provider
			Expect(json.NewDecoder(resp1.Body).Decode(&p1)).To(Succeed())
			_ = resp1.Body.Close()

			time.Sleep(time.Millisecond)

			resp2, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusOK))

			var p2 v1alpha1.Provider
			Expect(json.NewDecoder(resp2.Body).Decode(&p2)).To(Succeed())
			Expect(p2.UpdateTime).NotTo(BeNil())
			Expect(p2.CreateTime).NotTo(BeNil())
			Expect(p2.UpdateTime.After(*p1.CreateTime)).To(BeTrue())
			Expect(p2.CreateTime.Equal(*p1.CreateTime)).To(BeTrue())
			Expect(*p2.Id).To(Equal(*p1.Id))
		})

		It("allows same provider to re-register for same type (IT-SPR-070)", func() {
			client := &http.Client{Timeout: 2 * time.Second}

			body := `{"name":"vm-provider","endpoint":"https://vm.example.com","service_type":"vm","schema_version":"v1alpha1"}`
			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			var p1 v1alpha1.Provider
			Expect(json.NewDecoder(resp1.Body).Decode(&p1)).To(Succeed())
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusOK))
			Expect(resp2.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

			var p2 v1alpha1.Provider
			Expect(json.NewDecoder(resp2.Body).Decode(&p2)).To(Succeed())
			Expect(p2.Name).To(Equal("vm-provider"))
			Expect(*p2.Id).To(Equal(*p1.Id))
		})
	})

	Describe("Service Type Conflict with Embedded SP", func() {
		It("returns 409 when external SP conflicts with embedded slot (IT-SPR-060)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"other-sp","endpoint":"https://other.example.com","service_type":"container","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusConflict))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("CONFLICT"))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("container")))
		})
	})

	Describe("Persistence", func() {
		It("persists registrations across restart (IT-SPR-080)", func() {
			tmpDir := GinkgoT().TempDir()
			GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", filepath.Join(tmpDir, "registrations.json"))

			baseURL1, stop1 := startRealServer()
			DeferCleanup(stop1)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL1+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			stop1()

			baseURL2, stop2 := startRealServer()
			DeferCleanup(stop2)

			resp, err = client.Get(baseURL2 + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			var list v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&list)).To(Succeed())
			Expect(list.Results).NotTo(BeNil())

			found := false
			for _, p := range *list.Results {
				if p.Name == "db-provider" && p.ServiceType == "database" {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "db-provider for database must survive restart")
		})

		It("embedded SP preserves identity across restart (IT-SPR-085)", func() {
			tmpDir := GinkgoT().TempDir()
			GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", filepath.Join(tmpDir, "registrations.json"))
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "test-embedded")

			baseURL1, stop1 := startRealServer()
			DeferCleanup(stop1)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Get(baseURL1 + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			var list1 v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&list1)).To(Succeed())
			_ = resp.Body.Close()
			Expect(list1.Results).NotTo(BeNil())

			var origID string
			var origCreateTime time.Time
			for _, p := range *list1.Results {
				if p.ServiceType == "test-embedded" {
					origID = *p.Id
					origCreateTime = *p.CreateTime
					break
				}
			}
			Expect(origID).NotTo(BeEmpty(), "embedded SP must be listed")

			stop1()

			baseURL2, stop2 := startRealServer()
			DeferCleanup(stop2)

			resp, err = client.Get(baseURL2 + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			var list2 v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&list2)).To(Succeed())
			_ = resp.Body.Close()
			Expect(list2.Results).NotTo(BeNil())

			var newID string
			var newCreateTime time.Time
			for _, p := range *list2.Results {
				if p.ServiceType == "test-embedded" {
					newID = *p.Id
					newCreateTime = *p.CreateTime
					break
				}
			}
			Expect(newID).To(Equal(origID), "embedded SP ID must survive restart")
			Expect(newCreateTime).To(Equal(origCreateTime), "embedded SP create_time must survive restart")
		})

		It("fails fast on corrupted persistence (IT-SPR-170)", func() {
			tmpDir := GinkgoT().TempDir()
			corruptFile := filepath.Join(tmpDir, "registrations.json")
			Expect(os.WriteFile(corruptFile, []byte("{corrupted"), 0o644)).To(Succeed())
			GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", corruptFile)

			startupErr := startWithPersistence(corruptFile)
			Expect(startupErr).To(HaveOccurred())
		})
	})

	Describe("Re-registration with Changed Service Type", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		It("allows move to unoccupied service type (IT-SPR-090)", func() {
			client := &http.Client{Timeout: 2 * time.Second}

			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			resp, err = client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"db-provider","endpoint":"https://sp.example.com:8080","service_type":"analytics","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
			Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp.Body).Decode(&p)).To(Succeed())
			Expect(p.ServiceType).To(Equal("analytics"))

			By("verifying database slot is freed")
			resp3, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"new-db","endpoint":"https://newdb.example.com","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp3.Body.Close() }()
			Expect(resp3.StatusCode).To(Equal(http.StatusCreated))
		})

		It("rejects move to occupied service type (IT-SPR-100)", func() {
			client := &http.Client{Timeout: 2 * time.Second}

			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			resp, err = client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"other-provider","endpoint":"https://other.example.com","service_type":"analytics","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			resp, err = client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"db-provider","endpoint":"https://sp.example.com:8080","service_type":"analytics","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusConflict))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			// Assert the CONFLICTING PROVIDER'S NAME ("other-provider"), not
			// just the service type ("analytics") — disambiguating, since
			// name != service type here (unlike IT-SPR-060's embedded-SP
			// case, where name happens to equal service type and so can't
			// prove the name is actually surfaced).
			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("CONFLICT"))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("other-provider")),
				"conflict detail must name the provider that already holds the service type")
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("analytics")))

			By("verifying db-provider still serves database")
			listResp, err := client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = listResp.Body.Close() }()

			var list v1alpha1.ProviderList
			Expect(json.NewDecoder(listResp.Body).Decode(&list)).To(Succeed())
			Expect(list.Results).NotTo(BeNil())

			found := false
			for _, p := range *list.Results {
				if p.Name == "db-provider" && p.ServiceType == "database" {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "db-provider must still serve database after failed move")
		})
	})

	Describe("Request Validation", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		It("returns 400 for missing required fields (IT-SPR-110)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"test-sp","endpoint":"https://example.com","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))
		})

		It("returns 400 for missing schema_version (IT-SPR-150)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"test-sp","endpoint":"https://example.com","service_type":"database"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))
		})
	})

	Describe("Strict Handler Decode-Failure Wiring", func() {
		// Every field in the current CreateProvider request schema is either
		// an unconstrained string or a readOnly/strictly-formatted value that
		// the OpenAPI validator middleware (nethttpmiddleware.OapiRequestValidatorWithOptions)
		// already rejects before the request reaches the strict handler's own
		// json.Decode — so there is no live end-to-end HTTP payload today that
		// reaches the strict handler's decode step with a body the middleware
		// accepted. That is a property of today's schema, not a guarantee: any
		// future numeric/stricter-Go-type field would make it reachable again.
		// This test isolates the strict-handler composition-root wiring choice
		// directly (bypassing chi routing and the OpenAPI validator middleware
		// entirely) to guard against regressing to the SDK's default
		// (plain-text, non-RFC-7807) RequestErrorHandlerFunc — see REQ-HTTP-091
		// / AC-HTTP-091 and cmd/environment-agent/main.go's identical
		// StrictHTTPServerOptions construction.
		It("returns RFC 7807 problem+json when the strict handler's own JSON decode fails (IT-HTTP-110b)", func() {
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			fileStore, err := store.NewFileStore(filepath.Join(GinkgoT().TempDir(), "registrations.json"), logger)
			Expect(err).NotTo(HaveOccurred())
			providerSvc := service.New(fileStore, provider.NewRegistry(), provider.NewInMemoryHealthTracker(), nil, logger)
			healthSvc := health.NewService(noopMessaging{})
			strictHandler := handler.New(healthSvc, providerSvc)
			h := server.NewStrictHandlerWithOptions(strictHandler, nil, server.StrictHTTPServerOptions{
				RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
					httperror.WriteInvalidArgument(w, r, logger, err.Error())
				},
				ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
					httperror.WriteResponse(w, logger, http.StatusInternalServerError,
						"INTERNAL", "Internal Server Error", err.Error(), &r.RequestURI)
				},
			})

			// "name": 123 is valid JSON syntax but the wrong type for the
			// Provider.Name string field — encoding/json fails to decode it.
			req := httptest.NewRequest(http.MethodPost, "/api/v1alpha1/providers",
				strings.NewReader(`{"name":123}`))
			rec := httptest.NewRecorder()
			h.CreateProvider(rec, req, v1alpha1.CreateProviderParams{})

			Expect(rec.Code).To(Equal(http.StatusBadRequest))
			Expect(rec.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(rec.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("INVALID_ARGUMENT"))
		})
	})

	Describe("Provider ID Handling", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		It("uses query parameter as provider ID (IT-SPR-120)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers?id=custom-001",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp.Body).Decode(&p)).To(Succeed())
			Expect(p.Id).To(HaveValue(Equal("custom-001")))
		})

		It("auto-generates UUID v4 when no ?id= parameter (IT-SPR-130)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp.Body).Decode(&p)).To(Succeed())
			Expect(p.Id).NotTo(BeNil())
			Expect(*p.Id).To(MatchRegexp(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`))
		})

		It("returns 422 for AEP-122 pattern violation (IT-SPR-140)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers?id=INVALID_ID!",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("UNPROCESSABLE_ENTITY"))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("id")))
		})
	})

	Describe("Provider ID Immutability", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		It("returns 409 when re-registering with a different ?id= (IT-SPR-145)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			body := `{"name":"immut-provider","endpoint":"https://sp.example.com","service_type":"immut-svc","schema_version":"v1alpha1"}`

			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers?id=original-id", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.StatusCode).To(Equal(http.StatusCreated))
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers?id=different-id", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusConflict))
			Expect(resp2.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp2.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("CONFLICT"))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("original-id")))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("different-id")))
		})

		It("preserves original ID when re-registering without ?id= (IT-SPR-146)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			body := `{"name":"preserve-provider","endpoint":"https://sp.example.com","service_type":"preserve-svc","schema_version":"v1alpha1"}`

			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers?id=my-stable-id", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.StatusCode).To(Equal(http.StatusCreated))
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusOK))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp2.Body).Decode(&p)).To(Succeed())
			Expect(p.Id).To(HaveValue(Equal("my-stable-id")))
		})

		It("returns 409 with the colliding provider's name when a DIFFERENT provider name requests an already-used ?id= (IT-SPR-148)", func() {
			// Distinct from IT-SPR-145: that test re-registers the SAME
			// provider name with a different ID (hits ensureIDConsistency).
			// This exercises the separate cross-provider-ID-collision branch
			// in assignProviderID (service.go), reachable when a brand NEW
			// provider name requests an ?id= already claimed by a different,
			// existing provider.
			client := &http.Client{Timeout: 2 * time.Second}
			firstBody := `{"name":"collision-holder","endpoint":"https://sp.example.com","service_type":"collision-svc-a","schema_version":"v1alpha1"}`
			secondBody := `{"name":"collision-challenger","endpoint":"https://sp2.example.com","service_type":"collision-svc-b","schema_version":"v1alpha1"}`

			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers?id=shared-id", "application/json", strings.NewReader(firstBody))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.StatusCode).To(Equal(http.StatusCreated))
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers?id=shared-id", "application/json", strings.NewReader(secondBody))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusConflict))
			Expect(resp2.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp2.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("CONFLICT"))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("shared-id")))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("collision-holder")),
				"the conflict detail must name the provider that already holds the requested ID")
		})

		It("succeeds when re-registering with the same ?id= (IT-SPR-147)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			body := `{"name":"same-id-provider","endpoint":"https://sp.example.com","service_type":"same-id-svc","schema_version":"v1alpha1"}`

			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers?id=consistent-id", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.StatusCode).To(Equal(http.StatusCreated))
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers?id=consistent-id", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusOK))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp2.Body).Decode(&p)).To(Succeed())
			Expect(p.Id).To(HaveValue(Equal("consistent-id")))
		})
	})

	Describe("Semantic Validation", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		It("returns 422 for invalid schema_version (IT-SPR-160)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"test-sp","endpoint":"https://example.com","service_type":"database","schema_version":"invalid-version"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("UNPROCESSABLE_ENTITY"))
		})

		It("returns 422 for invalid endpoint URI (IT-SPR-165)", func() {
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"bad-sp","endpoint":"not-a-url","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("UNPROCESSABLE_ENTITY"))
		})
	})

	Describe("Service Type Uniqueness", func() {
		It("enforces one SP per service type (IT-SPR-180)", func() {
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}

			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(validProviderBody()),
			)
			Expect(err).NotTo(HaveOccurred())
			_ = resp.Body.Close()

			resp, err = client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"other-db","endpoint":"https://other.example.com","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusConflict))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("CONFLICT"))
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("database")))
			// name != service type here ("db-provider" vs "database"), so
			// this disambiguates the conflict detail actually naming the
			// holder, not merely echoing the type.
			Expect(errBody.Detail).To(HaveValue(ContainSubstring("db-provider")),
				"conflict detail must name the provider that already holds the service type")
		})
	})

	Describe("Input Normalization", func() {
		var (
			baseURL string
			stop    func()
		)

		BeforeEach(func() {
			baseURL, stop = startRealServer()
			DeferCleanup(stop)
		})

		// ValidateName/ValidateServiceType rejected purely empty (post-trim)
		// values but never trimmed the value actually used for
		// idempotency/collision keys, so "provider1" and "provider1 "
		// registered as distinct providers instead of being treated as the
		// same natural key.
		It("trims leading/trailing whitespace in name so it cannot bypass idempotency (IT-SPR-149)", func() {
			client := &http.Client{Timeout: 2 * time.Second}

			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json",
				strings.NewReader(`{"name":"ws-provider","endpoint":"https://sp.example.com","service_type":"ws-svc-a","schema_version":"v1alpha1"}`))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.StatusCode).To(Equal(http.StatusCreated))
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json",
				strings.NewReader(`{"name":"  ws-provider  ","endpoint":"https://sp2.example.com","service_type":"ws-svc-a","schema_version":"v1alpha1"}`))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusOK),
				"a whitespace-padded name must match the existing provider (idempotent update), not create a second one")

			listResp, err := client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = listResp.Body.Close() }()
			var list v1alpha1.ProviderList
			Expect(json.NewDecoder(listResp.Body).Decode(&list)).To(Succeed())
			Expect(list.Results).NotTo(BeNil())

			count := 0
			for _, p := range *list.Results {
				if p.Name == "ws-provider" {
					count++
				}
			}
			Expect(count).To(Equal(1), "exactly one provider named (trimmed) 'ws-provider' must exist")
		})

		// Symmetric case: whitespace around service_type must not bypass the
		// single-slot-per-service-type invariant (REQ-SPR-200).
		It("trims leading/trailing whitespace in service_type so it cannot bypass slot collision checks (IT-SPR-149b)", func() {
			client := &http.Client{Timeout: 2 * time.Second}

			resp1, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json",
				strings.NewReader(`{"name":"ws-svc-holder","endpoint":"https://sp.example.com","service_type":"ws-database","schema_version":"v1alpha1"}`))
			Expect(err).NotTo(HaveOccurred())
			Expect(resp1.StatusCode).To(Equal(http.StatusCreated))
			_ = resp1.Body.Close()

			resp2, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json",
				strings.NewReader(`{"name":"ws-svc-challenger","endpoint":"https://sp2.example.com","service_type":"  ws-database  ","schema_version":"v1alpha1"}`))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp2.Body.Close() }()

			Expect(resp2.StatusCode).To(Equal(http.StatusConflict),
				"whitespace-padded service_type must still collide with the existing 'ws-database' slot")
		})
	})
})
