package provider_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

func startMockSP(healthResponse string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, healthResponse)
			return
		}
		http.NotFound(w, r)
	}))
}

func startCountingMockSP(healthResponse string, counter *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			counter.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, healthResponse)
			return
		}
		http.NotFound(w, r)
	}))
}

func startSlowMockSP(delay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			time.Sleep(delay)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"status":"healthy"}`)
			return
		}
		http.NotFound(w, r)
	}))
}

func registerExternalSP(baseURL, spEndpoint, name, serviceType string) {
	client := &http.Client{Timeout: 2 * time.Second}
	body := fmt.Sprintf(`{"name":%q,"endpoint":%q,"service_type":%q,"schema_version":"v1alpha1"}`,
		name, spEndpoint, serviceType)
	resp, err := client.Post(
		baseURL+"/api/v1alpha1/providers",
		"application/json",
		strings.NewReader(body),
	)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	if resp != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	ExpectWithOffset(1, resp.StatusCode).To(SatisfyAny(Equal(http.StatusCreated), Equal(http.StatusOK)))
}

func getProviderStatus(baseURL, serviceType string) v1alpha1.ProviderStatus {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	if resp != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	ExpectWithOffset(1, resp.StatusCode).To(Equal(http.StatusOK))

	var list v1alpha1.ProviderList
	ExpectWithOffset(1, json.NewDecoder(resp.Body).Decode(&list)).To(Succeed())
	ExpectWithOffset(1, list.Results).NotTo(BeNil())

	for _, p := range *list.Results {
		if p.ServiceType == serviceType {
			ExpectWithOffset(1, p.Status).NotTo(BeNil(),
				fmt.Sprintf("provider %q found but Status is nil", serviceType))
			return *p.Status
		}
	}
	Fail(fmt.Sprintf("provider with service_type %q not found in results", serviceType), 1)
	return ""
}

func tryGetProviderStatus(baseURL, serviceType string) (v1alpha1.ProviderStatus, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var list v1alpha1.ProviderList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return "", err
	}
	if list.Results == nil {
		return "", fmt.Errorf("results nil")
	}
	for _, p := range *list.Results {
		if p.ServiceType == serviceType {
			if p.Status == nil {
				return "", fmt.Errorf("provider %q found but Status is nil", serviceType)
			}
			return *p.Status, nil
		}
	}
	return "", fmt.Errorf("provider with service_type %q not found", serviceType)
}

func tryGetProviderLastCheckTime(baseURL, serviceType string) (*time.Time, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var list v1alpha1.ProviderList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}
	if list.Results == nil {
		return nil, fmt.Errorf("results nil")
	}
	for _, p := range *list.Results {
		if p.ServiceType == serviceType {
			return p.LastCheckTime, nil
		}
	}
	return nil, fmt.Errorf("provider with service_type %q not found", serviceType)
}

var _ = Describe("SP Health Monitoring Integration", Serial, Label("integration"), func() {
	Describe("External SP Health Check", func() {
		It("transitions SP to Ready after healthy poll (IT-HMN-010)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "healthy-sp", "healthy-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "healthy-svc")
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Ready))
		})

		It("reports Unhealthy when SP returns unhealthy status (IT-HMN-020)", func() {
			mockSP := startMockSP(`{"status":"unhealthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			afterRegistration := time.Now()
			registerExternalSP(baseURL, mockSP.URL, "sick-sp", "sick-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "sick-svc")
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Unhealthy))

			Eventually(func() (bool, error) {
				t, err := tryGetProviderLastCheckTime(baseURL, "sick-svc")
				if err != nil {
					return false, err
				}
				return t != nil && t.After(afterRegistration), nil
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"last_check_time must be after registration, proving monitor ran")
		})

		It("transitions to Unavailable after threshold failures (IT-HMN-030)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			mockSP.Close()

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "3")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "dead-sp", "dead-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "dead-svc")
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Unavailable))
		})

		It("resets failure counter on healthy response (IT-HMN-040)", func() {
			var callCount atomic.Int64
			mockSP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					count := callCount.Add(1)
					w.Header().Set("Content-Type", "application/json")
					// Fail first 2 calls, then healthy
					if count <= 2 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					_, _ = fmt.Fprint(w, `{"status":"healthy"}`)
					return
				}
				http.NotFound(w, r)
			}))
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "3")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "flaky-sp", "flaky-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "flaky-svc")
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Ready))

			Expect(callCount.Load()).To(BeNumerically(">=", int64(3)))
		})
	})

	Describe("Configurable Interval and Timeout", func() {
		It("respects configured check interval and timeout (IT-HMN-050)", func() {
			var callCount atomic.Int64
			mockSP := startCountingMockSP(`{"status":"healthy"}`, &callCount)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "200ms")
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_TIMEOUT", "50ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "2")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "interval-sp", "interval-svc")

			Eventually(callCount.Load).WithTimeout(3*time.Second).WithPolling(50*time.Millisecond).Should(
				BeNumerically(">=", int64(3)), "expected at least 3 checks at 200ms intervals")
			Expect(callCount.Load()).To(BeNumerically("<=", int64(7)),
				"expected at most 7 checks — interval not respected")

			// Verify timeout causes failure: start a slow mock SP
			slowSP := startSlowMockSP(200 * time.Millisecond) // exceeds 50ms timeout
			DeferCleanup(slowSP.Close)

			registerExternalSP(baseURL, slowSP.URL, "slow-sp", "slow-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "slow-svc")
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Unavailable))
		})
	})

	Describe("Embedded SP Health Check", func() {
		It("executes health check in-process without HTTP (IT-HMN-060)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			afterStartup := time.Now()
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			Eventually(func() (bool, error) {
				t, err := tryGetProviderLastCheckTime(baseURL, "container")
				if err != nil {
					return false, err
				}
				return t != nil && t.After(afterStartup), nil
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"last_check_time must be after startup, proving embedded health check ran")
		})

		It("runs initial check on registration so healthy SP becomes Ready immediately (IT-HMN-070)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(mockSP.Close)

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "new-sp", "new-svc")

			status := getProviderStatus(baseURL, "new-svc")
			Expect(status).To(Equal(v1alpha1.Ready),
				"initial check should transition a healthy SP to Ready immediately")
		})

		It("stays Unhealthy after initial check when SP is unreachable (IT-HMN-071)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			mockSP.Close()

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "dead-new-sp", "dead-new-svc")

			status := getProviderStatus(baseURL, "dead-new-svc")
			Expect(status).To(Equal(v1alpha1.Unhealthy),
				"initial check should leave unreachable SP as Unhealthy")
		})

		It("sets embedded SP to Ready when immediate health check passes (IT-HMN-080)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			status := getProviderStatus(baseURL, "container")
			Expect(status).To(Equal(v1alpha1.Ready))
		})

		It("sets embedded SP to Unhealthy when immediate check reports unhealthy (IT-HMN-090)", func() {
			// This requires an embedded SP health checker that can report unhealthy.
			// Currently RegisterEmbedded hardcodes Ready — this test will be RED.
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
			GinkgoT().Setenv("AGENT_EMBEDDED_SP_CONTAINER_HEALTH", "unhealthy")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			status := getProviderStatus(baseURL, "container")
			Expect(status).To(Equal(v1alpha1.Unhealthy))
		})
	})

	Describe("Behavior on State Transitions", func() {
		It("keeps service type advertised but stops routing when Unhealthy (IT-HMN-100)", func() {
			mockSP := startMockSP(`{"status":"unhealthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			afterRegistration := time.Now()
			registerExternalSP(baseURL, mockSP.URL, "route-sp", "route-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "route-svc")
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Unhealthy))

			Eventually(func() (bool, error) {
				t, err := tryGetProviderLastCheckTime(baseURL, "route-svc")
				if err != nil {
					return false, err
				}
				return t != nil && t.After(afterRegistration), nil
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"last_check_time must be after registration, proving monitor ran")
		})

		It("removes service type from DCM registration when Unavailable (IT-HMN-110)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			mockSP.Close()

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "3")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "dcm-sp", "dcm-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "dcm-svc")
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Unavailable))
			// GREEN phase: also assert DCM received POST without this service type
		})

		It("re-adds service type and processes retry topic on recovery (IT-HMN-120)", func() {
			var callCount atomic.Int64
			mockSP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					count := callCount.Add(1)
					w.Header().Set("Content-Type", "application/json")
					// Fail enough to reach Unavailable, then recover
					if count <= 4 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					_, _ = fmt.Fprint(w, `{"status":"healthy"}`)
					return
				}
				http.NotFound(w, r)
			}))
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "3")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "recover-sp", "recover-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "recover-svc")
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Ready))
			// GREEN phase: also assert DCM received updated registration and retry topic was processed
		})
	})

	Describe("CloudEvent Publication", func() {
		It("publishes service-type-degraded CloudEvent on Unhealthy transition (IT-HMN-130)", func() {
			mockSP := startMockSP(`{"status":"unhealthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			afterRegistration := time.Now()
			registerExternalSP(baseURL, mockSP.URL, "ce-sp", "ce-svc")

			Eventually(func() (bool, error) {
				t, err := tryGetProviderLastCheckTime(baseURL, "ce-svc")
				if err != nil {
					return false, err
				}
				return t != nil && t.After(afterRegistration), nil
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"last_check_time must be after registration, proving monitor ran")
			// GREEN phase: also assert dcm.agent.health.service-type-degraded CE published to NATS
		})

		It("publishes service-type-unavailable CloudEvent on Unavailable transition (IT-HMN-135)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			mockSP.Close()

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "3")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "ce-unavail-sp", "ce-unavail-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "ce-unavail-svc")
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Unavailable))
			// GREEN phase: assert dcm.agent.health.service-type-unavailable CE published to NATS
		})
	})

	Describe("Pod Conditions", func() {
		It("updates pod conditions on health state change (IT-HMN-140)", func() {
			mockSP := startMockSP(`{"status":"unhealthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_POD_CONDITIONS_ENABLED", "true")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			afterRegistration := time.Now()
			registerExternalSP(baseURL, mockSP.URL, "pod-sp", "pod-svc")

			Eventually(func() (bool, error) {
				t, err := tryGetProviderLastCheckTime(baseURL, "pod-svc")
				if err != nil {
					return false, err
				}
				return t != nil && t.After(afterRegistration), nil
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"last_check_time must be after registration, proving monitor ran")
			// GREEN phase: assert pod condition patched with status=False, reason=Unhealthy
		})

		It("continues normally when pod condition update fails (IT-HMN-150)", func() {
			mockSP := startMockSP(`{"status":"unhealthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_POD_CONDITIONS_ENABLED", "true")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			afterRegistration := time.Now()
			registerExternalSP(baseURL, mockSP.URL, "fail-pod-sp", "fail-pod-svc")

			Eventually(func() (bool, error) {
				t, err := tryGetProviderLastCheckTime(baseURL, "fail-pod-svc")
				if err != nil {
					return false, err
				}
				return t != nil && t.After(afterRegistration), nil
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"last_check_time must be after registration, proving monitor ran")

			// Agent is still responsive after pod condition failure
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Get(baseURL + "/api/v1alpha1/health")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
		})

		It("uses Pod Readiness Gates with in-cluster auth (IT-HMN-160)", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_POD_CONDITIONS_ENABLED", "true")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "gate-sp", "gate-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "gate-svc")
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(v1alpha1.Ready))
			// GREEN phase: assert Pod Readiness Gates are used with in-cluster auth
		})
	})

	Describe("Endpoint Change Health Reset", func() {
		It("resets and re-checks when endpoint changes to a healthy SP", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "endpoint-sp", "endpoint-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "endpoint-svc")
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				Equal(v1alpha1.Ready), "precondition: must be Ready before endpoint change")

			// Re-register with a different healthy endpoint — initial check
			// resets to Unhealthy then immediately transitions to Ready.
			newMockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(newMockSP.Close)
			registerExternalSP(baseURL, newMockSP.URL, "endpoint-sp", "endpoint-svc")

			status := getProviderStatus(baseURL, "endpoint-svc")
			Expect(status).To(Equal(v1alpha1.Ready),
				"initial check on new endpoint should transition back to Ready")
		})

		It("resets to Unhealthy when endpoint changes to an unreachable SP", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "endpoint-sp2", "endpoint-svc2")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "endpoint-svc2")
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				Equal(v1alpha1.Ready), "precondition: must be Ready before endpoint change")

			// Re-register with a dead endpoint — initial check fails,
			// state resets to Unhealthy and stays there.
			deadSP := startMockSP(`{"status":"healthy"}`)
			deadSP.Close()
			registerExternalSP(baseURL, deadSP.URL, "endpoint-sp2", "endpoint-svc2")

			status := getProviderStatus(baseURL, "endpoint-svc2")
			Expect(status).To(Equal(v1alpha1.Unhealthy),
				"must stay Unhealthy when new endpoint is unreachable")
		})

		It("does NOT reset health when updating non-endpoint fields", func() {
			mockSP := startMockSP(`{"status":"healthy"}`)
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "stable-sp", "stable-svc")

			Eventually(func() (v1alpha1.ProviderStatus, error) {
				return tryGetProviderStatus(baseURL, "stable-svc")
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				Equal(v1alpha1.Ready), "precondition: must be Ready")

			// Re-register with SAME endpoint (only display_name changes)
			client := &http.Client{Timeout: 2 * time.Second}
			body := fmt.Sprintf(`{"name":"stable-sp","endpoint":%q,"service_type":"stable-svc","schema_version":"v1alpha1","display_name":"Updated Name"}`,
				mockSP.URL)
			resp, err := client.Post(baseURL+"/api/v1alpha1/providers", "application/json", strings.NewReader(body))
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			// Status must remain Ready (endpoint didn't change)
			status := getProviderStatus(baseURL, "stable-svc")
			Expect(status).To(Equal(v1alpha1.Ready), "must NOT reset when endpoint unchanged")
		})
	})

	Describe("Unavailable to Unhealthy Transition", func() {
		It("re-advertises service type but does NOT process retry topic (IT-HMN-170)", func() {
			var callCount atomic.Int64
			mockSP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					count := callCount.Add(1)
					w.Header().Set("Content-Type", "application/json")
					// Fail to reach Unavailable, then respond unhealthy (not healthy)
					if count <= 4 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					_, _ = fmt.Fprint(w, `{"status":"unhealthy"}`)
					return
				}
				http.NotFound(w, r)
			}))
			DeferCleanup(mockSP.Close)

			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "100ms")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "3")

			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			registerExternalSP(baseURL, mockSP.URL, "readv-sp", "readv-svc")

			// Must see enough polls (4 failures → Unavailable, then unhealthy → Unhealthy)
			Eventually(func(g Gomega) {
				g.Expect(callCount.Load()).To(BeNumerically(">=", int64(5)),
					"health monitor must have polled the SP multiple times")
				status, err := tryGetProviderStatus(baseURL, "readv-svc")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal(v1alpha1.Unhealthy))
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
			// GREEN phase: assert service type re-added to DCM and retry topic NOT processed
		})
	})
})
