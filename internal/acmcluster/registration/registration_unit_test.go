package registration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	spmv1alpha1 "github.com/dcm-project/service-provider-manager/api/v1alpha1/provider"
	spmclient "github.com/dcm-project/service-provider-manager/pkg/client/provider"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/registration"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// syncBuffer is a goroutine-safe bytes.Buffer for capturing log output
// shared between the registrar goroutine (writer) and the test goroutine (reader).
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// newClusterImageSet creates an unstructured ClusterImageSet with the given
// name and OCP release image tag (e.g., "4.17.3-multi").
func newClusterImageSet(name, releaseTag string) *unstructured.Unstructured {
	cis := &unstructured.Unstructured{}
	cis.SetGroupVersionKind(util.ClusterImageSetGVK)
	cis.SetName(name)
	_ = unstructured.SetNestedField(
		cis.Object,
		"quay.io/openshift-release-dev/ocp-release:"+releaseTag,
		"spec", "releaseImage",
	)
	return cis
}

// newFakeK8sClient builds a controller-runtime fake client with a scheme that
// knows about ClusterImageSet (unstructured) and pre-populated objects.
func newFakeK8sClient(objects ...*unstructured.Unstructured) *fake.ClientBuilder {
	s := runtime.NewScheme()

	s.AddKnownTypeWithName(util.ClusterImageSetGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(util.ClusterImageSetListGVK, &unstructured.UnstructuredList{})

	builder := fake.NewClientBuilder().WithScheme(s)
	if len(objects) > 0 {
		runtimeObjs := make([]runtime.Object, len(objects))
		for i, o := range objects {
			runtimeObjs[i] = o.DeepCopy()
		}
		builder = builder.WithRuntimeObjects(runtimeObjs...)
	}
	return builder
}

// defaultRegistrationConfig returns a RegistrationConfig suitable for unit tests
// with short intervals so async tests converge quickly.
func defaultRegistrationConfig(serverURL string) config.RegistrationConfig {
	return config.RegistrationConfig{
		DCMRegistrationURL:         serverURL,
		ProviderName:               "acm-cluster-sp",
		ProviderEndpoint:           "https://my-sp.example.com",
		RegistrationInitialBackoff: 20 * time.Millisecond,
		RegistrationMaxBackoff:     200 * time.Millisecond,
		VersionCheckInterval:       100 * time.Millisecond,
		ProviderDisplayName:        "ACM Cluster SP",
		ProviderRegion:             "us-east-1",
		ProviderZone:               "az-1",
	}
}

var _ = Describe("Registration", func() {
	var (
		mockServer *httptest.Server
		logBuf     *syncBuffer
	)

	AfterEach(func() {
		if mockServer != nil {
			mockServer.Close()
			mockServer = nil
		}
	})

	// ---------------------------------------------------------------
	// TC-REG-UT-001: Successful first-time registration with capabilities
	// ---------------------------------------------------------------
	Describe("Register()", func() {
		It("sends a POST to /providers with correct capabilities (TC-REG-UT-001)", func() {
			var receivedBody spmv1alpha1.Provider
			var receivedMethod string
			var receivedPath string
			var requestReceived atomic.Bool

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedMethod = r.Method
				receivedPath = r.URL.Path
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &receivedBody)
				requestReceived.Store(true)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				resp := spmv1alpha1.Provider{
					Name:          receivedBody.Name,
					Endpoint:      receivedBody.Endpoint,
					ServiceType:   receivedBody.ServiceType,
					SchemaVersion: receivedBody.SchemaVersion,
				}
				_ = json.NewEncoder(w).Encode(resp)
			}))

			cis416 := newClusterImageSet("img4.16.0-multi", "4.16.0-multi")
			cis417 := newClusterImageSet("img4.17.3-multi", "4.17.3-multi")
			k8sClient := newFakeK8sClient(cis416, cis417).Build()

			cfg := defaultRegistrationConfig(mockServer.URL)
			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			regErr := reg.Register(context.Background())

			// RED: Register() returns "not implemented", so we expect the mock server
			// was never called and the registration payload is never sent.
			Expect(regErr).NotTo(HaveOccurred(),
				"Register() should succeed when DCM returns 201")
			Expect(requestReceived.Load()).To(BeTrue(),
				"mock server should have received a registration request")
			Expect(receivedMethod).To(Equal(http.MethodPost))
			Expect(receivedPath).To(Equal("/providers"))
			Expect(receivedBody.ServiceType).To(Equal("cluster"))
			Expect(receivedBody.Endpoint).To(Equal(cfg.ProviderEndpoint + "/api/v1alpha1/clusters"))
			Expect(receivedBody.Operations).NotTo(BeNil())
			Expect(*receivedBody.Operations).To(ConsistOf("CREATE", "DELETE", "READ"))

			// Verify metadata capabilities
			Expect(receivedBody.Metadata).NotTo(BeNil())
			sp, spFound := receivedBody.Metadata.Get("supportedPlatforms")
			Expect(spFound).To(BeTrue(), "metadata should include supportedPlatforms")
			Expect(sp).To(ConsistOf("kubevirt", "baremetal"))

			spt, sptFound := receivedBody.Metadata.Get("supportedProvisioningTypes")
			Expect(sptFound).To(BeTrue(), "metadata should include supportedProvisioningTypes")
			Expect(spt).To(ConsistOf("hypershift"))

			ksv, ksvFound := receivedBody.Metadata.Get("kubernetesSupportedVersions")
			Expect(ksvFound).To(BeTrue(), "metadata should include kubernetesSupportedVersions")
			versions, ok := ksv.([]interface{})
			Expect(ok).To(BeTrue())
			versionStrings := make([]string, len(versions))
			for i, v := range versions {
				versionStrings[i] = v.(string)
			}
			sort.Strings(versionStrings)
			Expect(versionStrings).To(Equal([]string{"1.29", "1.30"}))

			// REQ-REG-020: display_name, region, zone
			Expect(receivedBody.DisplayName).NotTo(BeNil())
			Expect(*receivedBody.DisplayName).To(Equal("ACM Cluster SP"))
			Expect(receivedBody.Metadata.RegionCode).NotTo(BeNil())
			Expect(*receivedBody.Metadata.RegionCode).To(Equal("us-east-1"))
			Expect(receivedBody.Metadata.Zone).NotTo(BeNil())
			Expect(*receivedBody.Metadata.Zone).To(Equal("az-1"))
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-002: Idempotent re-registration
		// ---------------------------------------------------------------
		It("proceeds normally when DCM returns 200 for existing provider (TC-REG-UT-002)", func() {
			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				resp := spmv1alpha1.Provider{
					Name:          "acm-cluster-sp",
					Endpoint:      "https://my-sp.example.com/api/v1alpha1",
					ServiceType:   "cluster",
					SchemaVersion: "v1alpha1",
				}
				_ = json.NewEncoder(w).Encode(resp)
			}))

			k8sClient := newFakeK8sClient().Build()
			cfg := defaultRegistrationConfig(mockServer.URL)
			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			// RED: Register() returns "not implemented", so this will fail.
			regErr := reg.Register(context.Background())
			Expect(regErr).NotTo(HaveOccurred(),
				"Register() should succeed when DCM returns 200 (existing provider)")
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-007: No ClusterImageSets at startup
		// ---------------------------------------------------------------
		It("registers with empty kubernetesSupportedVersions when no CIS exist (TC-REG-UT-007)", func() {
			var receivedBody spmv1alpha1.Provider
			var requestReceived atomic.Bool

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &receivedBody)
				requestReceived.Store(true)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(spmv1alpha1.Provider{
					Name: "acm-cluster-sp", Endpoint: "https://my-sp.example.com/api/v1alpha1",
					ServiceType: "cluster", SchemaVersion: "v1alpha1",
				})
			}))

			// No ClusterImageSets
			k8sClient := newFakeK8sClient().Build()
			cfg := defaultRegistrationConfig(mockServer.URL)
			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			// RED: Register() returns "not implemented"
			regErr := reg.Register(context.Background())
			Expect(regErr).NotTo(HaveOccurred(),
				"Register() should succeed even with no ClusterImageSets")
			Expect(requestReceived.Load()).To(BeTrue(),
				"mock server should have received the registration request")

			Expect(receivedBody.Metadata).NotTo(BeNil())
			ksv, ksvFound := receivedBody.Metadata.Get("kubernetesSupportedVersions")
			Expect(ksvFound).To(BeTrue(), "metadata should include kubernetesSupportedVersions")
			versions, ok := ksv.([]interface{})
			Expect(ok).To(BeTrue())
			Expect(versions).To(BeEmpty(),
				"kubernetesSupportedVersions should be empty when no CIS exist")
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-011: Registration uses DCM client library
		// ---------------------------------------------------------------
		It("uses the DCM client library, not raw HTTP (TC-REG-UT-011)", func() {
			var requestReceived atomic.Bool

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestReceived.Store(true)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(spmv1alpha1.Provider{
					Name: "acm-cluster-sp", Endpoint: "https://my-sp.example.com/api/v1alpha1",
					ServiceType: "cluster", SchemaVersion: "v1alpha1",
				})
			}))

			k8sClient := newFakeK8sClient().Build()
			cfg := defaultRegistrationConfig(mockServer.URL)
			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			// RED: Register() returns "not implemented" without using dcmClient at all.
			regErr := reg.Register(context.Background())
			Expect(regErr).NotTo(HaveOccurred(),
				"Register() should succeed via DCM client")
			Expect(requestReceived.Load()).To(BeTrue(),
				"Register() should use the DCM client to POST to the registry")
		})
	})

	// ---------------------------------------------------------------
	// TC-REG-UT-003: Registry unreachable with infinite retry
	// ---------------------------------------------------------------
	Describe("Start() retry behavior", func() {
		It("retries registration indefinitely until context cancellation (TC-REG-UT-003)", func() {
			var requestCount atomic.Int32

			// Create and immediately close the server so connections fail.
			closedServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
				requestCount.Add(1)
			}))
			closedServerURL := closedServer.URL
			closedServer.Close()

			k8sClient := newFakeK8sClient().Build()
			cfg := defaultRegistrationConfig(closedServerURL)
			cfg.RegistrationInitialBackoff = 10 * time.Millisecond
			cfg.RegistrationMaxBackoff = 50 * time.Millisecond

			dcmClient, err := spmclient.NewClientWithResponses(closedServerURL)
			Expect(err).NotTo(HaveOccurred())

			logBuf = &syncBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			reg.Start(ctx)

			// Wait for several retries to prove it retries indefinitely.
			Eventually(logBuf.String).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				ContainSubstring("retry"),
				"Start() should log retry attempts when registry is unreachable",
			)

			// Cancel context and verify goroutine exits via Done().
			cancel()
			Eventually(reg.Done()).WithTimeout(2*time.Second).Should(BeClosed(),
				"registration goroutine should exit after context cancellation")
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-009: Registry returns non-2xx server errors (two sub-scenarios)
		// ---------------------------------------------------------------
		Context("server error (500)", func() {
			It("retries with backoff on 5xx responses (TC-REG-UT-009a)", func() {
				var requestCount atomic.Int32

				mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requestCount.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(spmv1alpha1.Error{
						Title: "Internal Server Error",
						Type:  string(v1alpha1.ErrorTypeINTERNAL),
					})
				}))

				k8sClient := newFakeK8sClient().Build()
				cfg := defaultRegistrationConfig(mockServer.URL)
				cfg.RegistrationInitialBackoff = 10 * time.Millisecond

				dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
				Expect(err).NotTo(HaveOccurred())

				logBuf = &syncBuffer{}
				logger := slog.New(slog.NewJSONHandler(logBuf, nil))
				reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				reg.Start(ctx)

				Eventually(requestCount.Load).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
					BeNumerically(">=", 2),
					"Start() should retry on 500 errors",
				)

				// REQ-REG-050: registration failures logged with detail
				Eventually(logBuf.String).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
					ContainSubstring("retry"),
					"registration failure should be logged with retry detail",
				)
			})
		})

		Context("client error (400)", func() {
			It("does NOT retry on 4xx responses (TC-REG-UT-009b)", func() {
				var requestCount atomic.Int32

				mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requestCount.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(spmv1alpha1.Error{
						Title: "Bad Request",
						Type:  "VALIDATION",
					})
				}))

				k8sClient := newFakeK8sClient().Build()
				cfg := defaultRegistrationConfig(mockServer.URL)
				cfg.RegistrationInitialBackoff = 10 * time.Millisecond

				dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
				Expect(err).NotTo(HaveOccurred())

				logBuf = &syncBuffer{}
				logger := slog.New(slog.NewJSONHandler(logBuf, nil))
				reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				reg.Start(ctx)

				// Wait for the goroutine to exit (4xx = non-retryable = immediate stop).
				Eventually(reg.Done()).WithTimeout(2*time.Second).Should(BeClosed(),
					"registration goroutine should exit after non-retryable error")

				// Verify only 1 request was sent (no retries on 4xx).
				Expect(requestCount.Load()).To(BeNumerically("==", 1),
					"Start() should NOT retry on 400 client errors")

				// REQ-REG-050: non-retryable failure logged
				Expect(logBuf.String()).To(
					ContainSubstring("non-retryable"),
					"non-retryable failure should be logged",
				)
			})
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-010b: Calling Start() twice does not launch duplicate goroutines
		// ---------------------------------------------------------------
		It("does not launch duplicate goroutines on repeated Start() calls (TC-REG-UT-010b)", func() {
			var requestCount atomic.Int32

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestCount.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(spmv1alpha1.Provider{
					Name: "acm-cluster-sp", Endpoint: "https://my-sp.example.com/api/v1alpha1",
					ServiceType: "cluster", SchemaVersion: "v1alpha1",
				})
			}))

			k8sClient := newFakeK8sClient().Build()
			cfg := defaultRegistrationConfig(mockServer.URL)
			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logBuf = &syncBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Call Start() twice.
			reg.Start(ctx)
			reg.Start(ctx)

			// Wait for initial registration to complete.
			Eventually(requestCount.Load).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				BeNumerically(">=", 1),
				"at least one registration request should be sent",
			)

			// Only one goroutine should have been launched, so only 1 registration
			// request should have been sent initially (not 2).
			Consistently(requestCount.Load, 200*time.Millisecond, 50*time.Millisecond).Should(
				BeNumerically("==", 1),
				"second Start() should not launch a duplicate goroutine",
			)
		})

		// ---------------------------------------------------------------
		// Done() channel
		// ---------------------------------------------------------------
		It("closes Done() when the registration goroutine exits (TC-REG-UT-Done)", func() {
			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"type":"VALIDATION","title":"Bad Request"}`))
			}))

			k8sClient := newFakeK8sClient().Build()
			cfg := defaultRegistrationConfig(mockServer.URL)
			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			reg.Start(ctx)

			// 4xx = non-retryable, so Done() should close quickly.
			Eventually(reg.Done()).WithTimeout(2*time.Second).Should(BeClosed(),
				"Done() should be closed after registration goroutine exits")
		})
	})

	// ---------------------------------------------------------------
	// TC-REG-UT-005 / TC-REG-UT-008 / TC-REG-UT-010: Version watching
	// ---------------------------------------------------------------
	Describe("Start() version watching", func() {
		// ---------------------------------------------------------------
		// TC-REG-UT-005: Version refresh triggers re-registration
		// ---------------------------------------------------------------
		It("re-registers when new ClusterImageSets appear (TC-REG-UT-005)", func() {
			var mu sync.Mutex
			var registrations [][]byte

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				registrations = append(registrations, body)
				mu.Unlock()

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(spmv1alpha1.Provider{
					Name: "acm-cluster-sp", Endpoint: "https://my-sp.example.com/api/v1alpha1",
					ServiceType: "cluster", SchemaVersion: "v1alpha1",
				})
			}))

			cis416 := newClusterImageSet("img4.16.0-multi", "4.16.0-multi")
			cis417 := newClusterImageSet("img4.17.3-multi", "4.17.3-multi")
			k8sClient := newFakeK8sClient(cis416, cis417).Build()

			cfg := defaultRegistrationConfig(mockServer.URL)
			cfg.VersionCheckInterval = 100 * time.Millisecond

			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logBuf = &syncBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// RED: Start() is a no-op, so no initial registration or version watching happens.
			reg.Start(ctx)

			// Wait for initial registration.
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(registrations)
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				BeNumerically(">=", 1),
				"Start() should perform initial registration",
			)

			// Simulate new CIS appearing by creating a new object via the K8s client.
			cis418 := newClusterImageSet("img4.18.0-multi", "4.18.0-multi")
			Expect(k8sClient.Create(ctx, cis418)).To(Succeed())

			// Wait for re-registration with updated versions.
			Eventually(func() bool {
				mu.Lock()
				defer mu.Unlock()
				for _, body := range registrations {
					var p spmv1alpha1.Provider
					if err := json.Unmarshal(body, &p); err == nil {
						if p.Metadata != nil {
							ksv, found := p.Metadata.Get("kubernetesSupportedVersions")
							if found {
								if versions, ok := ksv.([]interface{}); ok {
									for _, v := range versions {
										if v == "1.31" {
											return true
										}
									}
								}
							}
						}
					}
				}
				return false
			}).WithTimeout(3*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"Start() should re-register with 1.31 after CIS for OCP 4.18.0 appears",
			)
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-008: ClusterImageSet deletion triggers re-registration
		// ---------------------------------------------------------------
		It("re-registers when ClusterImageSets are deleted (TC-REG-UT-008)", func() {
			var mu sync.Mutex
			var registrations [][]byte

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				registrations = append(registrations, body)
				mu.Unlock()

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(spmv1alpha1.Provider{
					Name: "acm-cluster-sp", Endpoint: "https://my-sp.example.com/api/v1alpha1",
					ServiceType: "cluster", SchemaVersion: "v1alpha1",
				})
			}))

			cis416 := newClusterImageSet("img4.16.0-multi", "4.16.0-multi")
			cis417 := newClusterImageSet("img4.17.3-multi", "4.17.3-multi")
			cis418 := newClusterImageSet("img4.18.0-multi", "4.18.0-multi")
			k8sClient := newFakeK8sClient(cis416, cis417, cis418).Build()

			cfg := defaultRegistrationConfig(mockServer.URL)
			cfg.VersionCheckInterval = 100 * time.Millisecond

			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logBuf = &syncBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// RED: Start() is a no-op.
			reg.Start(ctx)

			// Wait for initial registration with all three versions.
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(registrations)
			}).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				BeNumerically(">=", 1),
				"Start() should perform initial registration",
			)

			// Delete the CIS for OCP 4.18.0.
			Expect(k8sClient.Delete(ctx, cis418)).To(Succeed())

			// Wait for re-registration that no longer includes 1.31.
			Eventually(func() bool {
				mu.Lock()
				defer mu.Unlock()
				if len(registrations) < 2 {
					return false
				}
				// Check the latest registration body.
				lastBody := registrations[len(registrations)-1]
				var p spmv1alpha1.Provider
				if err := json.Unmarshal(lastBody, &p); err != nil {
					return false
				}
				if p.Metadata == nil {
					return false
				}
				ksv, found := p.Metadata.Get("kubernetesSupportedVersions")
				if !found {
					return false
				}
				versions, ok := ksv.([]interface{})
				if !ok {
					return false
				}
				// Should have 1.29 and 1.30 but NOT 1.31.
				has129 := false
				has130 := false
				has131 := false
				for _, v := range versions {
					switch v {
					case "1.29":
						has129 = true
					case "1.30":
						has130 = true
					case "1.31":
						has131 = true
					}
				}
				return has129 && has130 && !has131
			}).WithTimeout(3*time.Second).WithPolling(50*time.Millisecond).Should(BeTrue(),
				"Start() should re-register with [1.29, 1.30] after CIS for 4.18.0 is deleted",
			)
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-010: Version check detects changes but re-registration fails
		// ---------------------------------------------------------------
		It("retries re-registration and logs error when DCM returns 500 (TC-REG-UT-010)", func() {
			var requestCount atomic.Int32
			firstRegistration := true
			var firstRegMu sync.Mutex

			mockServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestCount.Add(1)
				w.Header().Set("Content-Type", "application/json")

				firstRegMu.Lock()
				isFirst := firstRegistration
				firstRegistration = false
				firstRegMu.Unlock()

				if isFirst {
					// First registration succeeds.
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(spmv1alpha1.Provider{
						Name: "acm-cluster-sp", Endpoint: "https://my-sp.example.com/api/v1alpha1",
						ServiceType: "cluster", SchemaVersion: "v1alpha1",
					})
				} else {
					// Subsequent re-registrations fail with 500.
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(spmv1alpha1.Error{
						Title: "Internal Server Error",
						Type:  string(v1alpha1.ErrorTypeINTERNAL),
					})
				}
			}))

			cis416 := newClusterImageSet("img4.16.0-multi", "4.16.0-multi")
			k8sClient := newFakeK8sClient(cis416).Build()

			cfg := defaultRegistrationConfig(mockServer.URL)
			cfg.VersionCheckInterval = 100 * time.Millisecond
			cfg.RegistrationInitialBackoff = 10 * time.Millisecond

			dcmClient, err := spmclient.NewClientWithResponses(mockServer.URL)
			Expect(err).NotTo(HaveOccurred())

			logBuf = &syncBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			reg := registration.New(cfg, dcmClient, k8sClient, logger, registration.DefaultCompatibilityMatrix)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// RED: Start() is a no-op.
			reg.Start(ctx)

			// Wait for initial registration.
			Eventually(requestCount.Load).WithTimeout(2*time.Second).WithPolling(50*time.Millisecond).Should(
				BeNumerically(">=", 1),
				"Start() should perform initial registration",
			)

			// Add a new CIS to trigger version change detection.
			cis417 := newClusterImageSet("img4.17.0-multi", "4.17.0-multi")
			Expect(k8sClient.Create(ctx, cis417)).To(Succeed())

			// The version watcher should detect the change and attempt re-registration,
			// which will fail with 500. It should retry and log errors.
			Eventually(requestCount.Load).WithTimeout(3*time.Second).WithPolling(50*time.Millisecond).Should(
				BeNumerically(">=", 2),
				"Start() should attempt re-registration when versions change",
			)

			Eventually(logBuf.String).WithTimeout(3*time.Second).WithPolling(50*time.Millisecond).Should(
				SatisfyAny(
					ContainSubstring("error"),
					ContainSubstring("failed"),
					ContainSubstring("retry"),
				),
				"Start() should log errors when re-registration fails",
			)
		})
	})

	// ---------------------------------------------------------------
	// TC-REG-UT-012 / TC-REG-UT-013: VersionDiscoverer
	// ---------------------------------------------------------------
	Describe("VersionDiscoverer.DiscoverVersions()", func() {
		// ---------------------------------------------------------------
		// TC-REG-UT-012: ClusterImageSet without matrix mapping is not advertised
		// ---------------------------------------------------------------
		It("excludes CIS versions not in the compatibility matrix (TC-REG-UT-012)", func() {
			cis416 := newClusterImageSet("img4.16.0-multi", "4.16.0-multi")
			cis417 := newClusterImageSet("img4.17.0-multi", "4.17.0-multi")
			// 4.22 is NOT in the DefaultCompatibilityMatrix
			cis422 := newClusterImageSet("img4.22.0-multi", "4.22.0-multi")
			k8sClient := newFakeK8sClient(cis416, cis417, cis422).Build()

			discoverer := registration.NewVersionDiscoverer(k8sClient, registration.DefaultCompatibilityMatrix)

			// RED: DiscoverVersions() returns nil, fmt.Errorf("not implemented")
			versions, err := discoverer.DiscoverVersions(context.Background())
			Expect(err).NotTo(HaveOccurred(),
				"DiscoverVersions() should not return an error")
			Expect(versions).NotTo(BeNil(),
				"DiscoverVersions() should return a non-nil slice")

			sort.Strings(versions)
			Expect(versions).To(Equal([]string{"1.29", "1.30"}),
				"versions should include 1.29 (4.16) and 1.30 (4.17) but NOT anything for 4.22")
		})

		// ---------------------------------------------------------------
		// TC-REG-UT-013: Matrix entry without matching CIS is not advertised
		// ---------------------------------------------------------------
		It("only advertises versions for CIS that actually exist (TC-REG-UT-013)", func() {
			// Only one CIS for OCP 4.16.0; matrix has entries for 4.14-4.18
			cis416 := newClusterImageSet("img4.16.0-multi", "4.16.0-multi")
			k8sClient := newFakeK8sClient(cis416).Build()

			discoverer := registration.NewVersionDiscoverer(k8sClient, registration.DefaultCompatibilityMatrix)

			// RED: DiscoverVersions() returns nil, fmt.Errorf("not implemented")
			versions, err := discoverer.DiscoverVersions(context.Background())
			Expect(err).NotTo(HaveOccurred(),
				"DiscoverVersions() should not return an error")
			Expect(versions).NotTo(BeNil(),
				"DiscoverVersions() should return a non-nil slice")

			Expect(versions).To(Equal([]string{"1.29"}),
				"versions should only include 1.29 (from CIS 4.16), not other matrix entries")
		})
	})

	Describe("LoadCompatibilityMatrix", func() {
		It("returns default matrix when path is empty", func() {
			matrix, err := registration.LoadCompatibilityMatrix("")
			Expect(err).NotTo(HaveOccurred())
			Expect(matrix).To(Equal(registration.DefaultCompatibilityMatrix))
		})

		It("loads matrix from JSON file", func() {
			content := `{"4.16": "1.29", "4.17": "1.30"}`
			tmpFile := GinkgoT().TempDir() + "/matrix.json"
			Expect(os.WriteFile(tmpFile, []byte(content), 0o644)).To(Succeed())

			matrix, err := registration.LoadCompatibilityMatrix(tmpFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(matrix).To(HaveLen(2))
			Expect(matrix["4.16"]).To(Equal("1.29"))
			Expect(matrix["4.17"]).To(Equal("1.30"))
		})

		It("returns error for non-existent file", func() {
			_, err := registration.LoadCompatibilityMatrix("/nonexistent/path.json")
			Expect(err).To(HaveOccurred())
		})

		It("returns error for invalid JSON", func() {
			tmpFile := GinkgoT().TempDir() + "/bad.json"
			Expect(os.WriteFile(tmpFile, []byte("not json"), 0o644)).To(Succeed())

			_, err := registration.LoadCompatibilityMatrix(tmpFile)
			Expect(err).To(HaveOccurred())
		})
	})
})

// Compile-time assertion: ensure we import packages that are used. The blank
// identifier prevents "imported and not used" errors for packages that are
// only used inside Eventually/Consistently closures and may be optimised away.
var _ = bytes.Compare
