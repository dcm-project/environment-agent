package provider_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

var _ = Describe("Provider Query Endpoints Integration", Serial, Label("integration"), func() {
	Describe("List Providers (GET /api/v1alpha1/providers)", func() {
		It("returns all providers with health fields (IT-STS-010)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"db-provider","endpoint":"https://sp.example.com:8080","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			_ = resp.Body.Close()

			resp, err = client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var listResp v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&listResp)).To(Succeed())
			Expect(listResp.Results).NotTo(BeNil())
			Expect(*listResp.Results).To(HaveLen(2))

			serviceTypes := make([]string, 0, 2)
			for _, p := range *listResp.Results {
				serviceTypes = append(serviceTypes, p.ServiceType)
			}
			Expect(serviceTypes).To(ContainElements("container", "database"))

			for _, p := range *listResp.Results {
				Expect(p.Type).NotTo(BeNil(), "provider %s: Type", p.ServiceType)
				Expect(p.Status).NotTo(BeNil(), "provider %s: Status", p.ServiceType)
				Expect(p.LastCheckTime).NotTo(BeNil(), "provider %s: LastCheckTime", p.ServiceType)
			}
		})

		It("returns 200 with empty results when no providers (IT-STS-020)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var listResp v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&listResp)).To(Succeed())
			Expect(listResp.Results).NotTo(BeNil())
			Expect(*listResp.Results).To(BeEmpty())
		})

		It("includes all providers regardless of health state (IT-STS-030)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}

			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"db-provider","endpoint":"https://db.example.com:8080","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			_ = resp.Body.Close()

			resp, err = client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"net-provider","endpoint":"https://net.example.com:8080","service_type":"network","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			_ = resp.Body.Close()

			resp, err = client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var listResp v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&listResp)).To(Succeed())
			Expect(listResp.Results).NotTo(BeNil())
			Expect(*listResp.Results).To(HaveLen(3))

			serviceTypes := make([]string, 0, 3)
			for _, p := range *listResp.Results {
				serviceTypes = append(serviceTypes, p.ServiceType)
			}
			Expect(serviceTypes).To(ContainElements("container", "database", "network"))
		})

		It("reflects real-time health state on provider (IT-STS-060)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers",
				"application/json",
				strings.NewReader(`{"name":"ext-db","endpoint":"https://sp.example.com:8080","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			_ = resp.Body.Close()

			resp, err = client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var listResp v1alpha1.ProviderList
			Expect(json.NewDecoder(resp.Body).Decode(&listResp)).To(Succeed())
			Expect(listResp.Results).NotTo(BeNil())
			Expect(*listResp.Results).To(HaveLen(1))
			Expect((*listResp.Results)[0].Status).To(HaveValue(Equal(v1alpha1.Unhealthy)))
		})
	})

	Describe("Get Provider (GET /api/v1alpha1/providers/{provider_id})", func() {
		It("returns provider with health fields by ID (IT-STS-040)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers?id=sp-db-001",
				"application/json",
				strings.NewReader(`{"name":"db-provider","endpoint":"https://sp.example.com:8080","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			_ = resp.Body.Close()

			resp, err = client.Get(baseURL + "/api/v1alpha1/providers/sp-db-001")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var p v1alpha1.Provider
			Expect(json.NewDecoder(resp.Body).Decode(&p)).To(Succeed())
			Expect(p.Name).To(Equal("db-provider"))
			Expect(p.ServiceType).To(Equal("database"))
			Expect(p.Type).NotTo(BeNil())
			Expect(p.Status).NotTo(BeNil())
			Expect(p.LastCheckTime).NotTo(BeNil())
		})

		It("returns 404 for non-existent provider (IT-STS-050)", func() {
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Get(baseURL + "/api/v1alpha1/providers/nonexistent")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = resp.Body.Close() }()

			Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
			Expect(resp.Header.Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(resp.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Status).To(HaveValue(Equal(404)))
			Expect(errBody.Type).NotTo(BeEmpty())
			Expect(errBody.Title).NotTo(BeEmpty())
		})
	})

	Describe("Response Format", func() {
		It("sets Content-Type application/json on provider responses (IT-STS-070)", func() {
			GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "container")
			baseURL, stop := startRealServer()
			DeferCleanup(stop)

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Post(
				baseURL+"/api/v1alpha1/providers?id=sp-db-001",
				"application/json",
				strings.NewReader(`{"name":"db-provider","endpoint":"https://sp.example.com:8080","service_type":"database","schema_version":"v1alpha1"}`),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusCreated))
			_ = resp.Body.Close()

			listResp, err := client.Get(baseURL + "/api/v1alpha1/providers")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = listResp.Body.Close() }()
			Expect(listResp.StatusCode).To(Equal(http.StatusOK))
			Expect(listResp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))

			getResp, err := client.Get(baseURL + "/api/v1alpha1/providers/sp-db-001")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = getResp.Body.Close() }()
			Expect(getResp.StatusCode).To(Equal(http.StatusOK))
			Expect(getResp.Header.Get("Content-Type")).To(ContainSubstring("application/json"))
		})
	})
})
