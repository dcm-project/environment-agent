package monitor_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/health/monitor"
)

var _ = Describe("ExternalChecker", Label("unit"), func() {
	Describe("health URL construction", func() {
		var requestedPath string
		var server *httptest.Server

		BeforeEach(func() {
			requestedPath = ""
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestedPath = r.URL.Path
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"healthy"}`))
			}))
		})

		AfterEach(func() {
			server.Close()
		})

		// UT-HMN-080: guards against the endpoint+"/health" string-concatenation
		// bug, which produced "//health" whenever the registered endpoint already
		// ended in a trailing slash (e.g. "http://host/api/v1alpha1/<type>/").
		It("requests /health without a double slash when the endpoint has a trailing slash", func() {
			checker := monitor.NewExternalChecker(server.URL + "/")

			result := checker.Check(context.Background())

			Expect(result).To(Equal(monitor.CheckHealthy))
			Expect(requestedPath).To(Equal("/health"))
		})

		It("requests /health when the endpoint has no trailing slash", func() {
			checker := monitor.NewExternalChecker(server.URL)

			result := checker.Check(context.Background())

			Expect(result).To(Equal(monitor.CheckHealthy))
			Expect(requestedPath).To(Equal("/health"))
		})

		It("joins /health onto a base path without doubling slashes", func() {
			checker := monitor.NewExternalChecker(server.URL + "/api/v1alpha1/database/")

			result := checker.Check(context.Background())

			Expect(result).To(Equal(monitor.CheckHealthy))
			Expect(requestedPath).To(Equal("/api/v1alpha1/database/health"))
		})
	})
})
