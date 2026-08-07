package routing_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/routing"
)

var _ = Describe("Forwarder", Label("unit"), func() {
	Describe("DELETE URL construction", func() {
		var (
			receivedURIs []string
			server       *httptest.Server
			fwd          *routing.Forwarder
		)

		BeforeEach(func() {
			receivedURIs = nil
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedURIs = append(receivedURIs, r.RequestURI)
				w.WriteHeader(http.StatusOK)
			}))
			fwd = routing.NewForwarder(routing.ForwarderConfig{
				HTTPClient: server.Client(),
			})
		})

		AfterEach(func() {
			server.Close()
		})

		It("escapes path traversal sequences in resourceID", func() {
			err := fwd.DeleteResource(context.Background(), server.URL+"/api/v1/resources", false, routing.DeleteResourceRequest{
				ResourceID: "../admin", ServiceType: "database", EventID: "evt-1",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedURIs).To(HaveLen(1))
			// ../admin must be escaped so it doesn't traverse above /resources
			Expect(receivedURIs[0]).To(ContainSubstring("..%2Fadmin"))
			Expect(receivedURIs[0]).To(HavePrefix("/api/v1/resources/"))
		})

		It("escapes slash in resourceID as single path segment", func() {
			err := fwd.DeleteResource(context.Background(), server.URL+"/api/v1/resources", false, routing.DeleteResourceRequest{
				ResourceID: "res/123", ServiceType: "database", EventID: "evt-2",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedURIs).To(HaveLen(1))
			Expect(receivedURIs[0]).To(HaveSuffix("res%2F123"))
		})

		It("passes simple UUID-like resourceID unchanged", func() {
			err := fwd.DeleteResource(context.Background(), server.URL+"/api/v1/resources", false, routing.DeleteResourceRequest{
				ResourceID: "550e8400-e29b-41d4-a716-446655440000", ServiceType: "database", EventID: "evt-3",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedURIs).To(HaveLen(1))
			Expect(receivedURIs[0]).To(Equal("/api/v1/resources/550e8400-e29b-41d4-a716-446655440000"))
		})

		It("escapes query and fragment characters in resourceID", func() {
			err := fwd.DeleteResource(context.Background(), server.URL+"/api", false, routing.DeleteResourceRequest{
				ResourceID: "res?key=val#frag", ServiceType: "database", EventID: "evt-4",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedURIs).To(HaveLen(1))
			// ? and # must be percent-encoded in the URI
			Expect(receivedURIs[0]).To(ContainSubstring("%3F"))
			Expect(receivedURIs[0]).To(ContainSubstring("%23"))
		})
	})
})
