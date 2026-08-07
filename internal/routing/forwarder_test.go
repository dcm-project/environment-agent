package routing_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/provider/store"
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
			Expect(receivedURIs[0]).To(ContainSubstring("%3F"))
			Expect(receivedURIs[0]).To(ContainSubstring("%23"))
		})
	})

	Describe("Idempotency-Key header (REQ-RCM-210)", func() {
		var (
			receivedHeaders http.Header
			server          *httptest.Server
			fwd             *routing.Forwarder
		)

		BeforeEach(func() {
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaders = r.Header.Clone()
				w.WriteHeader(http.StatusOK)
			}))
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client()})
		})

		AfterEach(func() { server.Close() })

		It("sends Idempotency-Key on POST (create)", func() {
			err := fwd.CreateResource(context.Background(), server.URL+"/api", false, routing.CreateResourceRequest{
				ResourceID: "res-1", ServiceType: "db", Spec: json.RawMessage(`{"size":"small"}`), EventID: "ce-id-123",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedHeaders.Get("Idempotency-Key")).To(Equal("ce-id-123"))
		})

		It("sends Idempotency-Key on DELETE", func() {
			err := fwd.DeleteResource(context.Background(), server.URL+"/api", false, routing.DeleteResourceRequest{
				ResourceID: "res-1", ServiceType: "db", EventID: "ce-id-456",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedHeaders.Get("Idempotency-Key")).To(Equal("ce-id-456"))
		})
	})

	Describe("HTTP status classification", func() {
		var fwd *routing.Forwarder

		makeServer := func(status int) *httptest.Server {
			return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
		}

		It("returns nil for 2xx responses", func() {
			for _, code := range []int{200, 201, 204} {
				server := makeServer(code)
				fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client()})
				err := fwd.CreateResource(context.Background(), server.URL, false, routing.CreateResourceRequest{
					ResourceID: "r", ServiceType: "s", Spec: json.RawMessage(`{}`), EventID: "e",
				})
				Expect(err).NotTo(HaveOccurred(), "expected nil for status %d", code)
				server.Close()
			}
		})

		It("returns SPResponseError with status code for 4xx", func() {
			server := makeServer(http.StatusBadRequest)
			defer server.Close()
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client()})
			err := fwd.CreateResource(context.Background(), server.URL, false, routing.CreateResourceRequest{
				ResourceID: "r", ServiceType: "s", Spec: json.RawMessage(`{}`), EventID: "e",
			})
			Expect(err).To(HaveOccurred())
			var spErr *routing.SPResponseError
			Expect(err).To(BeAssignableToTypeOf(spErr))
			Expect(routing.IsRetryable(err)).To(BeFalse(), "4xx should not be retryable")
		})

		It("returns retryable SPResponseError for 5xx", func() {
			server := makeServer(http.StatusServiceUnavailable)
			defer server.Close()
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client()})
			err := fwd.CreateResource(context.Background(), server.URL, false, routing.CreateResourceRequest{
				ResourceID: "r", ServiceType: "s", Spec: json.RawMessage(`{}`), EventID: "e",
			})
			Expect(err).To(HaveOccurred())
			Expect(routing.IsRetryable(err)).To(BeTrue(), "5xx should be retryable")
		})

		It("returns retryable SPResponseError for 429", func() {
			server := makeServer(http.StatusTooManyRequests)
			defer server.Close()
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client()})
			err := fwd.CreateResource(context.Background(), server.URL, false, routing.CreateResourceRequest{
				ResourceID: "r", ServiceType: "s", Spec: json.RawMessage(`{}`), EventID: "e",
			})
			Expect(err).To(HaveOccurred())
			Expect(routing.IsRetryable(err)).To(BeTrue(), "429 should be retryable")
		})
	})

	Describe("ForwardToSP dispatch", func() {
		var (
			receivedMethod string
			server         *httptest.Server
			fwd            *routing.Forwarder
		)

		BeforeEach(func() {
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedMethod = r.Method
				w.WriteHeader(http.StatusOK)
			}))
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client()})
		})

		AfterEach(func() { server.Close() })

		It("dispatches POST for create operations", func() {
			sp := &store.StoredProvider{Endpoint: server.URL, Type: "external"}
			err := routing.ForwardToSP(context.Background(), fwd, sp, routing.ForwardParams{
				ResourceID: "r1", ServiceType: "db", Spec: json.RawMessage(`{}`),
				EventID: "e1", IsCreate: true,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedMethod).To(Equal(http.MethodPost))
		})

		It("dispatches DELETE for delete operations", func() {
			sp := &store.StoredProvider{Endpoint: server.URL + "/api", Type: "external"}
			err := routing.ForwardToSP(context.Background(), fwd, sp, routing.ForwardParams{
				ResourceID: "r1", ServiceType: "db", EventID: "e2", IsCreate: false,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedMethod).To(Equal(http.MethodDelete))
		})
	})

	Describe("embedded SP routing", func() {
		It("returns error for unregistered embedded service type", func() {
			fwd := routing.NewForwarder(routing.ForwarderConfig{})
			err := fwd.CreateResource(context.Background(), "", true, routing.CreateResourceRequest{
				ResourceID: "r", ServiceType: "unknown", Spec: json.RawMessage(`{}`), EventID: "e",
			})
			Expect(err).To(HaveOccurred())
			var spErr *routing.SPResponseError
			Expect(err).To(BeAssignableToTypeOf(spErr))
		})

		It("delegates to registered embedded handler on create", func() {
			var receivedReq routing.CreateResourceRequest
			handler := &fakeEmbeddedHandler{
				createFn: func(_ context.Context, req routing.CreateResourceRequest) error {
					receivedReq = req
					return nil
				},
			}
			fwd := routing.NewForwarder(routing.ForwarderConfig{
				Embedded: map[string]routing.EmbeddedHandler{"db": handler},
			})
			err := fwd.CreateResource(context.Background(), "", true, routing.CreateResourceRequest{
				ResourceID: "res-1", ServiceType: "db", Spec: json.RawMessage(`{"size":"large"}`), EventID: "ce-99",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedReq.ResourceID).To(Equal("res-1"))
			Expect(receivedReq.EventID).To(Equal("ce-99"))
		})

		It("delegates to registered embedded handler on delete", func() {
			var receivedReq routing.DeleteResourceRequest
			handler := &fakeEmbeddedHandler{
				deleteFn: func(_ context.Context, req routing.DeleteResourceRequest) error {
					receivedReq = req
					return nil
				},
			}
			fwd := routing.NewForwarder(routing.ForwarderConfig{
				Embedded: map[string]routing.EmbeddedHandler{"db": handler},
			})
			err := fwd.DeleteResource(context.Background(), "", true, routing.DeleteResourceRequest{
				ResourceID: "res-2", ServiceType: "db", EventID: "ce-100",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedReq.ResourceID).To(Equal("res-2"))
		})
	})

	Describe("ForwardToSP embedded dispatch", func() {
		It("routes to embedded handler when SP type is embedded", func() {
			var called bool
			handler := &fakeEmbeddedHandler{
				createFn: func(_ context.Context, _ routing.CreateResourceRequest) error {
					called = true
					return nil
				},
			}
			fwd := routing.NewForwarder(routing.ForwarderConfig{
				Embedded: map[string]routing.EmbeddedHandler{"db": handler},
			})
			sp := &store.StoredProvider{Endpoint: "", Type: "embedded"}
			err := routing.ForwardToSP(context.Background(), fwd, sp, routing.ForwardParams{
				ResourceID: "r1", ServiceType: "db", Spec: json.RawMessage(`{}`),
				EventID: "e1", IsCreate: true,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(called).To(BeTrue())
		})
	})
})

type fakeEmbeddedHandler struct {
	createFn func(context.Context, routing.CreateResourceRequest) error
	deleteFn func(context.Context, routing.DeleteResourceRequest) error
}

func (f *fakeEmbeddedHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	if f.createFn != nil {
		return f.createFn(ctx, req)
	}
	return nil
}

func (f *fakeEmbeddedHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	if f.deleteFn != nil {
		return f.deleteFn(ctx, req)
	}
	return nil
}
