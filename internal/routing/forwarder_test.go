package routing_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// captureLogHandler records slog records for assertion.
type captureLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureLogHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureLogHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *captureLogHandler) last() slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[len(h.records)-1]
}

// all returns a snapshot of every record captured so far, so callers can
// scan the full dispatch log sequence rather than just the final entry.
func (h *captureLogHandler) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, len(h.records))
	copy(out, h.records)
	return out
}

func attrValue(rec slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

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

	Describe("dispatch outcome logging (AC-RCM-150)", func() {
		var (
			ch  *captureLogHandler
			fwd *routing.Forwarder
		)

		BeforeEach(func() {
			ch = &captureLogHandler{}
		})

		It("logs INFO with resource_id, service_type, provider_kind, operation, duration on external success", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
			defer server.Close()
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client(), Logger: slog.New(ch)})

			err := fwd.CreateResource(context.Background(), server.URL, false, routing.CreateResourceRequest{
				ResourceID: "res-log-1", ServiceType: "db", Spec: json.RawMessage(`{}`), EventID: "e",
			})
			Expect(err).NotTo(HaveOccurred())

			rec := ch.last()
			Expect(rec.Message).To(Equal("SP dispatch completed"))
			Expect(rec.Level).To(Equal(slog.LevelInfo))
			v, ok := attrValue(rec, "resource_id")
			Expect(ok).To(BeTrue())
			Expect(v.String()).To(Equal("res-log-1"))
			v, _ = attrValue(rec, "service_type")
			Expect(v.String()).To(Equal("db"))
			v, _ = attrValue(rec, "operation")
			Expect(v.String()).To(Equal("create"))
			v, _ = attrValue(rec, "provider_kind")
			Expect(v.String()).To(Equal("external"))
			_, ok = attrValue(rec, "duration")
			Expect(ok).To(BeTrue())
		})

		It("logs WARN with http_status on external failure, without leaking the response body", func() {
			const sensitiveBody = "sensitive backend detail"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(sensitiveBody))
			}))
			defer server.Close()
			fwd = routing.NewForwarder(routing.ForwarderConfig{HTTPClient: server.Client(), Logger: slog.New(ch)})

			err := fwd.DeleteResource(context.Background(), server.URL, false, routing.DeleteResourceRequest{
				ResourceID: "res-log-2", ServiceType: "db", EventID: "e",
			})
			Expect(err).To(HaveOccurred())

			rec := ch.last()
			Expect(rec.Message).To(Equal("SP dispatch failed"))
			Expect(rec.Level).To(Equal(slog.LevelWarn))
			v, _ := attrValue(rec, "operation")
			Expect(v.String()).To(Equal("delete"))
			v, ok := attrValue(rec, "http_status")
			Expect(ok).To(BeTrue())
			Expect(v.Kind()).To(Equal(slog.KindInt64), "http_status must be logged as an int, not stringified")
			Expect(v.Int64()).To(BeEquivalentTo(http.StatusServiceUnavailable))

			// Scan every captured record (not just the last) and every attribute
			// on each record (not just known keys), plus the message text, so a
			// leak via an earlier log call or an unexpected attribute key is
			// still caught.
			for _, r := range ch.all() {
				Expect(r.Message).NotTo(ContainSubstring(sensitiveBody),
					"record message must not leak the SP response body")
				r.Attrs(func(a slog.Attr) bool {
					Expect(a.Value.String()).NotTo(ContainSubstring(sensitiveBody),
						"attribute %q must not leak the SP response body", a.Key)
					return true
				})
			}
		})

		It("logs embedded dispatch outcome with provider_kind=embedded and no http_status", func() {
			handler := &fakeEmbeddedHandler{
				createFn: func(_ context.Context, _ routing.CreateResourceRequest) error { return nil },
			}
			fwd = routing.NewForwarder(routing.ForwarderConfig{
				Embedded: map[string]routing.EmbeddedHandler{"db": handler},
				Logger:   slog.New(ch),
			})

			err := fwd.CreateResource(context.Background(), "", true, routing.CreateResourceRequest{
				ResourceID: "res-log-3", ServiceType: "db", Spec: json.RawMessage(`{}`), EventID: "e",
			})
			Expect(err).NotTo(HaveOccurred())

			rec := ch.last()
			v, _ := attrValue(rec, "provider_kind")
			Expect(v.String()).To(Equal("embedded"))
			_, ok := attrValue(rec, "http_status")
			Expect(ok).To(BeFalse())
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
