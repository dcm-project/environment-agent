package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/dcm-project/environment-agent/internal/provider/store"
)

const (
	idempotencyKeyHeader = "Idempotency-Key"
	maxResponseBodyBytes = 4096
)

// EmbeddedHandler provides in-process SP handling for embedded SPs.
type EmbeddedHandler interface {
	CreateResource(ctx context.Context, req CreateResourceRequest) error
	DeleteResource(ctx context.Context, req DeleteResourceRequest) error
}

// Forwarder implements SPForwarder using HTTP for external SPs and
// in-process handlers for embedded SPs.
type Forwarder struct {
	httpClient *http.Client
	embedded   map[string]EmbeddedHandler
	logger     *slog.Logger
}

// ForwarderConfig holds forwarder options.
type ForwarderConfig struct {
	HTTPClient *http.Client
	Embedded   map[string]EmbeddedHandler
	Logger     *slog.Logger
}

// NewForwarder creates a Forwarder. If HTTPClient is nil, http.DefaultClient is used.
func NewForwarder(cfg ForwarderConfig) *Forwarder {
	c := cfg.HTTPClient
	if c == nil {
		c = http.DefaultClient
	}
	e := cfg.Embedded
	if e == nil {
		e = make(map[string]EmbeddedHandler)
	}
	l := cfg.Logger
	if l == nil {
		l = slog.Default()
	}
	return &Forwarder{httpClient: c, embedded: e, logger: l}
}

func (f *Forwarder) CreateResource(ctx context.Context, endpoint string, embedded bool, req CreateResourceRequest) error {
	if embedded {
		return f.createEmbedded(ctx, req)
	}
	return f.createExternal(ctx, endpoint, req)
}

func (f *Forwarder) DeleteResource(ctx context.Context, endpoint string, embedded bool, req DeleteResourceRequest) error {
	if embedded {
		return f.deleteEmbedded(ctx, req)
	}
	return f.deleteExternal(ctx, endpoint, req)
}

func (f *Forwarder) createEmbedded(ctx context.Context, req CreateResourceRequest) error {
	h, ok := f.embedded[req.ServiceType]
	if !ok {
		return &SPResponseError{StatusCode: http.StatusServiceUnavailable, Message: "no embedded handler for service type: " + req.ServiceType}
	}
	return h.CreateResource(ctx, req)
}

func (f *Forwarder) deleteEmbedded(ctx context.Context, req DeleteResourceRequest) error {
	h, ok := f.embedded[req.ServiceType]
	if !ok {
		return &SPResponseError{StatusCode: http.StatusServiceUnavailable, Message: "no embedded handler for service type: " + req.ServiceType}
	}
	return h.DeleteResource(ctx, req)
}

func (f *Forwarder) createExternal(ctx context.Context, endpoint string, req CreateResourceRequest) error {
	body, err := json.Marshal(req.Spec)
	if err != nil {
		return &SPResponseError{StatusCode: http.StatusBadRequest, Message: "failed to marshal spec: " + err.Error()}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating POST request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(idempotencyKeyHeader, req.EventID)

	return f.doRequest(httpReq)
}

func (f *Forwarder) deleteExternal(ctx context.Context, endpoint string, req DeleteResourceRequest) error {
	deleteURL, err := url.JoinPath(endpoint, url.PathEscape(req.ResourceID))
	if err != nil {
		return fmt.Errorf("building DELETE URL: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteURL, nil)
	if err != nil {
		return fmt.Errorf("creating DELETE request: %w", err)
	}
	httpReq.Header.Set(idempotencyKeyHeader, req.EventID)

	return f.doRequest(httpReq)
}

func (f *Forwarder) doRequest(req *http.Request) error {
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	msg := string(bodyBytes)
	if msg == "" {
		msg = resp.Status
	}
	return &SPResponseError{StatusCode: resp.StatusCode, Message: msg}
}

// ForwardParams holds the parameters for a single SP forward operation.
type ForwardParams struct {
	ResourceID  string
	ServiceType string
	Spec        json.RawMessage
	EventID     string
	IsCreate    bool
}

// ForwardToSP forwards a single create or delete operation to the given SP.
// This is a pure forward helper — callers own retry, error, and ack policy.
func ForwardToSP(ctx context.Context, fwd SPForwarder, sp *store.StoredProvider, params ForwardParams) error {
	embedded := sp.Type == "embedded"
	if params.IsCreate {
		return fwd.CreateResource(ctx, sp.Endpoint, embedded, CreateResourceRequest{
			ResourceID:  params.ResourceID,
			ServiceType: params.ServiceType,
			Spec:        params.Spec,
			EventID:     params.EventID,
		})
	}
	return fwd.DeleteResource(ctx, sp.Endpoint, embedded, DeleteResourceRequest{
		ResourceID:  params.ResourceID,
		ServiceType: params.ServiceType,
		EventID:     params.EventID,
	})
}
