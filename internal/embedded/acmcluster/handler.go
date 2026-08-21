package acmcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	acmv1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	acmruntime "github.com/dcm-project/environment-agent/internal/acmcluster/runtime"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ServiceType is the embedded SP identifier and registry service type for the
// ACM cluster service provider.
const ServiceType = "cluster"

// clusterService is the subset of the ACM cluster service used by the agent.
type clusterService interface {
	Create(ctx context.Context, id string, cluster acmv1.Cluster) (*acmv1.Cluster, error)
	Delete(ctx context.Context, id string) error
}

// healthChecker is the subset of the ACM health checker used by the agent.
type healthChecker interface {
	Check(ctx context.Context) acmv1.Health
}

// Handler implements routing.EmbeddedHandler by delegating to the ACM cluster
// service in-process.
type Handler struct {
	clusters clusterService
	logger   *slog.Logger
}

var _ routing.EmbeddedHandler = (*Handler)(nil)

// NewHandler creates an embedded cluster handler backed by clusterService.
func NewHandler(clusterService clusterService, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{clusters: clusterService, logger: logger}
}

// CreateResource creates a cluster using the inbound resource ID and spec.
func (h *Handler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	cluster, err := parseCreateCluster(req.Spec)
	if err != nil {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}

	_, err = h.clusters.Create(ctx, req.ResourceID, cluster)
	if err != nil {
		h.logger.Warn("embedded cluster create failed",
			"resource_id", req.ResourceID, "ce_id", req.EventID, "error", err)
		return mapServiceError(err)
	}
	return nil
}

// DeleteResource deletes a cluster by resource ID.
func (h *Handler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.clusters.Delete(ctx, req.ResourceID)
	if err != nil {
		h.logger.Warn("embedded cluster delete failed",
			"resource_id", req.ResourceID, "ce_id", req.EventID, "error", err)
		return mapServiceError(err)
	}
	return nil
}

func parseCreateCluster(raw json.RawMessage) (acmv1.Cluster, error) {
	if len(raw) == 0 {
		return acmv1.Cluster{}, fmt.Errorf("spec is required")
	}

	var cluster acmv1.Cluster
	if err := json.Unmarshal(raw, &cluster); err != nil {
		return acmv1.Cluster{}, fmt.Errorf("invalid cluster spec: %w", err)
	}
	return cluster, nil
}

func mapServiceError(err error) error {
	opErr := acmruntime.MapOperationError(err)
	if opErr == nil {
		return nil
	}
	return &routing.SPResponseError{StatusCode: opErr.StatusCode, Message: opErr.Message}
}
