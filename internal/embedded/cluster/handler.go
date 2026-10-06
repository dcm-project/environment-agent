// Package cluster embeds the ACM cluster service provider in the agent.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	clusterapi "github.com/dcm-project/environment-agent/api/cluster/v1alpha1"
	acmapp "github.com/dcm-project/environment-agent/internal/openshift/acmcluster/app"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ServiceType is the embedded SP identifier for the cluster service provider.
const ServiceType = "cluster"

// Operations are the resource operations advertised at registration time.
// They mirror the createCluster/getCluster/listClusters/deleteCluster
// operations declared by the cluster capability contract in api/cluster/v1alpha1.
var Operations = []string{"CREATE", "READ", "DELETE"}

// clusterLifecycle is the subset of service.ClusterService the embedded handler needs.
type clusterLifecycle interface {
	Create(ctx context.Context, id string, cluster clusterapi.Cluster) (*clusterapi.Cluster, error)
	Delete(ctx context.Context, id string) error
}

// clusterHandler implements routing.EmbeddedHandler for in-process cluster lifecycle.
type clusterHandler struct {
	lifecycle clusterLifecycle
}

var _ routing.EmbeddedHandler = (*clusterHandler)(nil)

// NewClusterHandler creates an embedded cluster handler.
func NewClusterHandler(lifecycle clusterLifecycle) routing.EmbeddedHandler {
	if lifecycle == nil {
		panic("embedded cluster handler: lifecycle must not be nil")
	}
	return &clusterHandler{lifecycle: lifecycle}
}

func (h *clusterHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	cluster, err := parseCreateCluster(req.Spec)
	if err != nil {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}

	_, err = h.lifecycle.Create(ctx, req.ResourceID, cluster)
	if err != nil {
		return mapServiceError(err)
	}
	return nil
}

func (h *clusterHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.lifecycle.Delete(ctx, req.ResourceID)
	if err != nil {
		return mapServiceError(err)
	}
	return nil
}

func parseCreateCluster(raw json.RawMessage) (clusterapi.Cluster, error) {
	if len(raw) == 0 {
		return clusterapi.Cluster{}, fmt.Errorf("spec is required")
	}

	var cluster clusterapi.Cluster
	if err := json.Unmarshal(raw, &cluster); err != nil {
		return clusterapi.Cluster{}, fmt.Errorf("invalid cluster spec: %w", err)
	}
	return cluster, nil
}

func mapServiceError(err error) error {
	opErr := acmapp.MapOperationError(err)
	if opErr == nil {
		return nil
	}
	return &routing.SPResponseError{StatusCode: opErr.StatusCode, Message: opErr.Message}
}
