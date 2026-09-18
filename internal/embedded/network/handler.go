package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	networkapi "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	embutil "github.com/dcm-project/environment-agent/internal/embedded/util"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	"github.com/dcm-project/environment-agent/internal/openshift/network/validate"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ServiceType is the embedded SP identifier for the network service provider.
const ServiceType = "network"

// networkLifecycle is the subset of store.NetworkRepository the embedded handler needs.
type networkLifecycle interface {
	Create(ctx context.Context, spec networkapi.NetworkSpec, id string) (*networkapi.Network, error)
	Delete(ctx context.Context, networkID string) error
}

// networkHandler implements routing.EmbeddedHandler for in-process network lifecycle.
type networkHandler struct {
	lifecycle networkLifecycle
}

var _ routing.EmbeddedHandler = (*networkHandler)(nil)

// NewNetworkHandler creates an embedded network handler.
func NewNetworkHandler(lifecycle networkLifecycle) routing.EmbeddedHandler {
	if lifecycle == nil {
		panic("embedded network handler: lifecycle must not be nil")
	}
	return &networkHandler{lifecycle: lifecycle}
}

func (h *networkHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	spec, err := parseNetworkSpec(req.Spec)
	if err != nil {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}

	originalName := spec.Metadata.Name
	id := req.ResourceID
	if originalName != id && originalName != "" {
		spec.Metadata.Name = id
	} else if spec.Metadata.Name == "" {
		spec.Metadata.Name = id
	}

	if err := validate.ValidateCreate(id, spec); err != nil {
		return mapStoreError(err)
	}

	_, err = h.lifecycle.Create(ctx, spec, id)
	if err != nil {
		return mapStoreError(err)
	}
	return nil
}

func (h *networkHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.lifecycle.Delete(ctx, req.ResourceID)
	if err != nil {
		return mapStoreError(err)
	}
	return nil
}

func parseNetworkSpec(raw json.RawMessage) (networkapi.NetworkSpec, error) {
	payload, err := embutil.SpecJSON(raw)
	if err != nil {
		return networkapi.NetworkSpec{}, err
	}

	var wrapped networkapi.Network
	if err := json.Unmarshal(payload, &wrapped); err == nil && wrapped.Spec.ServiceType != "" {
		return wrapped.Spec, nil
	}

	var spec networkapi.NetworkSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return networkapi.NetworkSpec{}, fmt.Errorf("invalid network spec: %w", err)
	}
	if spec.Metadata.Name == "" {
		return networkapi.NetworkSpec{}, fmt.Errorf("spec.metadata.name is required")
	}
	return spec, nil
}

func mapStoreError(err error) error {
	var notFound *store.NotFoundError
	if errors.As(err, &notFound) {
		return &routing.SPResponseError{StatusCode: http.StatusNotFound, Message: err.Error()}
	}
	var conflict *store.ConflictError
	if errors.As(err, &conflict) {
		return &routing.SPResponseError{StatusCode: http.StatusConflict, Message: err.Error()}
	}
	var invalid *store.InvalidArgumentError
	if errors.As(err, &invalid) {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}
	return &routing.SPResponseError{StatusCode: http.StatusInternalServerError, Message: err.Error()}
}
