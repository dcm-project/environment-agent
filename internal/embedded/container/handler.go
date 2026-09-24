// Package container embeds the k8s container service provider in the agent.
package container

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	containerapi "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	embutil "github.com/dcm-project/environment-agent/internal/embedded/util"
	"github.com/dcm-project/environment-agent/internal/openshift/container/store"
	"github.com/dcm-project/environment-agent/internal/openshift/container/validate"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ServiceType is the embedded SP identifier for the container service provider.
const ServiceType = "container"

// Operations are the resource operations advertised at registration time.
// They mirror the createContainer/getContainer/listContainers/deleteContainer
// operations declared by the container capability contract in api/container/v1alpha1.
var Operations = []string{"CREATE", "READ", "DELETE"}

// containerLifecycle is the subset of store.Repository the embedded handler needs.
type containerLifecycle interface {
	Create(ctx context.Context, spec containerapi.ContainerSpec, id string) (*containerapi.Container, error)
	Delete(ctx context.Context, containerID string) error
}

// containerHandler implements routing.EmbeddedHandler for in-process container lifecycle.
type containerHandler struct {
	lifecycle containerLifecycle
}

var _ routing.EmbeddedHandler = (*containerHandler)(nil)

// NewContainerHandler creates an embedded container handler.
func NewContainerHandler(lifecycle containerLifecycle) routing.EmbeddedHandler {
	if lifecycle == nil {
		panic("embedded container handler: lifecycle must not be nil")
	}
	return &containerHandler{lifecycle: lifecycle}
}

func (h *containerHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	spec, err := parseContainerSpec(req.Spec)
	if err != nil {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}

	if err := validate.ValidateCreate(req.ResourceID, spec); err != nil {
		return mapStoreError(err)
	}

	_, err = h.lifecycle.Create(ctx, spec, req.ResourceID)
	if err != nil {
		return mapStoreError(err)
	}
	return nil
}

func (h *containerHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.lifecycle.Delete(ctx, req.ResourceID)
	if err != nil {
		return mapStoreError(err)
	}
	return nil
}

func parseContainerSpec(raw json.RawMessage) (containerapi.ContainerSpec, error) {
	payload, err := embutil.SpecJSON(raw)
	if err != nil {
		return containerapi.ContainerSpec{}, err
	}

	var spec containerapi.ContainerSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return containerapi.ContainerSpec{}, fmt.Errorf("invalid container spec: %w", err)
	}
	if spec.Metadata.Name == "" {
		return containerapi.ContainerSpec{}, fmt.Errorf("spec.metadata.name is required")
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
