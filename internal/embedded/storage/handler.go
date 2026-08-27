package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	storageapi "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	embutil "github.com/dcm-project/environment-agent/internal/embedded/util"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/validate"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ServiceType is the embedded SP identifier for the storage service provider.
const ServiceType = "storage"

// volumeLifecycle is the subset of store.VolumeRepository the embedded handler needs.
type volumeLifecycle interface {
	Create(ctx context.Context, spec storageapi.StorageSpec, id string) (*storageapi.Volume, error)
	Delete(ctx context.Context, volumeID string) error
}

// storageHandler implements routing.EmbeddedHandler for in-process storage lifecycle.
type storageHandler struct {
	lifecycle volumeLifecycle
}

var _ routing.EmbeddedHandler = (*storageHandler)(nil)

// NewStorageHandler creates an embedded storage handler.
func NewStorageHandler(lifecycle volumeLifecycle) routing.EmbeddedHandler {
	if lifecycle == nil {
		panic("embedded storage handler: lifecycle must not be nil")
	}
	return &storageHandler{lifecycle: lifecycle}
}

func (h *storageHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	spec, err := parseStorageSpec(req.Spec)
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

func (h *storageHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.lifecycle.Delete(ctx, req.ResourceID)
	if err != nil {
		return mapStoreError(err)
	}
	return nil
}

func parseStorageSpec(raw json.RawMessage) (storageapi.StorageSpec, error) {
	payload, err := embutil.SpecJSON(raw)
	if err != nil {
		return storageapi.StorageSpec{}, err
	}

	var wrapped storageapi.Volume
	if err := json.Unmarshal(payload, &wrapped); err == nil && wrapped.Spec.ServiceType != "" {
		return wrapped.Spec, nil
	}

	var spec storageapi.StorageSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return storageapi.StorageSpec{}, fmt.Errorf("invalid storage spec: %w", err)
	}
	if spec.Metadata.Name == "" {
		return storageapi.StorageSpec{}, fmt.Errorf("spec.metadata.name is required")
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
	var failed *store.FailedPreconditionError
	if errors.As(err, &failed) {
		return &routing.SPResponseError{StatusCode: http.StatusUnprocessableEntity, Message: err.Error()}
	}
	return &routing.SPResponseError{StatusCode: http.StatusInternalServerError, Message: err.Error()}
}
