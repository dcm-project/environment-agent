// Package database embeds the cnpg database service provider in the agent.
package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	databaseapi "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	embutil "github.com/dcm-project/environment-agent/internal/embedded/util"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"github.com/dcm-project/environment-agent/internal/openshift/database/validate"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// ServiceType is the embedded SP identifier for the database service provider.
const ServiceType = "database"

// databaseLifecycle is the subset of store.DatabaseRepository the embedded handler needs.
type databaseLifecycle interface {
	Create(ctx context.Context, spec databaseapi.DatabaseSpec, id string) (*databaseapi.Database, error)
	Delete(ctx context.Context, databaseID string) error
}

// databaseHandler implements routing.EmbeddedHandler for in-process database lifecycle.
type databaseHandler struct {
	lifecycle databaseLifecycle
}

var _ routing.EmbeddedHandler = (*databaseHandler)(nil)

// NewDatabaseHandler creates an embedded database handler.
func NewDatabaseHandler(lifecycle databaseLifecycle) routing.EmbeddedHandler {
	if lifecycle == nil {
		panic("embedded database handler: lifecycle must not be nil")
	}
	return &databaseHandler{lifecycle: lifecycle}
}

func (h *databaseHandler) CreateResource(ctx context.Context, req routing.CreateResourceRequest) error {
	spec, err := parseDatabaseSpec(req.Spec)
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

func (h *databaseHandler) DeleteResource(ctx context.Context, req routing.DeleteResourceRequest) error {
	err := h.lifecycle.Delete(ctx, req.ResourceID)
	if err != nil {
		return mapStoreError(err)
	}
	return nil
}

func parseDatabaseSpec(raw json.RawMessage) (databaseapi.DatabaseSpec, error) {
	payload, err := embutil.SpecJSON(raw)
	if err != nil {
		return databaseapi.DatabaseSpec{}, err
	}

	var spec databaseapi.DatabaseSpec
	if err := json.Unmarshal(payload, &spec); err != nil {
		return databaseapi.DatabaseSpec{}, fmt.Errorf("invalid database spec: %w", err)
	}
	if spec.Metadata.Name == "" {
		return databaseapi.DatabaseSpec{}, fmt.Errorf("spec.metadata.name is required")
	}
	return spec, nil
}

func mapStoreError(err error) error {
	if err, ok := errors.AsType[*store.NotFoundError](err); ok {
		return &routing.SPResponseError{StatusCode: http.StatusNotFound, Message: err.Error()}
	}
	if err, ok := errors.AsType[*store.ConflictError](err); ok {
		return &routing.SPResponseError{StatusCode: http.StatusConflict, Message: err.Error()}
	}
	if err, ok := errors.AsType[*store.InvalidArgumentError](err); ok {
		return &routing.SPResponseError{StatusCode: http.StatusBadRequest, Message: err.Error()}
	}
	return &routing.SPResponseError{StatusCode: http.StatusInternalServerError, Message: err.Error()}
}
