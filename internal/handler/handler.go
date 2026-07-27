// Package handler implements the strict server interface by delegating to domain services.
package handler

import (
	"context"

	oapigen "github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/health"
)

// Compile-time interface check.
var _ oapigen.StrictServerInterface = (*Handler)(nil)

// Handler implements StrictServerInterface by delegating to domain services.
type Handler struct {
	health *health.Service
}

// New creates a Handler with the given health service.
func New(h *health.Service) *Handler {
	return &Handler{health: h}
}

func (h *Handler) GetHealth(_ context.Context, _ oapigen.GetHealthRequestObject) (oapigen.GetHealthResponseObject, error) {
	return oapigen.GetHealth200JSONResponse(h.health.Status()), nil
}

// ListProviders is not yet implemented. TODO: implement when provider registration feature lands.
func (h *Handler) ListProviders(_ context.Context, _ oapigen.ListProvidersRequestObject) (oapigen.ListProvidersResponseObject, error) {
	return nil, nil
}

// CreateProvider is not yet implemented. TODO: implement when provider registration feature lands.
func (h *Handler) CreateProvider(_ context.Context, _ oapigen.CreateProviderRequestObject) (oapigen.CreateProviderResponseObject, error) {
	return nil, nil
}

// GetProvider is not yet implemented. TODO: implement when provider registration feature lands.
func (h *Handler) GetProvider(_ context.Context, _ oapigen.GetProviderRequestObject) (oapigen.GetProviderResponseObject, error) {
	return nil, nil
}
