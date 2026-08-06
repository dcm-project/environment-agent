// Package handler implements the strict server interface by delegating to domain services.
package handler

import (
	"context"
	"encoding/json"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	oapigen "github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/health"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/service"
	"github.com/dcm-project/environment-agent/internal/ptr"
	"github.com/dcm-project/environment-agent/internal/requestctx"
)

// Compile-time interface check.
var _ oapigen.StrictServerInterface = (*Handler)(nil)

// Handler implements StrictServerInterface by delegating to domain services.
type Handler struct {
	health   *health.Service
	provider *service.ProviderService
}

// New creates a Handler with the given services.
func New(h *health.Service, p *service.ProviderService) *Handler {
	return &Handler{health: h, provider: p}
}

func (h *Handler) GetHealth(_ context.Context, _ oapigen.GetHealthRequestObject) (oapigen.GetHealthResponseObject, error) {
	return oapigen.GetHealth200JSONResponse(h.health.Status()), nil
}

func (h *Handler) CreateProvider(ctx context.Context, request oapigen.CreateProviderRequestObject) (oapigen.CreateProviderResponseObject, error) {
	body := request.Body
	instance := requestctx.URIFromContext(ctx)

	var providerID *string
	if request.Params.Id != nil {
		if err := provider.ValidateProviderID(*request.Params.Id); err != nil {
			return validationError("id", err, instance), nil
		}
		providerID = request.Params.Id
	}
	if err := provider.ValidateName(body.Name); err != nil {
		return validationError("name", err, instance), nil
	}
	if err := provider.ValidateServiceType(body.ServiceType); err != nil {
		return validationError("service_type", err, instance), nil
	}
	if err := provider.ValidateSchemaVersion(body.SchemaVersion); err != nil {
		return validationError("schema_version", err, instance), nil
	}
	if err := provider.ValidateEndpoint(body.Endpoint); err != nil {
		return validationError("endpoint", err, instance), nil
	}

	var metadataRaw json.RawMessage
	if body.Metadata != nil {
		data, err := json.Marshal(body.Metadata)
		if err != nil {
			return nil, err
		}
		metadataRaw = data
	}

	result, isNew, err := h.provider.Register(ctx, service.RegistrationInput{
		Name:          body.Name,
		Endpoint:      body.Endpoint,
		ServiceType:   body.ServiceType,
		SchemaVersion: body.SchemaVersion,
		DisplayName:   body.DisplayName,
		ProviderID:    providerID,
		Operations:    body.Operations,
		Metadata:      metadataRaw,
	})
	if err != nil {
		if domErr, ok := err.(*service.DomainError); ok && domErr.Code == service.ErrCodeConflict {
			return oapigen.CreateProvider409ApplicationProblemPlusJSONResponse(v1alpha1.Error{
				Type:     "CONFLICT",
				Title:    "Conflict",
				Status:   ptr.To(409),
				Detail:   ptr.To(domErr.Message),
				Instance: instance,
			}), nil
		}
		return nil, err
	}

	if isNew {
		return oapigen.CreateProvider201JSONResponse(*result), nil
	}
	return oapigen.CreateProvider200JSONResponse(*result), nil
}

func (h *Handler) ListProviders(ctx context.Context, _ oapigen.ListProvidersRequestObject) (oapigen.ListProvidersResponseObject, error) {
	providers, err := h.provider.List(ctx)
	if err != nil {
		return nil, err
	}
	return oapigen.ListProviders200JSONResponse(v1alpha1.ProviderList{
		Results: &providers,
	}), nil
}

func (h *Handler) GetProvider(ctx context.Context, request oapigen.GetProviderRequestObject) (oapigen.GetProviderResponseObject, error) {
	result, err := h.provider.Get(ctx, request.ProviderId)
	if err != nil {
		if domErr, ok := err.(*service.DomainError); ok && domErr.Code == service.ErrCodeNotFound {
			return oapigen.GetProvider404ApplicationProblemPlusJSONResponse(v1alpha1.Error{
				Type:     "NOT_FOUND",
				Title:    "Provider Not Found",
				Status:   ptr.To(404),
				Detail:   ptr.To(domErr.Message),
				Instance: requestctx.URIFromContext(ctx),
			}), nil
		}
		return nil, err
	}
	return oapigen.GetProvider200JSONResponse(*result), nil
}

func validationError(field string, err error, instance *string) oapigen.CreateProvider422ApplicationProblemPlusJSONResponse {
	return oapigen.CreateProvider422ApplicationProblemPlusJSONResponse(v1alpha1.Error{
		Type:     "UNPROCESSABLE_ENTITY",
		Title:    "Validation Failed",
		Status:   ptr.To(422),
		Detail:   ptr.To(fmt.Sprintf("invalid %s: %s", field, err.Error())),
		Instance: instance,
	})
}
