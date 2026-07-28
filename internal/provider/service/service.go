// Package service implements SP registration business logic.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

// ProviderService orchestrates SP registration operations.
type ProviderService struct {
	mu       sync.Mutex
	store    store.Store
	registry *provider.Registry
	logger   *slog.Logger
}

// New creates a ProviderService with the given dependencies.
func New(s store.Store, registry *provider.Registry, logger *slog.Logger) *ProviderService {
	return &ProviderService{store: s, registry: registry, logger: logger}
}

// Register creates or updates a provider registration.
// The caller (handler) is responsible for input validation (ID, schema_version, endpoint).
func (s *ProviderService) Register(ctx context.Context, name, endpoint, serviceType, schemaVersion string, displayName, providerID *string, operations *[]string, metadata json.RawMessage) (*v1alpha1.Provider, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := s.findByName(ctx, name)
	if err != nil {
		return nil, false, err
	}

	if existing != nil {
		if err := s.ensureIDConsistency(existing.ID, providerID); err != nil {
			return nil, false, err
		}
		result, err := s.updateRegistration(ctx, existing, endpoint, serviceType, schemaVersion, displayName, operations, metadata)
		return result, false, err
	}

	id, err := s.assignProviderID(ctx, providerID)
	if err != nil {
		return nil, false, err
	}
	result, err := s.createRegistration(ctx, id, name, endpoint, serviceType, schemaVersion, displayName, operations, metadata)
	return result, true, err
}

// findByName looks up an existing provider by its natural key (name).
func (s *ProviderService) findByName(ctx context.Context, name string) (*store.StoredProvider, error) {
	return s.store.GetByName(ctx, name)
}

// ensureIDConsistency verifies that a client-supplied ID does not conflict with
// the existing provider's immutable ID. A nil requestedID means the caller did
// not assert an ID, which is always consistent.
func (s *ProviderService) ensureIDConsistency(existingID string, requestedID *string) error {
	if requestedID == nil {
		return nil
	}
	if *requestedID == existingID {
		return nil
	}
	return &DomainError{
		Code:    ErrCodeConflict,
		Message: fmt.Sprintf("provider already exists with ID '%s'; cannot re-register with different ID '%s'", existingID, *requestedID),
	}
}

// updateRegistration applies mutable field changes to an existing provider.
// The provider's ID and CreateTime are immutable and never modified.
func (s *ProviderService) updateRegistration(ctx context.Context, existing *store.StoredProvider, endpoint, serviceType, schemaVersion string, displayName *string, operations *[]string, metadata json.RawMessage) (*v1alpha1.Provider, error) {
	oldServiceType := existing.ServiceType
	if oldServiceType != serviceType {
		if err := s.registry.Move(existing.Name, oldServiceType, serviceType); err != nil {
			return nil, &DomainError{Code: ErrCodeConflict, Message: err.Error()}
		}
	}

	existing.Endpoint = endpoint
	existing.ServiceType = serviceType
	existing.SchemaVersion = schemaVersion
	existing.DisplayName = displayName
	existing.Operations = operations
	existing.Metadata = metadata
	existing.UpdateTime = time.Now().UTC()

	if err := s.store.Save(ctx, *existing); err != nil {
		if oldServiceType != serviceType {
			_ = s.registry.Move(existing.Name, serviceType, oldServiceType)
		}
		return nil, err
	}
	return s.toAPI(existing), nil
}

// assignProviderID resolves the provider ID for a new registration.
// If the caller supplied an ID, it is validated for uniqueness. Otherwise a UUID is generated.
func (s *ProviderService) assignProviderID(ctx context.Context, requestedID *string) (string, error) {
	if requestedID == nil {
		return provider.GenerateProviderID(), nil
	}

	holder, err := s.store.GetByID(ctx, *requestedID)
	if err != nil {
		return "", err
	}
	if holder != nil {
		return "", &DomainError{
			Code:    ErrCodeConflict,
			Message: fmt.Sprintf("provider ID '%s' is already used by provider '%s'", *requestedID, holder.Name),
		}
	}
	return *requestedID, nil
}

// createRegistration claims the service type slot and persists a new provider record.
func (s *ProviderService) createRegistration(ctx context.Context, id, name, endpoint, serviceType, schemaVersion string, displayName *string, operations *[]string, metadata json.RawMessage) (*v1alpha1.Provider, error) {
	if err := s.registry.Claim(name, serviceType); err != nil {
		return nil, &DomainError{Code: ErrCodeConflict, Message: err.Error()}
	}

	now := time.Now().UTC()
	sp := store.StoredProvider{
		ID:            id,
		Name:          name,
		Endpoint:      endpoint,
		ServiceType:   serviceType,
		SchemaVersion: schemaVersion,
		DisplayName:   displayName,
		Operations:    operations,
		Metadata:      metadata,
		Type:          string(v1alpha1.External),
		CreateTime:    now,
		UpdateTime:    now,
	}
	if err := s.store.Save(ctx, sp); err != nil {
		s.registry.Release(serviceType)
		return nil, err
	}
	return s.toAPI(&sp), nil
}

// List returns all registered providers.
func (s *ProviderService) List(ctx context.Context) ([]v1alpha1.Provider, error) {
	stored, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]v1alpha1.Provider, 0, len(stored))
	for i := range stored {
		results = append(results, *s.toAPI(&stored[i]))
	}
	return results, nil
}

// Get returns a single provider by ID.
func (s *ProviderService) Get(ctx context.Context, id string) (*v1alpha1.Provider, error) {
	sp, err := s.store.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if sp == nil {
		return nil, &DomainError{Code: ErrCodeNotFound, Message: fmt.Sprintf("provider '%s' not found", id)}
	}
	return s.toAPI(sp), nil
}

// LoadPersisted loads previously persisted external registrations into the registry.
// Only external providers are restored; embedded providers derive from configuration.
func (s *ProviderService) LoadPersisted() error {
	providers, err := s.store.List(context.Background())
	if err != nil {
		return err
	}
	for _, p := range providers {
		if p.Type != string(v1alpha1.External) {
			continue
		}
		if err := s.registry.Claim(p.Name, p.ServiceType); err != nil {
			s.logger.Warn("conflict loading persisted provider", "name", p.Name, "error", err)
		}
	}
	return nil
}

// RegisterEmbedded registers embedded SPs for the given service types.
// Removes stale embedded records not in the current enabled list.
func (s *ProviderService) RegisterEmbedded(serviceTypes []string) {
	enabled := make(map[string]bool, len(serviceTypes))
	for _, st := range serviceTypes {
		st = strings.TrimSpace(st)
		if st != "" {
			enabled[st] = true
		}
	}

	// Remove persisted embedded providers that are no longer enabled.
	all, err := s.store.List(context.Background())
	if err == nil {
		for _, p := range all {
			if p.Type == string(v1alpha1.Embedded) && !enabled[p.ServiceType] {
				_ = s.store.Delete(context.Background(), p.Name)
			}
		}
	}

	for _, st := range serviceTypes {
		st = strings.TrimSpace(st)
		if st == "" {
			continue
		}

		existing, err := s.store.GetByName(context.Background(), st)
		if err != nil {
			s.logger.Error("failed to check store for embedded SP", "service_type", st, "error", err)
			continue
		}
		if existing != nil && existing.Type == string(v1alpha1.External) {
			s.logger.Warn("skipping embedded SP: slot occupied by external provider",
				"service_type", st, "holder", existing.Name)
			continue
		}

		if err := s.registry.Claim(st, st); err != nil {
			s.logger.Warn("skipping embedded SP: slot occupied", "service_type", st, "error", err)
			continue
		}

		now := time.Now().UTC()
		sp := store.StoredProvider{
			ID:            provider.GenerateProviderID(),
			Name:          st,
			Endpoint:      "",
			ServiceType:   st,
			SchemaVersion: "v1alpha1",
			Type:          string(v1alpha1.Embedded),
			CreateTime:    now,
			UpdateTime:    now,
		}
		if err := s.store.Save(context.Background(), sp); err != nil {
			s.logger.Error("failed to save embedded SP", "service_type", st, "error", err)
		}
	}
}

func (s *ProviderService) toAPI(sp *store.StoredProvider) *v1alpha1.Provider {
	providerType := v1alpha1.ProviderType(sp.Type)
	path := fmt.Sprintf("providers/%s", sp.ID)
	p := &v1alpha1.Provider{
		Id:            &sp.ID,
		Path:          &path,
		Name:          sp.Name,
		Endpoint:      sp.Endpoint,
		ServiceType:   sp.ServiceType,
		SchemaVersion: sp.SchemaVersion,
		DisplayName:   sp.DisplayName,
		Operations:    sp.Operations,
		Type:          &providerType,
		CreateTime:    &sp.CreateTime,
		UpdateTime:    &sp.UpdateTime,
	}
	if len(sp.Metadata) > 0 {
		var meta v1alpha1.ProviderMetadata
		if err := json.Unmarshal(sp.Metadata, &meta); err == nil {
			p.Metadata = &meta
		}
	}
	return p
}
