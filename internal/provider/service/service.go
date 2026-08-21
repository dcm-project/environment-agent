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
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

// ProviderService orchestrates SP registration operations.
type ProviderService struct {
	mu       sync.Mutex
	store    store.Store
	registry *provider.Registry
	health   provider.HealthTracker
	mon      *monitor.Monitor // nil-safe: if nil, no monitoring
	logger   *slog.Logger

	onChangeMu       sync.RWMutex
	onChange         func()
	embeddedCheckers map[string]monitor.Checker
}

// New creates a ProviderService with the given dependencies.
func New(s store.Store, registry *provider.Registry, health provider.HealthTracker, mon *monitor.Monitor, logger *slog.Logger) *ProviderService {
	if health == nil {
		panic("provider: health tracker must not be nil")
	}
	return &ProviderService{store: s, registry: registry, health: health, mon: mon, logger: logger}
}

// SetOnChange sets a callback invoked after a provider is registered or updated.
// Safe to call before or after providers are registered.
func (s *ProviderService) SetOnChange(fn func()) {
	s.onChangeMu.Lock()
	defer s.onChangeMu.Unlock()
	s.onChange = fn
}

func (s *ProviderService) notifyChange() {
	s.onChangeMu.RLock()
	fn := s.onChange
	s.onChangeMu.RUnlock()
	if fn != nil {
		fn()
	}
}

// RegistrationInput holds the fields for a provider registration request.
type RegistrationInput struct {
	Name          string
	Endpoint      string
	ServiceType   string
	SchemaVersion string
	DisplayName   *string
	ProviderID    *string
	Operations    *[]string
	Metadata      json.RawMessage
}

// Register creates or updates a provider registration.
// The caller (handler) is responsible for input validation (ID, schema_version, endpoint).
func (s *ProviderService) Register(ctx context.Context, in RegistrationInput) (*v1alpha1.Provider, bool, error) {
	result, created, err := s.registerLocked(ctx, in)
	if err == nil {
		s.notifyChange()
	}
	return result, created, err
}

func (s *ProviderService) registerLocked(ctx context.Context, in RegistrationInput) (*v1alpha1.Provider, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := s.findByName(ctx, in.Name)
	if err != nil {
		return nil, false, err
	}

	if existing != nil {
		if err := s.ensureIDConsistency(existing.ID, in.ProviderID); err != nil {
			return nil, false, err
		}
		result, err := s.updateRegistration(ctx, existing, in)
		return result, false, err
	}

	id, err := s.assignProviderID(ctx, in.ProviderID)
	if err != nil {
		return nil, false, err
	}
	result, err := s.createRegistration(ctx, id, in)
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
func (s *ProviderService) updateRegistration(ctx context.Context, existing *store.StoredProvider, in RegistrationInput) (*v1alpha1.Provider, error) {
	oldServiceType := existing.ServiceType
	oldEndpoint := existing.Endpoint
	if oldServiceType != in.ServiceType {
		if err := s.registry.Move(existing.Name, oldServiceType, in.ServiceType); err != nil {
			return nil, &DomainError{Code: ErrCodeConflict, Message: err.Error()}
		}
	}

	existing.Endpoint = in.Endpoint
	existing.ServiceType = in.ServiceType
	existing.SchemaVersion = in.SchemaVersion
	existing.DisplayName = in.DisplayName
	existing.Operations = in.Operations
	existing.Metadata = in.Metadata
	existing.UpdateTime = time.Now().UTC()

	if err := s.store.Save(ctx, *existing); err != nil {
		if oldServiceType != in.ServiceType {
			_ = s.registry.Move(existing.Name, in.ServiceType, oldServiceType)
		}
		return nil, err
	}
	if oldEndpoint != in.Endpoint || oldServiceType != in.ServiceType {
		s.trackExternalProvider(existing.ID, in.Endpoint, in.ServiceType, true)
	}
	s.logger.Info("external SP updated", "service_type", in.ServiceType, "provider_id", existing.ID, "name", existing.Name)
	return s.toAPI(existing), nil
}

// assignProviderID resolves the provider ID for a new registration.
// If the caller supplied an ID, it is validated for uniqueness. Otherwise a UUID is generated.
func (s *ProviderService) assignProviderID(ctx context.Context, requestedID *string) (string, error) {
	if requestedID == nil {
		return provider.GenerateProviderID(), nil
	}
	if *requestedID == "" {
		return "", &DomainError{Code: ErrCodeValidation, Message: "provider ID must not be empty"}
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
func (s *ProviderService) createRegistration(ctx context.Context, id string, in RegistrationInput) (*v1alpha1.Provider, error) {
	if err := s.registry.Claim(in.Name, in.ServiceType); err != nil {
		s.logger.Warn("external SP registration rejected: service type conflict",
			"service_type", in.ServiceType, "name", in.Name, "error", err)
		return nil, &DomainError{Code: ErrCodeConflict, Message: err.Error()}
	}

	now := time.Now().UTC()
	sp := store.StoredProvider{
		ID:            id,
		Name:          in.Name,
		Endpoint:      in.Endpoint,
		ServiceType:   in.ServiceType,
		SchemaVersion: in.SchemaVersion,
		DisplayName:   in.DisplayName,
		Operations:    in.Operations,
		Metadata:      in.Metadata,
		Type:          string(v1alpha1.External),
		CreateTime:    now,
		UpdateTime:    now,
	}
	if err := s.store.Save(ctx, sp); err != nil {
		s.registry.Release(in.ServiceType)
		return nil, err
	}
	s.trackExternalProvider(id, in.Endpoint, in.ServiceType, true)
	s.logger.Info("external SP registered", "service_type", in.ServiceType, "provider_id", id, "name", in.Name)
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
	restored, conflicts := 0, 0
	for _, p := range providers {
		if p.Type != string(v1alpha1.External) {
			continue
		}
		if err := s.registry.Claim(p.Name, p.ServiceType); err != nil {
			s.logger.Warn("conflict loading persisted provider", "name", p.Name, "error", err)
			conflicts++
			continue
		}
		s.trackExternalProvider(p.ID, p.Endpoint, p.ServiceType, false)
		s.logger.Info("persisted provider restored", "name", p.Name, "service_type", p.ServiceType, "type", p.Type)
		restored++
	}
	s.logger.Info("finished loading persisted providers", "restored", restored, "conflicts", conflicts)
	return nil
}

// trackExternalProvider sets initial health state and registers for periodic monitoring.
// When initialCheck is true, performs an immediate health check so a healthy SP
// becomes Ready without waiting for the next poll tick. LoadPersisted passes false
// to avoid blocking startup with N sequential HTTP checks.
func (s *ProviderService) trackExternalProvider(id, endpoint, serviceType string, initialCheck bool) {
	s.health.SetState(id, v1alpha1.Unhealthy, time.Time{})
	if s.mon != nil {
		s.mon.RegisterProvider(id, monitor.NewExternalChecker(endpoint), serviceType, v1alpha1.Unhealthy, initialCheck)
	}
}

// SetEmbeddedCheckers supplies in-process health checkers keyed by service type.
// Must be called before RegisterEmbedded. Unlisted service types use the
// default env-var-based embedded health check.
func (s *ProviderService) SetEmbeddedCheckers(checkers map[string]monitor.Checker) {
	s.embeddedCheckers = checkers
}

// RegisterEmbedded registers embedded SPs for the given service types.
// Removes stale embedded records not in the current enabled list.
//
// Invariant: this is called exactly once per process lifetime, from main.go
// at startup, before the monitor loop starts. REQ (spec "Out of Scope"):
// agent configuration has no hot-reload in v1alpha1, so EmbeddedSPs cannot
// change without a restart. This matters because registerEmbeddedType always
// calls Monitor.RegisterProvider, which unconditionally replaces any existing
// monitor entry for that provider ID with a fresh StateMachine (failure
// counter reset, initial state Ready). If a future feature calls this more
// than once for the same service type (e.g. config hot-reload), an
// in-flight Unhealthy/Unavailable embedded SP would be silently reported as
// Ready until its next check — revisit this reset-on-(re)register semantics
// before introducing any reload path.
func (s *ProviderService) RegisterEmbedded(serviceTypes []string) {
	types := make([]string, 0, len(serviceTypes))
	for _, st := range serviceTypes {
		if st = strings.TrimSpace(st); st != "" {
			types = append(types, st)
		}
	}

	enabled := make(map[string]bool, len(types))
	for _, st := range types {
		enabled[st] = true
	}

	s.removeStaleEmbedded(enabled)
	for _, st := range types {
		s.registerEmbeddedType(st)
	}
}

func (s *ProviderService) removeStaleEmbedded(enabled map[string]bool) {
	all, err := s.store.List(context.Background())
	if err != nil {
		s.logger.Error("failed to list providers for stale embedded cleanup", "error", err)
		return
	}
	for _, p := range all {
		if p.Type == string(v1alpha1.Embedded) && !enabled[p.ServiceType] {
			// Only release the slot if this stale embedded provider is still
			// its holder — an unconditional Release could free a slot an
			// external provider has since claimed for the same service type.
			// Embedded provider Name always equals its ServiceType
			// (registerEmbeddedType), so this is a direct name comparison.
			if holder, ok := s.registry.Lookup(p.ServiceType); ok && holder == p.Name {
				s.registry.Release(p.ServiceType)
			}
			s.cleanupEmbeddedRecord(p)
		}
	}
}

// cleanupEmbeddedRecord removes an embedded provider record from the store,
// health tracker, and monitor. Used when an embedded service type is displaced
// or no longer enabled.
func (s *ProviderService) cleanupEmbeddedRecord(p store.StoredProvider) {
	if err := s.store.Delete(context.Background(), p.Name); err != nil {
		s.logger.Error("failed to delete embedded record", "name", p.Name, "error", err)
	} else {
		s.logger.Info("embedded SP removed", "service_type", p.ServiceType, "provider_id", p.ID, "name", p.Name)
	}
	s.health.DeleteState(p.ID)
	if s.mon != nil {
		s.mon.DeregisterProvider(p.ID)
	}
}

func (s *ProviderService) registerEmbeddedType(st string) {
	existing, err := s.store.GetByName(context.Background(), st)
	if err != nil {
		s.logger.Error("failed to check store for embedded SP", "service_type", st, "error", err)
		return
	}
	if existing != nil && existing.Type == string(v1alpha1.External) {
		s.logger.Warn("skipping embedded SP: slot occupied by external provider",
			"service_type", st, "holder", existing.Name)
		return
	}

	if err := s.registry.Claim(st, st); err != nil {
		s.logger.Warn("skipping embedded SP: slot occupied", "service_type", st, "error", err)
		if existing != nil && existing.Type == string(v1alpha1.Embedded) {
			s.logger.Info("removing stale embedded record for occupied service type",
				"service_type", st, "provider_id", existing.ID)
			s.cleanupEmbeddedRecord(*existing)
		}
		return
	}

	now := time.Now().UTC()
	id, createTime := resolveEmbeddedIdentity(existing, now)
	sp := store.StoredProvider{
		ID:            id,
		Name:          st,
		Endpoint:      "embedded://" + st,
		ServiceType:   st,
		SchemaVersion: "v1alpha1",
		Type:          string(v1alpha1.Embedded),
		CreateTime:    createTime,
		UpdateTime:    now,
	}
	persisted := true
	if err := s.store.Save(context.Background(), sp); err != nil {
		s.logger.Error("failed to save embedded SP", "service_type", st, "error", err)
		if existing == nil {
			s.registry.Release(st)
			return
		}
		persisted = false
	}
	checker := s.embeddedChecker(st)
	if s.mon != nil {
		s.mon.RegisterProvider(sp.ID, checker, st, v1alpha1.Ready, true)
	} else {
		s.health.SetState(sp.ID, v1alpha1.Ready, sp.UpdateTime)
	}
	if persisted {
		s.logger.Info("embedded SP registered", "service_type", st, "provider_id", sp.ID)
	} else {
		s.logger.Warn("embedded SP registered with stale persisted state (save failed)", "service_type", st, "provider_id", sp.ID)
	}
}

// resolveEmbeddedIdentity returns the provider ID and creation time for an
// embedded registration. Reuses values from an existing embedded record when
// available to preserve identity across restarts.
func (s *ProviderService) embeddedChecker(serviceType string) monitor.Checker {
	if s.embeddedCheckers != nil {
		if checker, ok := s.embeddedCheckers[serviceType]; ok && checker != nil {
			return checker
		}
	}
	return monitor.NewEmbeddedChecker(monitor.DefaultEmbeddedCheckFn(serviceType))
}

func resolveEmbeddedIdentity(existing *store.StoredProvider, now time.Time) (string, time.Time) {
	if existing == nil || existing.Type != string(v1alpha1.Embedded) {
		return provider.GenerateProviderID(), now
	}
	id := existing.ID
	if id == "" {
		id = provider.GenerateProviderID()
	}
	createTime := existing.CreateTime
	if createTime.IsZero() {
		createTime = now
	}
	return id, createTime
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
	if state, ok := s.health.GetState(sp.ID); ok {
		status := state.Status
		p.Status = &status
		if !state.LastCheckTime.IsZero() {
			lastCheck := state.LastCheckTime
			p.LastCheckTime = &lastCheck
		}
	} else {
		// Properly registered providers always have health state:
		// external → trackExternalProvider sets Unhealthy,
		// embedded → registerEmbeddedType sets Ready via monitor/tracker.
		// Missing state means the record is stale; default to Unavailable
		// so it won't be advertised.
		defaultStatus := v1alpha1.Unavailable
		p.Status = &defaultStatus
	}
	if len(sp.Metadata) > 0 {
		var meta v1alpha1.ProviderMetadata
		if err := json.Unmarshal(sp.Metadata, &meta); err == nil {
			p.Metadata = &meta
		}
	}
	return p
}
