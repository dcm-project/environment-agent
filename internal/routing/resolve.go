package routing

import (
	"context"
	"log/slog"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

// ResolveProvider looks up the provider for a service type by checking the
// registry, store, and health tracker. Returns ok=false if no provider is
// registered or the store lookup fails.
func ResolveProvider(
	ctx context.Context,
	registry *provider.Registry,
	st store.Store,
	ht provider.HealthTracker,
	logger *slog.Logger,
	serviceType string,
) (*store.StoredProvider, v1alpha1.ProviderStatus, bool) {
	providerName, found := registry.Lookup(serviceType)
	if !found {
		return nil, "", false
	}

	sp, err := st.GetByName(ctx, providerName)
	if err != nil || sp == nil {
		return nil, "", false
	}

	if sp.ID == "" {
		logger.Error("provider has empty ID (data corruption)", "name", providerName, "serviceType", serviceType)
		return sp, v1alpha1.Unavailable, true
	}

	state, found := ht.GetState(sp.ID)
	if !found {
		return sp, v1alpha1.Unavailable, true
	}
	return sp, state.Status, true
}
