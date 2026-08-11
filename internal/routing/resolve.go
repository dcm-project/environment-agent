package routing

import (
	"context"
	"fmt"
	"log/slog"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

// ResolveProvider looks up the provider for a service type by checking the
// registry, store, and health tracker. Returns ok=false if no provider is
// registered for the service type. Returns a non-nil error for transient
// store failures (callers should retry rather than treating as permanent).
func ResolveProvider(
	ctx context.Context,
	registry *provider.Registry,
	st store.Store,
	ht provider.HealthTracker,
	logger *slog.Logger,
	serviceType string,
) (*store.StoredProvider, v1alpha1.ProviderStatus, bool, error) {
	providerName, found := registry.Lookup(serviceType)
	if !found {
		return nil, "", false, nil
	}

	sp, err := st.GetByName(ctx, providerName)
	if err != nil {
		return nil, "", false, fmt.Errorf("store lookup for provider %q: %w", providerName, err)
	}
	if sp == nil {
		return nil, "", false, nil
	}

	if sp.ID == "" {
		logger.Error("provider has empty ID (data corruption)", "name", providerName, "service_type", serviceType)
		return sp, v1alpha1.Unavailable, true, nil
	}

	state, found := ht.GetState(sp.ID)
	if !found {
		return sp, v1alpha1.Unavailable, true, nil
	}
	return sp, state.Status, true, nil
}
