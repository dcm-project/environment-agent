package provider

import (
	"sync"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

// HealthState holds the runtime health of a single provider.
type HealthState struct {
	Status        v1alpha1.ProviderStatus
	LastCheckTime time.Time
}

// HealthTracker manages runtime health state for registered providers.
type HealthTracker interface {
	GetState(providerID string) (HealthState, bool)
	SetState(providerID string, status v1alpha1.ProviderStatus, lastCheckTime time.Time)
	DeleteState(providerID string)
}

// InMemoryHealthTracker is a thread-safe in-memory HealthTracker.
type InMemoryHealthTracker struct {
	mu     sync.RWMutex
	states map[string]HealthState
}

var _ HealthTracker = (*InMemoryHealthTracker)(nil)

// NewInMemoryHealthTracker creates a ready-to-use InMemoryHealthTracker.
func NewInMemoryHealthTracker() *InMemoryHealthTracker {
	return &InMemoryHealthTracker{states: make(map[string]HealthState)}
}

func (t *InMemoryHealthTracker) GetState(providerID string) (HealthState, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	state, ok := t.states[providerID]
	return state, ok
}

func (t *InMemoryHealthTracker) SetState(providerID string, status v1alpha1.ProviderStatus, lastCheckTime time.Time) {
	if providerID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.states[providerID] = HealthState{Status: status, LastCheckTime: lastCheckTime}
}

func (t *InMemoryHealthTracker) DeleteState(providerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, providerID)
}
