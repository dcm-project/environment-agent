package monitor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/provider"
)

type providerEntry struct {
	sm      *StateMachine
	checker Checker
}

// Monitor runs periodic health checks for registered providers.
type Monitor struct {
	healthTracker    provider.HealthTracker
	logger           *slog.Logger
	checkInterval    time.Duration
	checkTimeout     time.Duration
	failureThreshold int

	mu        sync.Mutex
	providers map[string]*providerEntry
	started   bool
	stopped   bool
	stopOnce  sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func New(healthTracker provider.HealthTracker, cfg config.HealthConfig, logger *slog.Logger) *Monitor {
	return &Monitor{
		healthTracker:    healthTracker,
		logger:           logger,
		checkInterval:    cfg.CheckInterval,
		checkTimeout:     cfg.CheckTimeout,
		failureThreshold: cfg.FailureThreshold,
		providers:        make(map[string]*providerEntry),
		stopCh:           make(chan struct{}),
	}
}

// Start begins the health monitoring loop. Non-blocking.
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.started {
		return
	}
	m.started = true
	m.wg.Add(1)
	go m.run(ctx)
	m.logger.Info("health monitor started", "interval", m.checkInterval, "timeout", m.checkTimeout)
}

// Stop gracefully stops the monitor. Idempotent.
func (m *Monitor) Stop() {
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
	m.logger.Info("health monitor stopped")
}

// RegisterProvider adds a provider to be monitored.
// If initialCheck is true, performs an immediate health check (for embedded SPs).
func (m *Monitor) RegisterProvider(id string, checker Checker, initialState v1alpha1.ProviderStatus, initialCheck bool) {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	sm := NewStateMachine(m.failureThreshold, initialState)
	m.providers[id] = &providerEntry{sm: sm, checker: checker}
	m.mu.Unlock()

	if initialCheck {
		checkCtx, cancel := context.WithTimeout(context.Background(), m.checkTimeout)
		result := checker.Check(checkCtx)
		cancel()

		m.mu.Lock()
		if !m.stopped {
			if entry := m.providers[id]; entry != nil && entry.sm == sm {
				_, to := sm.RecordResult(result)
				m.healthTracker.SetState(id, to, time.Now().UTC())
			}
		}
		m.mu.Unlock()
	}
}

// DeregisterProvider removes a provider from monitoring.
func (m *Monitor) DeregisterProvider(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.providers, id)
}

func (m *Monitor) run(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()
	m.checkAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkAll(ctx)
		}
	}
}

type providerSnapshot struct {
	id      string
	checker Checker
	sm      *StateMachine
}

func (m *Monitor) checkAll(ctx context.Context) {
	m.mu.Lock()
	snap := make([]providerSnapshot, 0, len(m.providers))
	for id, entry := range m.providers {
		snap = append(snap, providerSnapshot{id: id, checker: entry.checker, sm: entry.sm})
	}
	m.mu.Unlock()

	for _, p := range snap {
		checkCtx, cancel := context.WithTimeout(ctx, m.checkTimeout)
		result := p.checker.Check(checkCtx)
		cancel()

		m.mu.Lock()
		if entry := m.providers[p.id]; entry != nil && entry.sm == p.sm {
			from, to := p.sm.RecordResult(result)
			m.healthTracker.SetState(p.id, to, time.Now().UTC())
			if from != to {
				m.logger.Warn("provider health transition", "provider_id", p.id, "from", from, "to", to)
			}
		}
		m.mu.Unlock()
	}
}
