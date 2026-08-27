package monitoring_test

import (
	"context"
	"sync"

	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
)

// Compile-time assertion: NATSPublisher implements StatusPublisher (TC-U072).
var _ monitoring.StatusPublisher = (*monitoring.NATSPublisher)(nil)

// mockStatusPublisher records all published events for test assertions.
type mockStatusPublisher struct {
	mu     sync.Mutex
	events []monitoring.StatusEvent
	err    error
}

func newMockPublisher() *mockStatusPublisher {
	return &mockStatusPublisher{}
}

func (m *mockStatusPublisher) Publish(_ context.Context, event monitoring.StatusEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, event)
	return nil
}

func (m *mockStatusPublisher) Close() error {
	return nil
}

func (m *mockStatusPublisher) Events() []monitoring.StatusEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]monitoring.StatusEvent, len(m.events))
	copy(cp, m.events)
	return cp
}

// cancellingMockPublisher rejects Publish when ctx is already cancelled, matching
// NATSPublisher behavior during shutdown.
type cancellingMockPublisher struct {
	mockStatusPublisher
}

func newCancellingMockPublisher() *cancellingMockPublisher {
	return &cancellingMockPublisher{mockStatusPublisher: *newMockPublisher()}
}

func (m *cancellingMockPublisher) Publish(ctx context.Context, event monitoring.StatusEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.mockStatusPublisher.Publish(ctx, event)
}
