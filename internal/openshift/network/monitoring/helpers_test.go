package monitoring_test

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dcm-project/environment-agent/internal/openshift/network/monitoring"
)

// Compile-time assertion: NATSPublisher implements StatusPublisher.
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

type retryTrackingPublisher struct {
	attempts   atomic.Int32
	failAlways bool
}

func (p *retryTrackingPublisher) Publish(_ context.Context, _ monitoring.StatusEvent) error {
	n := p.attempts.Add(1)
	if p.failAlways || n <= 3 {
		return context.DeadlineExceeded
	}
	return nil
}

func (p *retryTrackingPublisher) Close() error {
	return nil
}

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

const testProviderName = "network"
