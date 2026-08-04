package monitor_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/provider"
)

var _ provider.HealthTracker = (*fakeHealthTracker)(nil)

type fakeHealthTracker struct {
	mu     sync.Mutex
	states map[string]provider.HealthState
}

func newFakeHealthTracker() *fakeHealthTracker {
	return &fakeHealthTracker{states: make(map[string]provider.HealthState)}
}

func (f *fakeHealthTracker) SetState(id string, status v1alpha1.ProviderStatus, t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[id] = provider.HealthState{Status: status, LastCheckTime: t}
}

func (f *fakeHealthTracker) GetState(id string) (provider.HealthState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.states[id]
	return s, ok
}

func (f *fakeHealthTracker) DeleteState(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.states, id)
}

// blockingChecker blocks until released via channel or context cancellation.
type blockingChecker struct {
	entered chan struct{}
	release chan struct{}
	result  monitor.HealthCheckResult
}

func (c *blockingChecker) Check(ctx context.Context) monitor.HealthCheckResult {
	select {
	case c.entered <- struct{}{}:
	case <-ctx.Done():
		return monitor.CheckFailed
	}
	select {
	case <-c.release:
		return c.result
	case <-ctx.Done():
		return monitor.CheckFailed
	}
}

type countingChecker struct {
	count  atomic.Int64
	result monitor.HealthCheckResult
}

func (c *countingChecker) Check(_ context.Context) monitor.HealthCheckResult {
	c.count.Add(1)
	return c.result
}

func newTestMonitor(ht provider.HealthTracker, cfg config.HealthConfig) *monitor.Monitor {
	return monitor.New(ht, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

var _ = Describe("Monitor", Label("unit"), func() {
	Describe("RecordResult return values", func() {
		DescribeTable("returns correct (from, to) for state transitions",
			func(threshold int, initial v1alpha1.ProviderStatus, results []monitor.HealthCheckResult, wantFrom, wantTo v1alpha1.ProviderStatus) {
				sm := monitor.NewStateMachine(threshold, initial)
				var from, to v1alpha1.ProviderStatus
				for _, r := range results {
					from, to = sm.RecordResult(r)
				}
				Expect(from).To(Equal(wantFrom), "from state")
				Expect(to).To(Equal(wantTo), "to state")
			},
			Entry("Ready → Ready (below threshold)",
				3, v1alpha1.Ready,
				[]monitor.HealthCheckResult{monitor.CheckFailed},
				v1alpha1.Ready, v1alpha1.Ready),
			Entry("Ready → Unavailable (threshold failures)",
				3, v1alpha1.Ready,
				[]monitor.HealthCheckResult{monitor.CheckFailed, monitor.CheckFailed, monitor.CheckFailed},
				v1alpha1.Ready, v1alpha1.Unavailable),
			Entry("Ready → Unhealthy",
				3, v1alpha1.Ready,
				[]monitor.HealthCheckResult{monitor.CheckUnhealthy},
				v1alpha1.Ready, v1alpha1.Unhealthy),
			Entry("Unhealthy → Ready",
				3, v1alpha1.Unhealthy,
				[]monitor.HealthCheckResult{monitor.CheckHealthy},
				v1alpha1.Unhealthy, v1alpha1.Ready),
			Entry("Unhealthy → Unavailable (threshold failures)",
				3, v1alpha1.Unhealthy,
				[]monitor.HealthCheckResult{monitor.CheckFailed, monitor.CheckFailed, monitor.CheckFailed},
				v1alpha1.Unhealthy, v1alpha1.Unavailable),
			Entry("Unavailable → Ready",
				3, v1alpha1.Unavailable,
				[]monitor.HealthCheckResult{monitor.CheckHealthy},
				v1alpha1.Unavailable, v1alpha1.Ready),
			Entry("Unavailable → Unhealthy",
				3, v1alpha1.Unavailable,
				[]monitor.HealthCheckResult{monitor.CheckUnhealthy},
				v1alpha1.Unavailable, v1alpha1.Unhealthy),
		)
	})

	Describe("Deregister during checkAll", func() {
		It("discards stale result when provider is deregistered mid-check", func() {
			ht := newFakeHealthTracker()
			checker := &blockingChecker{
				entered: make(chan struct{}, 1),
				release: make(chan struct{}),
				result:  monitor.CheckHealthy,
			}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			})
			m.RegisterProvider("p1", checker, v1alpha1.Unhealthy, false)

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			Eventually(checker.entered).Should(Receive())

			m.DeregisterProvider("p1")
			checker.release <- struct{}{}

			Consistently(func() bool {
				_, ok := ht.GetState("p1")
				return ok
			}).WithTimeout(200 * time.Millisecond).WithPolling(20 * time.Millisecond).Should(BeFalse())
		})
	})

	Describe("Re-register during in-flight initialCheck", func() {
		It("discards stale initialCheck result after re-registration", func() {
			ht := newFakeHealthTracker()
			slowChecker := &blockingChecker{
				entered: make(chan struct{}, 1),
				release: make(chan struct{}),
				result:  monitor.CheckHealthy,
			}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			})

			ctx, cancel := context.WithCancel(context.Background())
			DeferCleanup(cancel)

			// Register with initialCheck=true in a goroutine (it will block on slowChecker)
			done := make(chan struct{})
			go func() {
				defer close(done)
				m.RegisterProvider("p1", slowChecker, v1alpha1.Unhealthy, true)
			}()

			// Wait for initialCheck to start
			Eventually(slowChecker.entered).Should(Receive())

			// Re-register the same ID with a different checker while initialCheck is in-flight
			fastChecker := &countingChecker{result: monitor.CheckHealthy}
			m.RegisterProvider("p1", fastChecker, v1alpha1.Ready, false)

			// Release the slow checker — its result should be discarded (identity guard)
			slowChecker.release <- struct{}{}
			Eventually(done).Should(BeClosed())

			// Health state should reflect the fast (re-registered) checker's initial state,
			// not the slow checker's result
			Consistently(func() bool {
				state, ok := ht.GetState("p1")
				if !ok {
					return true // not set yet, acceptable
				}
				return state.Status == v1alpha1.Ready
			}).WithTimeout(200 * time.Millisecond).WithPolling(20 * time.Millisecond).Should(BeTrue())

			_ = ctx
		})
	})

	Describe("Start idempotency", func() {
		It("only starts one monitoring loop when called twice", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckHealthy}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			})
			m.RegisterProvider("p1", checker, v1alpha1.Unhealthy, false)

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			Eventually(func() int64 {
				return checker.count.Load()
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(Equal(int64(1)))

			Consistently(func() int64 {
				return checker.count.Load()
			}).WithTimeout(500 * time.Millisecond).WithPolling(50 * time.Millisecond).Should(Equal(int64(1)))
		})
	})
})
