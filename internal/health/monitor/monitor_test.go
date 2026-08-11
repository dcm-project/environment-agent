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

// captureHandler records slog records for assertion.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

func findRecord(records []slog.Record, msg string) (slog.Record, bool) {
	for _, r := range records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

func recordAttr(rec slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

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
			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Unhealthy, false)

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
				m.RegisterProvider("p1", slowChecker, "test-service", v1alpha1.Unhealthy, true)
			}()

			// Wait for initialCheck to start
			Eventually(slowChecker.entered).Should(Receive())

			// Re-register the same ID with a different checker while initialCheck is in-flight
			fastChecker := &countingChecker{result: monitor.CheckHealthy}
			m.RegisterProvider("p1", fastChecker, "test-service", v1alpha1.Ready, false)

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

	Describe("OnTransition callback", func() {
		It("fires on state transition during periodic check", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckFailed}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    50 * time.Millisecond,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 1,
			})

			var mu sync.Mutex
			var transitions []struct{ from, to v1alpha1.ProviderStatus }
			m.SetOnTransition(func(_ string, from, to v1alpha1.ProviderStatus) {
				mu.Lock()
				transitions = append(transitions, struct{ from, to v1alpha1.ProviderStatus }{from, to})
				mu.Unlock()
			})

			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, false)

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(transitions)
			}).WithTimeout(2 * time.Second).WithPolling(20 * time.Millisecond).Should(BeNumerically(">=", 1))

			mu.Lock()
			Expect(transitions[0].from).To(Equal(v1alpha1.Ready))
			Expect(transitions[0].to).To(Equal(v1alpha1.Unavailable))
			mu.Unlock()
		})

		It("fires on state transition during initial check", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckUnhealthy}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			})

			var mu sync.Mutex
			var transitions []struct{ from, to v1alpha1.ProviderStatus }
			m.SetOnTransition(func(_ string, from, to v1alpha1.ProviderStatus) {
				mu.Lock()
				transitions = append(transitions, struct{ from, to v1alpha1.ProviderStatus }{from, to})
				mu.Unlock()
			})

			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, true)

			mu.Lock()
			Expect(transitions).To(HaveLen(1))
			Expect(transitions[0].from).To(Equal(v1alpha1.Ready))
			Expect(transitions[0].to).To(Equal(v1alpha1.Unhealthy))
			mu.Unlock()
		})

		It("recovers from panicking callback and keeps monitoring", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckFailed}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    50 * time.Millisecond,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 1,
			})

			panicOnce := sync.Once{}
			postPanicCalls := atomic.Int64{}
			m.SetOnTransition(func(_ string, _, _ v1alpha1.ProviderStatus) {
				panicOnce.Do(func() { panic("boom") })
				postPanicCalls.Add(1)
			})

			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, false)

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			// First transition panics; monitor must survive and keep checking.
			Eventually(func() int64 {
				return checker.count.Load()
			}).WithTimeout(2 * time.Second).WithPolling(20 * time.Millisecond).Should(BeNumerically(">=", 3))
		})

		It("recovers from panicking callback during initial check", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckUnhealthy}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			})

			m.SetOnTransition(func(_ string, _, _ v1alpha1.ProviderStatus) {
				panic("boom during initial check")
			})

			Expect(func() {
				m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, true)
			}).NotTo(Panic())

			state, ok := ht.GetState("p1")
			Expect(ok).To(BeTrue())
			Expect(state.Status).To(Equal(v1alpha1.Unhealthy))
		})

		It("does not fire when state stays the same", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckHealthy}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    50 * time.Millisecond,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			})

			callCount := atomic.Int64{}
			m.SetOnTransition(func(_ string, _, _ v1alpha1.ProviderStatus) {
				callCount.Add(1)
			})

			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, false)

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			// Wait for multiple checks to run
			Eventually(func() int64 {
				return checker.count.Load()
			}).WithTimeout(2 * time.Second).WithPolling(20 * time.Millisecond).Should(BeNumerically(">=", 3))

			Expect(callCount.Load()).To(Equal(int64(0)))
		})
	})

	Describe("Stop during in-flight periodic check", func() {
		It("returns promptly instead of waiting for checkTimeout", func() {
			ht := newFakeHealthTracker()
			checker := &blockingChecker{
				entered: make(chan struct{}, 1),
				release: make(chan struct{}),
				result:  monitor.CheckHealthy,
			}

			m := newTestMonitor(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second, // much larger than the Stop() bound below
				FailureThreshold: 3,
			})
			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Unhealthy, false)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel() // deliberately NOT cancelled before Stop(), to reproduce the hang scenario
			m.Start(ctx)

			Eventually(checker.entered).Should(Receive())

			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				m.Stop()
			}()

			Eventually(stopped).WithTimeout(200*time.Millisecond).Should(BeClosed(),
				"Stop() must cancel in-flight checks via m.stopCtx rather than waiting for checkTimeout")

			close(checker.release)
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
			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Unhealthy, false)

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

	Describe("Health monitoring logging (IT-HMN-190, IT-HMN-191)", func() {
		It("logs INFO on RegisterProvider and DEBUG per health check", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckHealthy}
			ch := &captureHandler{}
			m := monitor.New(ht, config.HealthConfig{
				CheckInterval:    50 * time.Millisecond,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			}, slog.New(ch))

			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, false)

			registered, ok := findRecord(ch.all(), "provider registered for health monitoring")
			Expect(ok).To(BeTrue())
			Expect(registered.Level).To(Equal(slog.LevelInfo))
			v, ok := recordAttr(registered, "provider_id")
			Expect(ok).To(BeTrue())
			Expect(v.String()).To(Equal("p1"))
			v, ok = recordAttr(registered, "service_type")
			Expect(ok).To(BeTrue())
			Expect(v.String()).To(Equal("test-service"))
			v, ok = recordAttr(registered, "replaced_existing")
			Expect(ok).To(BeTrue())
			Expect(v.Bool()).To(BeFalse())

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			Eventually(func() bool {
				_, ok := findRecord(ch.all(), "health check completed")
				return ok
			}).WithTimeout(2 * time.Second).WithPolling(20 * time.Millisecond).Should(BeTrue())

			checkRec, _ := findRecord(ch.all(), "health check completed")
			Expect(checkRec.Level).To(Equal(slog.LevelDebug))
			v, _ = recordAttr(checkRec, "provider_id")
			Expect(v.String()).To(Equal("p1"))
			v, ok = recordAttr(checkRec, "service_type")
			Expect(ok).To(BeTrue())
			Expect(v.String()).To(Equal("test-service"))
			v, ok = recordAttr(checkRec, "result")
			Expect(ok).To(BeTrue())
			Expect(v.Kind()).To(Equal(slog.KindString),
				"result must be stored as a native string value, not a wrapped HealthCheckResult (Kind=Any), or it will serialize as a raw integer in JSON")
			Expect(v.String()).To(Equal("healthy"))
			_, ok = recordAttr(checkRec, "duration")
			Expect(ok).To(BeTrue())
		})

		It("logs 'replaced_existing' true when re-registering the same provider ID", func() {
			ht := newFakeHealthTracker()
			ch := &captureHandler{}
			m := monitor.New(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			}, slog.New(ch))

			m.RegisterProvider("p1", &countingChecker{result: monitor.CheckHealthy}, "test-service", v1alpha1.Ready, false)
			m.RegisterProvider("p1", &countingChecker{result: monitor.CheckHealthy}, "test-service", v1alpha1.Ready, false)

			records := ch.all()
			var registrations []slog.Record
			for _, r := range records {
				if r.Message == "provider registered for health monitoring" {
					registrations = append(registrations, r)
				}
			}
			Expect(registrations).To(HaveLen(2))

			v, ok := recordAttr(registrations[0], "replaced_existing")
			Expect(ok).To(BeTrue())
			Expect(v.Bool()).To(BeFalse())

			v, ok = recordAttr(registrations[1], "replaced_existing")
			Expect(ok).To(BeTrue())
			Expect(v.Bool()).To(BeTrue())
		})

		It("does not log health check completed when the result is discarded (deregistered mid-check)", func() {
			ht := newFakeHealthTracker()
			ch := &captureHandler{}
			checker := &blockingChecker{
				entered: make(chan struct{}, 1),
				release: make(chan struct{}),
				result:  monitor.CheckHealthy,
			}
			m := monitor.New(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			}, slog.New(ch))
			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Unhealthy, false)

			ctx, cancel := context.WithCancel(context.Background())
			m.Start(ctx)
			DeferCleanup(m.Stop)
			DeferCleanup(cancel)

			Eventually(checker.entered).Should(Receive())
			m.DeregisterProvider("p1")
			checker.release <- struct{}{}

			Consistently(func() bool {
				_, ok := findRecord(ch.all(), "health check completed")
				return ok
			}).WithTimeout(200*time.Millisecond).WithPolling(20*time.Millisecond).Should(BeFalse(),
				"the discarded result must not be logged, since it was never applied")
		})

		It("logs WARN transition parity when the initial check changes state", func() {
			ht := newFakeHealthTracker()
			checker := &countingChecker{result: monitor.CheckUnhealthy}
			ch := &captureHandler{}
			m := monitor.New(ht, config.HealthConfig{
				CheckInterval:    10 * time.Second,
				CheckTimeout:     5 * time.Second,
				FailureThreshold: 3,
			}, slog.New(ch))

			m.RegisterProvider("p1", checker, "test-service", v1alpha1.Ready, true)

			rec, ok := findRecord(ch.all(), "provider health transition")
			Expect(ok).To(BeTrue(), "initial check must log a transition, same as periodic checks")
			Expect(rec.Level).To(Equal(slog.LevelWarn))
			v, ok := recordAttr(rec, "service_type")
			Expect(ok).To(BeTrue())
			Expect(v.String()).To(Equal("test-service"))
			v, _ = recordAttr(rec, "from")
			Expect(v.String()).To(Equal(string(v1alpha1.Ready)))
			v, _ = recordAttr(rec, "to")
			Expect(v.String()).To(Equal(string(v1alpha1.Unhealthy)))

			checkRec, ok := findRecord(ch.all(), "health check completed")
			Expect(ok).To(BeTrue())
			v, ok = recordAttr(checkRec, "result")
			Expect(ok).To(BeTrue())
			Expect(v.Kind()).To(Equal(slog.KindString),
				"result must be stored as a native string value, not a wrapped HealthCheckResult (Kind=Any), or it will serialize as a raw integer in JSON")
			Expect(v.String()).To(Equal("unhealthy"))
		})
	})
})
