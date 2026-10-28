package monitoring_test

import (
	"sync"
	"sync/atomic"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status Monitor", func() {
	Describe("Debounce", func() {
		It("should coalesce rapid events within the debounce window", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			// Submit 3 rapid changes within the debounce window.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.PENDING,
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.FAILED,
			})

			// Wait for debounce window to elapse.
			time.Sleep(300 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(1), "only the last event should be published")
			Expect(published[0].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should debounce events independently per instance ID", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			// Rapid changes for instance A — should coalesce to last event only.
			debouncer.Submit("instance-a", monitoring.StatusEvent{
				InstanceID: "instance-a",
				Status:     v1alpha1.PENDING,
			})
			debouncer.Submit("instance-a", monitoring.StatusEvent{
				InstanceID: "instance-a",
				Status:     v1alpha1.RUNNING,
			})

			// Single event for instance B — should publish independently.
			debouncer.Submit("instance-b", monitoring.StatusEvent{
				InstanceID: "instance-b",
				Status:     v1alpha1.FAILED,
			})

			// Wait for debounce window to elapse.
			time.Sleep(300 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "each instance should publish independently")

			statuses := map[string]v1alpha1.DatabaseStatus{}
			for _, e := range published {
				statuses[e.InstanceID] = e.Status
			}
			Expect(statuses).To(HaveKeyWithValue("instance-a", v1alpha1.RUNNING))
			Expect(statuses).To(HaveKeyWithValue("instance-b", v1alpha1.FAILED))
		})

		It("should publish events separately when separated by full window gap", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			// First event.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})

			// Wait for debounce window to fully elapse.
			time.Sleep(200 * time.Millisecond)

			// Second event after the window.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.FAILED,
			})

			// Wait for second window to elapse.
			time.Sleep(200 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "events separated by window gap should publish separately")
			Expect(published[0].Status).To(Equal(v1alpha1.RUNNING))
			Expect(published[1].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should wait for in-flight publish before Stop returns", func() {
			publishStarted := make(chan struct{})
			publishComplete := make(chan struct{})

			debouncer := monitoring.NewDebouncer(10*time.Millisecond, func(_ monitoring.StatusEvent) bool {
				close(publishStarted)
				<-publishComplete
				return true
			})
			debouncer.SetReady()

			// Submit an event and wait for the callback to start executing.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})

			Eventually(publishStarted).Should(BeClosed())

			// Call Stop concurrently — it should block while publish is in flight.
			stopDone := make(chan struct{})
			go func() {
				debouncer.Stop()
				close(stopDone)
			}()

			// Stop should NOT have returned yet (publish still blocked).
			Consistently(stopDone, 50*time.Millisecond).ShouldNot(BeClosed())

			// Unblock the publish callback.
			close(publishComplete)

			// Now Stop should return promptly.
			Eventually(stopDone, 100*time.Millisecond).Should(BeClosed())

			// After Stop, no further publishes should occur.
			var postStopPublished atomic.Int32
			debouncer2 := monitoring.NewDebouncer(10*time.Millisecond, func(_ monitoring.StatusEvent) bool {
				postStopPublished.Add(1)
				return true
			})
			debouncer2.SetReady()
			debouncer2.Stop()
			debouncer2.Submit("xyz-789", monitoring.StatusEvent{
				InstanceID: "xyz-789",
				Status:     v1alpha1.PENDING,
			})
			time.Sleep(50 * time.Millisecond)
			Expect(postStopPublished.Load()).To(Equal(int32(0)), "no publish should occur after Stop")
		})

		It("should not reset the debounce timer when a resync delivers an identical pending event", func() {
			// A resync fires during the debounce window with the same status.
			// The timer must NOT be reset — the original deadline should be kept so
			// the event is delivered promptly and not delayed indefinitely by resyncs.
			var mu sync.Mutex
			var publishTimes []time.Time

			debounce := 100 * time.Millisecond
			debouncer := monitoring.NewDebouncer(debounce, func(_ monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				publishTimes = append(publishTimes, time.Now())
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			start := time.Now()
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})

			// Simulate 3 resync calls with the same event before the window elapses.
			time.Sleep(30 * time.Millisecond)
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})
			time.Sleep(30 * time.Millisecond)
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})

			// Wait for the original window (100ms from start) to elapse.
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			times := append([]time.Time(nil), publishTimes...)
			mu.Unlock()

			Expect(times).To(HaveLen(1), "only one publish should occur")
			// Elapsed from start should be close to one window, not three (no timer resets).
			elapsed := times[0].Sub(start)
			Expect(elapsed).To(BeNumerically("<", 3*debounce), "timer should not have been reset by resync submissions")
		})

		It("should suppress a resync submission when the status matches the last published event", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(50*time.Millisecond, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			// First submission: publishes after debounce window.
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})
			time.Sleep(150 * time.Millisecond)

			mu.Lock()
			firstCount := len(published)
			mu.Unlock()
			Expect(firstCount).To(Equal(1), "first event should be published")

			// Resync: same status — must be suppressed entirely.
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})
			time.Sleep(150 * time.Millisecond)

			mu.Lock()
			secondCount := len(published)
			mu.Unlock()
			Expect(secondCount).To(Equal(1), "resync with unchanged status must not trigger a second publish")

			// Status actually changes — must be published.
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.FAILED})
			time.Sleep(150 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "changed status must trigger a new publish")
			Expect(published[1].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should flush pending events when Stop is called before the debounce window elapses", func() {
			// Simulates context cancellation immediately after a status submission:
			// the timer hasn't fired yet, but Stop must still deliver the event.
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(500*time.Millisecond, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			debouncer.SetReady()

			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})
			debouncer.Submit("inst-2", monitoring.StatusEvent{InstanceID: "inst-2", Status: v1alpha1.FAILED})

			// Stop immediately — both timers are still pending (500ms window).
			debouncer.Stop()

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "all pending events must be flushed on Stop")

			statuses := map[string]v1alpha1.DatabaseStatus{}
			for _, e := range published {
				statuses[e.InstanceID] = e.Status
			}
			Expect(statuses).To(HaveKeyWithValue("inst-1", v1alpha1.RUNNING))
			Expect(statuses).To(HaveKeyWithValue("inst-2", v1alpha1.FAILED))
		})

		It("should allow a resync to retry an event that previously failed to publish", func() {
			// If publishFn returns false (all retries exhausted), the event must NOT
			// be recorded in lastPublished. A subsequent resync with the same event
			// must be queued and re-attempted rather than silently dropped.
			var mu sync.Mutex
			var published []monitoring.StatusEvent
			var publishCount atomic.Int32

			debounce := 50 * time.Millisecond
			debouncer := monitoring.NewDebouncer(debounce, func(event monitoring.StatusEvent) bool {
				n := publishCount.Add(1)
				if n == 1 {
					return false // first attempt fails
				}
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			event := monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING}

			// First submission — publishFn fails, must not record as delivered.
			debouncer.Submit("inst-1", event)
			time.Sleep(3 * debounce)

			Expect(publishCount.Load()).To(Equal(int32(1)), "first publish attempt should have fired")

			mu.Lock()
			firstCount := len(published)
			mu.Unlock()
			Expect(firstCount).To(Equal(0), "failed publish must not be recorded as delivered")

			// Resync with same event — must be re-queued and succeed.
			debouncer.Submit("inst-1", event)
			time.Sleep(3 * debounce)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(1), "resync after failed publish must be retried and recorded")
			Expect(published[0].Status).To(Equal(v1alpha1.RUNNING))
		})

		It("should not let a slow retrying first publish overwrite lastPublished after a faster second publish", func() {
			// Scenario: E1 fires, publishFn blocks (simulating retries). Before E1
			// finishes, E2 is submitted and published successfully — lastPublished=E2.
			// When E1 eventually succeeds it must NOT overwrite lastPublished with E1,
			// otherwise the next resync with E2 would be incorrectly suppressed.
			e1Started := make(chan struct{})
			e1Unblock := make(chan struct{})

			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debounce := 20 * time.Millisecond
			debouncer := monitoring.NewDebouncer(debounce, func(event monitoring.StatusEvent) bool {
				if event.Status == v1alpha1.PENDING {
					close(e1Started)
					<-e1Unblock // block until the test unblocks it
				}
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			e1 := monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.PENDING}
			e2 := monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING}

			// Submit E1; wait for the timer to fire and the publish goroutine to start.
			debouncer.Submit("inst-1", e1)
			Eventually(e1Started, 200*time.Millisecond).Should(BeClosed())

			// E1's publish is now blocked. Submit E2; its timer fires and publishes.
			debouncer.Submit("inst-1", e2)
			time.Sleep(5 * debounce)

			// Unblock E1. Its publish succeeds, but lastPublished must remain E2.
			close(e1Unblock)
			time.Sleep(5 * debounce)

			// A resync with E2 must be suppressed (lastPublished == E2).
			debouncer.Submit("inst-1", e2)
			time.Sleep(5 * debounce)

			mu.Lock()
			defer mu.Unlock()
			// E2 published first (while E1 was blocked), then E1 published after
			// being unblocked. The resync E2 must be suppressed because lastPublished
			// is still E2 — E1's completion must not overwrite it.
			Expect(published).To(HaveLen(2), "resync with E2 must be suppressed because lastPublished is E2, not E1")
			Expect(published[0].Status).To(Equal(v1alpha1.RUNNING))
			Expect(published[1].Status).To(Equal(v1alpha1.PENDING))
		})

		It("should evict lastPublished after a successful DELETED publish so a later resync is re-queued", func() {
			// After a database is deleted, its instance ID must be removed from
			// lastPublished so that a subsequent resync (same DELETED event) is
			// re-queued rather than silently dropped.
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debounce := 30 * time.Millisecond
			debouncer := monitoring.NewDebouncer(debounce, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()
			debouncer.SetReady()

			deleted := monitoring.StatusEvent{InstanceID: "inst-del", Status: v1alpha1.DELETED}

			// First publish: DELETED is delivered and must evict the tracking entry.
			debouncer.Submit("inst-del", deleted)
			time.Sleep(5 * debounce)

			mu.Lock()
			firstCount := len(published)
			mu.Unlock()
			Expect(firstCount).To(Equal(1), "DELETED event should be published once")

			// Resync: same DELETED event must be re-queued (not suppressed).
			debouncer.Submit("inst-del", deleted)
			time.Sleep(5 * debounce)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2), "resync after DELETED must be re-published, not suppressed")
			Expect(published[1].Status).To(Equal(v1alpha1.DELETED))
		})

		It("should suppress submissions until SetReady is called", func() {
			// Simulates informers where one cache is slower than the debounce interval:
			// events submitted before SetReady must never be queued, so the
			// publish that matters (the post-sync sweep) is the authoritative one.
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debounce := 50 * time.Millisecond
			debouncer := monitoring.NewDebouncer(debounce, func(event monitoring.StatusEvent) bool {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
				return true
			})
			defer debouncer.Stop()

			// Submit while gate is closed — these must be silently dropped.
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.RUNNING})
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.PENDING})

			// Simulate one informer being slower: wait longer than the debounce window
			// to prove the gate prevents the timer from firing during sync.
			time.Sleep(2 * debounce)

			mu.Lock()
			earlyCount := len(published)
			mu.Unlock()
			Expect(earlyCount).To(Equal(0), "no events should publish before SetReady")

			// Now the sweep opens the gate and submits the authoritative snapshot.
			debouncer.SetReady()
			debouncer.Submit("inst-1", monitoring.StatusEvent{InstanceID: "inst-1", Status: v1alpha1.FAILED})

			time.Sleep(2 * debounce)

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(1), "only the post-ready sweep event should publish")
			Expect(published[0].Status).To(Equal(v1alpha1.FAILED))
		})
	})
})
