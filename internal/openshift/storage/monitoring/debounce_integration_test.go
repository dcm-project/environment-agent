package monitoring_test

import (
	"sync"
	"sync/atomic"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status Monitor", func() {
	Describe("Debounce", func() {
		It("should coalesce rapid events within the debounce window (TC-I066)", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.PROVISIONING,
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.FAILED,
			})

			Eventually(func() []monitoring.StatusEvent {
				mu.Lock()
				defer mu.Unlock()
				cp := make([]monitoring.StatusEvent, len(published))
				copy(cp, published)
				return cp
			}, 500*time.Millisecond, 20*time.Millisecond).Should(HaveLen(1))

			mu.Lock()
			defer mu.Unlock()
			Expect(published[0].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should debounce events independently per instance ID (TC-I115)", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			debouncer.Submit("instance-a", monitoring.StatusEvent{
				InstanceID: "instance-a",
				Status:     v1alpha1.PROVISIONING,
			})
			debouncer.Submit("instance-a", monitoring.StatusEvent{
				InstanceID: "instance-a",
				Status:     v1alpha1.RUNNING,
			})
			debouncer.Submit("instance-b", monitoring.StatusEvent{
				InstanceID: "instance-b",
				Status:     v1alpha1.FAILED,
			})

			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 500*time.Millisecond, 20*time.Millisecond).Should(Equal(2))

			mu.Lock()
			defer mu.Unlock()
			statuses := map[string]v1alpha1.StorageStatus{}
			for _, e := range published {
				statuses[e.InstanceID] = e.Status
			}
			Expect(statuses).To(HaveKeyWithValue("instance-a", v1alpha1.RUNNING))
			Expect(statuses).To(HaveKeyWithValue("instance-b", v1alpha1.FAILED))
		})

		It("should publish events separately when separated by full window gap (TC-I067)", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(100*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 500*time.Millisecond, 20*time.Millisecond).Should(Equal(1))

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.FAILED,
			})
			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 500*time.Millisecond, 20*time.Millisecond).Should(Equal(2))

			mu.Lock()
			defer mu.Unlock()
			Expect(published[0].Status).To(Equal(v1alpha1.RUNNING))
			Expect(published[1].Status).To(Equal(v1alpha1.FAILED))
		})

		It("should flush pending events on Stop", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			debouncer := monitoring.NewDebouncer(time.Hour, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.PROVISIONING,
				Message:    "pending",
			})
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
				Message:    "latest",
			})
			debouncer.Submit("xyz-789", monitoring.StatusEvent{
				InstanceID: "xyz-789",
				Status:     v1alpha1.FAILED,
				Message:    "other",
			})

			debouncer.Stop()

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveLen(2))
			byID := map[string]monitoring.StatusEvent{}
			for _, e := range published {
				byID[e.InstanceID] = e
			}
			Expect(byID).To(HaveKeyWithValue("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
				Message:    "latest",
			}))
			Expect(byID).To(HaveKeyWithValue("xyz-789", monitoring.StatusEvent{
				InstanceID: "xyz-789",
				Status:     v1alpha1.FAILED,
				Message:    "other",
			}))
		})

		It("should wait for in-flight publish before Stop returns (TC-U079)", func() {
			publishStarted := make(chan struct{})
			publishComplete := make(chan struct{})

			debouncer := monitoring.NewDebouncer(10*time.Millisecond, func(_ monitoring.StatusEvent) {
				close(publishStarted)
				<-publishComplete
			})

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
			})

			Eventually(publishStarted).Should(BeClosed())

			stopDone := make(chan struct{})
			go func() {
				debouncer.Stop()
				close(stopDone)
			}()

			Consistently(stopDone, 50*time.Millisecond).ShouldNot(BeClosed())
			close(publishComplete)
			Eventually(stopDone, 100*time.Millisecond).Should(BeClosed())

			var postStopPublished atomic.Int32
			debouncer2 := monitoring.NewDebouncer(10*time.Millisecond, func(_ monitoring.StatusEvent) {
				postStopPublished.Add(1)
			})
			debouncer2.Stop()
			debouncer2.Submit("xyz-789", monitoring.StatusEvent{
				InstanceID: "xyz-789",
				Status:     v1alpha1.PROVISIONING,
			})
			Consistently(postStopPublished.Load, 50*time.Millisecond, 10*time.Millisecond).Should(Equal(int32(0)))
		})

		It("should publish only the latest event when a timer is replaced (stale callback)", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent

			// Long enough that we can replace the timer before the first fires,
			// and short enough that Eventually still completes quickly.
			interval := 80 * time.Millisecond
			debouncer := monitoring.NewDebouncer(interval, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.PROVISIONING,
				Message:    "old",
			})
			// Replace before the first timer fires.
			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
				Message:    "new",
			})

			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 500*time.Millisecond, 20*time.Millisecond).Should(Equal(1))

			Consistently(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 150*time.Millisecond, 20*time.Millisecond).Should(Equal(1))

			mu.Lock()
			defer mu.Unlock()
			Expect(published[0].Status).To(Equal(v1alpha1.RUNNING))
			Expect(published[0].Message).To(Equal("new"))
		})

		It("should serialize publishes for the same instance while a callback is in flight", func() {
			var mu sync.Mutex
			var published []monitoring.StatusEvent
			publishStarted := make(chan struct{})
			publishGate := make(chan struct{})

			debouncer := monitoring.NewDebouncer(10*time.Millisecond, func(event monitoring.StatusEvent) {
				if event.Status == v1alpha1.PROVISIONING {
					close(publishStarted)
					<-publishGate
				}
				mu.Lock()
				defer mu.Unlock()
				published = append(published, event)
			})
			defer debouncer.Stop()

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.PROVISIONING,
				Message:    "first",
			})

			Eventually(publishStarted).Should(BeClosed())

			debouncer.Submit("abc-123", monitoring.StatusEvent{
				InstanceID: "abc-123",
				Status:     v1alpha1.RUNNING,
				Message:    "second",
			})

			close(publishGate)

			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 500*time.Millisecond, 20*time.Millisecond).Should(Equal(2))

			mu.Lock()
			defer mu.Unlock()
			Expect(published[0].Status).To(Equal(v1alpha1.PROVISIONING))
			Expect(published[1].Status).To(Equal(v1alpha1.RUNNING))
		})

		It("should tolerate concurrent Submit bursts without races (same and distinct IDs)", func() {
			var mu sync.Mutex
			published := map[string]monitoring.StatusEvent{}

			debouncer := monitoring.NewDebouncer(30*time.Millisecond, func(event monitoring.StatusEvent) {
				mu.Lock()
				defer mu.Unlock()
				published[event.InstanceID] = event
			})
			defer debouncer.Stop()

			const workers = 16
			const perWorker = 50
			var wg sync.WaitGroup
			wg.Add(workers)
			for w := 0; w < workers; w++ {
				go func(worker int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						id := "vol-a"
						if worker%2 == 1 {
							id = "vol-b"
						}
						debouncer.Submit(id, monitoring.StatusEvent{
							InstanceID: id,
							Status:     v1alpha1.RUNNING,
							Message:    "burst",
						})
					}
				}(w)
			}
			wg.Wait()

			Eventually(func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}, 500*time.Millisecond, 20*time.Millisecond).Should(Equal(2))

			mu.Lock()
			defer mu.Unlock()
			Expect(published).To(HaveKey("vol-a"))
			Expect(published).To(HaveKey("vol-b"))
			Expect(published["vol-a"].Status).To(Equal(v1alpha1.RUNNING))
			Expect(published["vol-b"].Status).To(Equal(v1alpha1.RUNNING))
		})
	})
})
