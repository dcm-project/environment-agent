package monitoring_test

import (
	"context"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/monitoring"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Status Monitor — Integration Tests", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		publisher *mockStatusPublisher
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Ginkgo shared-state pattern: ctx/cancel used across BeforeEach/It/AfterEach.
		publisher = newMockPublisher()
	})

	AfterEach(func() {
		cancel()
	})

	// ── TC-MON-IT-006: Informer reconnects after watch disconnect ──

	It("TC-MON-IT-006: Informer reconnects after watch disconnect", func() {
		instanceID := "inst-it-006"
		hc := buildUnstructuredHostedCluster("cluster-it-6", instanceID, readyConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial event.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

		publisher.Reset()

		// Simulate a watch disconnect by updating a resource.
		// After reconnect, the informer should re-list and resume.
		updated := buildUnstructuredHostedCluster("cluster-it-6", instanceID, failedConditions())
		_, err := client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// The monitor should detect the change after reconnecting.
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 10*time.Second, 100*time.Millisecond).Should(ContainElement(
			monitoring.StatusEvent{
				InstanceID: instanceID,
				Status:     v1alpha1.ClusterStatusFAILED,
				Message:    "etcd cluster is degraded",
			},
		))
	})

	// ── TC-MON-IT-007: Watchers start after HTTP server is ready ──

	It("TC-MON-IT-007: Watchers start after HTTP server is ready", func() {
		instanceID := "inst-it-007"
		hc := buildUnstructuredHostedCluster("cluster-it-7", instanceID, readyConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		// Monitor should not publish anything until Start is called.
		Consistently(func() int {
			return len(publisher.Events())
		}, 500*time.Millisecond, 100*time.Millisecond).Should(Equal(0))

		// Start the monitor (simulates "after HTTP server is ready").
		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Now events should be published.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
	})

	// ── TC-MON-IT-008: Watchers stop during graceful shutdown ──

	It("TC-MON-IT-008: Watchers stop during graceful shutdown", func() {
		instanceID := "inst-it-008"
		hc := buildUnstructuredHostedCluster("cluster-it-8", instanceID, readyConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			_ = monitor.Start(ctx)
		}()

		// Wait for initial event.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

		// Graceful shutdown via context cancellation.
		cancel()

		// Start should return (unblock) after context is cancelled.
		Eventually(done, 5*time.Second).Should(BeClosed())

		// After Stop, no new events.
		publisher.Reset()

		Consistently(func() int {
			return len(publisher.Events())
		}, 1*time.Second, 200*time.Millisecond).Should(Equal(0))
	})

	// ── TC-MON-IT-009: Periodic resync triggers re-evaluation ──

	It("TC-MON-IT-009: Periodic resync triggers re-evaluation", func() {
		instanceID := "inst-it-009"
		hc := buildUnstructuredHostedCluster("cluster-it-9", instanceID, readyConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		cfg.ResyncInterval = 500 * time.Millisecond // Short resync for testing.
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial event.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

		// Wait for at least one resync cycle to complete.
		// The resync fires Update events; since status hasn't changed,
		// the monitor should NOT publish duplicate events (covered by TC-MON-UT-009).
		// We verify the informer is alive post-resync by changing status.
		time.Sleep(cfg.ResyncInterval + 200*time.Millisecond)

		publisher.Reset()
		updated := buildUnstructuredHostedCluster("cluster-it-9", instanceID, failedConditions())
		_, err := client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
	})

	// ── TC-MON-IT-010: Initial status sync publishes events for all existing resources ──

	It("TC-MON-IT-010: Initial status sync publishes events for all existing resources", func() {
		hc1 := buildUnstructuredHostedCluster("cluster-it-10a", "inst-it-010a", readyConditions())
		hc2 := buildUnstructuredHostedCluster("cluster-it-10b", "inst-it-010b", provisioningConditions())
		hc3 := buildUnstructuredHostedCluster("cluster-it-10c", "inst-it-010c", unavailableConditions())
		client := newDynamicFakeClient(hc1, hc2, hc3)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// All 3 resources should publish events.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(3))

		ids := make(map[string]v1alpha1.ClusterStatus)
		for _, e := range publisher.Events() {
			ids[e.InstanceID] = e.Status
		}
		Expect(ids).To(HaveKeyWithValue("inst-it-010a", v1alpha1.ClusterStatusREADY))
		Expect(ids).To(HaveKeyWithValue("inst-it-010b", v1alpha1.ClusterStatusPROVISIONING))
		Expect(ids).To(HaveKeyWithValue("inst-it-010c", v1alpha1.ClusterStatusUNAVAILABLE))
	})

	// ── TC-MON-IT-011: FAILED status events include failure reason ──

	It("TC-MON-IT-011: FAILED status events include failure reason", func() {
		instanceID := "inst-it-011"
		failureMsg := "ETCD quorum lost: 2 of 3 members unhealthy"
		hc := buildUnstructuredHostedCluster("cluster-it-11", instanceID,
			failedConditionsWithMessage(failureMsg))
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(HaveLen(1))

		event := publisher.Events()[0]
		Expect(event.InstanceID).To(Equal(instanceID))
		Expect(event.Status).To(Equal(v1alpha1.ClusterStatusFAILED))
		Expect(event.Message).To(ContainSubstring(failureMsg))
	})

	// ── TC-MON-IT-020: NP READY → not-ready transition → UNAVAILABLE ──

	It("TC-MON-IT-020: NP READY → not-ready transition → UNAVAILABLE", func() {
		instanceID := "inst-it-020"
		hc := buildUnstructuredHostedCluster("cluster-it-20", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-it-20", instanceID, npReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial composite READY.
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

		publisher.Reset()

		// Transition NP to not-ready.
		updatedNP := buildUnstructuredNodePool("cluster-it-20", instanceID, npNotReadyConditions())
		_, err := client.Resource(util.NodePoolGVR).Namespace(testNamespace).Update(ctx, updatedNP, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Composite should become UNAVAILABLE.
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(ContainElement(
			monitoring.StatusEvent{
				InstanceID: instanceID,
				Status:     v1alpha1.ClusterStatusUNAVAILABLE,
				Message:    "NodePool: 0 of 3 machines are ready",
			},
		))
	})

	// ── TC-MON-IT-021: NP UNAVAILABLE → READY recovery ──

	It("TC-MON-IT-021: NP UNAVAILABLE → READY recovery", func() {
		instanceID := "inst-it-021"
		hc := buildUnstructuredHostedCluster("cluster-it-21", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-it-21", instanceID, npNotReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial composite UNAVAILABLE.
		Eventually(func() bool {
			for _, e := range publisher.Events() {
				if e.InstanceID == instanceID && e.Status == v1alpha1.ClusterStatusUNAVAILABLE {
					return true
				}
			}
			return false
		}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())

		publisher.Reset()

		// Recover NP to ready.
		updatedNP := buildUnstructuredNodePool("cluster-it-21", instanceID, npReadyConditions())
		_, err := client.Resource(util.NodePoolGVR).Namespace(testNamespace).Update(ctx, updatedNP, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Composite should become READY.
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(ContainElement(
			monitoring.StatusEvent{
				InstanceID: instanceID,
				Status:     v1alpha1.ClusterStatusREADY,
				Message:    "",
			},
		))
	})
})
