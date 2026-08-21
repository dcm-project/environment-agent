package monitoring_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/monitoring"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Status Monitor — Unit Tests", func() {
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

	// ── TC-MON-UT-001: Condition change publishes CloudEvent with correct format ──

	It("TC-MON-UT-001: Condition change publishes CloudEvent with correct format", func() {
		instanceID := "inst-001"
		hc := buildUnstructuredHostedCluster("cluster-1", instanceID, readyConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for the monitor to process the event.
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(HaveLen(1))

		event := publisher.Events()[0]
		Expect(event.InstanceID).To(Equal(instanceID))
		Expect(event.Status).To(Equal(v1alpha1.ClusterStatusREADY))

		// Verify CloudEvent format.
		ce, err := monitoring.NewStatusCloudEvent(testProviderName, instanceID, v1alpha1.ClusterStatusREADY, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(ce).NotTo(BeNil())
		Expect(ce.SpecVersion).To(Equal("1.0"))
		Expect(ce.Type).To(Equal("dcm.status.cluster"))
		Expect(ce.Subject).To(Equal("dcm.cluster"))
		Expect(ce.DataContentType).To(Equal("application/json"))
		Expect(ce.ID).NotTo(BeEmpty())
		Expect(ce.Time).NotTo(BeEmpty())
		Expect(ce.Source).NotTo(BeEmpty())

		// Verify payload structure.
		payloadBytes, err := json.Marshal(ce.Data)
		Expect(err).NotTo(HaveOccurred())
		var payload monitoring.StatusPayload
		Expect(json.Unmarshal(payloadBytes, &payload)).To(Succeed())
		Expect(payload.ID).To(Equal(instanceID))
		Expect(payload.Status).To(Equal(string(v1alpha1.ClusterStatusREADY)))
	})

	// ── TC-MON-UT-002: Deletion event publishes DELETED status ──

	It("TC-MON-UT-002: Deletion event publishes DELETED status", func() {
		instanceID := "inst-002"
		hc := buildUnstructuredHostedCluster("cluster-2", instanceID, readyConditions())
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

		// Delete the resource.
		err := client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Delete(ctx, "cluster-2", metav1.DeleteOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(ContainElement(
			monitoring.StatusEvent{
				InstanceID: instanceID,
				Status:     v1alpha1.ClusterStatusDELETED,
				Message:    "",
			},
		))
	})

	// ── TC-MON-UT-003: Non-DCM resources are ignored ──

	It("TC-MON-UT-003: Non-DCM resources are ignored", func() {
		// Resource without DCM labels.
		hc := buildUnstructuredHostedCluster("non-dcm", "inst-003", readyConditions(), withoutDCMLabels())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// No events should be published.
		Consistently(func() int {
			return len(publisher.Events())
		}, 2*time.Second, 200*time.Millisecond).Should(Equal(0))
	})

	// ── TC-MON-UT-004: Debounce rapid oscillations ──

	It("TC-MON-UT-004: Debounce rapid oscillations", func() {
		instanceID := "inst-004"
		cfg := defaultMonitorConfig()
		cfg.DebounceInterval = 500 * time.Millisecond

		var (
			mu       sync.Mutex
			received []monitoring.StatusEvent
		)
		debouncer := monitoring.NewDebouncer(cfg.DebounceInterval, func(event monitoring.StatusEvent) {
			mu.Lock()
			received = append(received, event)
			mu.Unlock()
		})
		defer debouncer.Stop()

		// Submit rapid events.
		debouncer.Submit(instanceID, monitoring.StatusEvent{InstanceID: instanceID, Status: v1alpha1.ClusterStatusPROVISIONING})
		debouncer.Submit(instanceID, monitoring.StatusEvent{InstanceID: instanceID, Status: v1alpha1.ClusterStatusREADY})
		debouncer.Submit(instanceID, monitoring.StatusEvent{InstanceID: instanceID, Status: v1alpha1.ClusterStatusUNAVAILABLE})

		// Within debounce window, at most 1 event should fire.
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(received)
		}, 2*time.Second, 100*time.Millisecond).Should(Equal(1))

		// The last submitted event should be the one published.
		mu.Lock()
		Expect(received[0].Status).To(Equal(v1alpha1.ClusterStatusUNAVAILABLE))
		mu.Unlock()
	})

	// ── TC-MON-UT-005: Startup re-lists and publishes current statuses ──

	It("TC-MON-UT-005: Startup re-lists and publishes current statuses", func() {
		hc1 := buildUnstructuredHostedCluster("cluster-a", "inst-005a", readyConditions())
		hc2 := buildUnstructuredHostedCluster("cluster-b", "inst-005b", provisioningConditions())
		hc3 := buildUnstructuredHostedCluster("cluster-c", "inst-005c", failedConditions())
		client := newDynamicFakeClient(hc1, hc2, hc3)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// All 3 existing resources should publish events on startup.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(3))

		ids := make(map[string]bool)
		for _, e := range publisher.Events() {
			ids[e.InstanceID] = true
		}
		Expect(ids).To(HaveKey("inst-005a"))
		Expect(ids).To(HaveKey("inst-005b"))
		Expect(ids).To(HaveKey("inst-005c"))
	})

	// ── TC-MON-UT-007: StatusPublisher interface decoupling ──

	It("TC-MON-UT-007: StatusPublisher interface decoupling", func() {
		instanceID := "inst-007"
		hc := buildUnstructuredHostedCluster("cluster-7", instanceID, readyConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// All events go through StatusPublisher.Publish().
		Eventually(func() int {
			return publisher.CallCount()
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

		// Verify the mock received the event (proves decoupling).
		Expect(publisher.Events()).NotTo(BeEmpty())
	})

	// ── TC-MON-UT-008: Label selector filtering ──

	It("TC-MON-UT-008: Label selector filtering", func() {
		dcmHC := buildUnstructuredHostedCluster("dcm-cluster", "inst-008", readyConditions())
		nonDCMHC := buildUnstructuredHostedCluster("other-cluster", "inst-008b", readyConditions(), withoutDCMLabels())
		client := newDynamicFakeClient(dcmHC, nonDCMHC)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Only the DCM-labeled resource should generate an event.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(1))

		Expect(publisher.Events()[0].InstanceID).To(Equal("inst-008"))

		// Verify no extra events.
		Consistently(func() int {
			return len(publisher.Events())
		}, 1*time.Second, 200*time.Millisecond).Should(Equal(1))
	})

	// ── TC-MON-UT-009: Condition update without DCM status change does not publish ──

	It("TC-MON-UT-009: Condition update without DCM status change does not publish", func() {
		instanceID := "inst-009"
		hc := buildUnstructuredHostedCluster("cluster-9", instanceID, readyConditions())
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
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(1))

		publishCountBefore := publisher.CallCount()

		// Update the resource with same status (still READY).
		updated := buildUnstructuredHostedCluster("cluster-9", instanceID, readyConditions())
		_, err := client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// No new event should be published (status unchanged).
		Consistently(func() int {
			return publisher.CallCount() - publishCountBefore
		}, 2*time.Second, 200*time.Millisecond).Should(Equal(0))
	})

	// ── TC-MON-UT-010: NATS publish failure retries and drops on exhaustion ──

	It("TC-MON-UT-010: NATS publish failure retries and drops on exhaustion", func() {
		instanceID := "inst-010"
		cfg := defaultMonitorConfig()
		cfg.PublishRetryMax = 3
		cfg.PublishRetryInterval = 10 * time.Millisecond

		By("Scenario A: first 3 calls fail, 4th succeeds")
		hcA := buildUnstructuredHostedCluster("cluster-10a", instanceID, readyConditions())
		clientA := newDynamicFakeClient(hcA)
		pubA := newMockPublisher()
		callCount := 0
		pubA.SetErrorFunc(func() error {
			callCount++
			if callCount <= 3 {
				return errors.New("publish failed")
			}
			return nil
		})
		monitorA := monitoring.New(clientA, cfg, pubA, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitorA.Start(ctx)
		}()

		// After retries, the 4th attempt should succeed.
		Eventually(func() int {
			return len(pubA.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
		// Total calls: 3 failures + 1 success = 4.
		Expect(pubA.CallCount()).To(Equal(4))

		cancel()
		ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // Reset context mid-test after cancellation.

		By("Scenario B: always fails, drops after exhaustion")
		hcB := buildUnstructuredHostedCluster("cluster-10b", "inst-010b", readyConditions())
		clientB := newDynamicFakeClient(hcB)
		publisher.SetError(errors.New("publish failed"))
		monitorB := monitoring.New(clientB, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitorB.Start(ctx)
		}()

		// Should attempt 1 initial + PublishRetryMax retries then drop.
		Eventually(func() int {
			return publisher.CallCount()
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(cfg.PublishRetryMax + 1))

		// No events stored (all failed).
		Expect(publisher.Events()).To(BeEmpty())

		// Subsequent events should still be processed (not blocked).
		publisher.SetError(nil)
		publisher.Reset()

		updated := buildUnstructuredHostedCluster("cluster-10b", "inst-010b", failedConditions())
		_, err := clientB.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
	})

	// ── TC-MON-UT-011: Update event with deletionTimestamp publishes DELETING ──

	It("TC-MON-UT-011: Update event with deletionTimestamp publishes DELETING", func() {
		instanceID := "inst-011"
		hc := buildUnstructuredHostedCluster("cluster-11", instanceID, readyConditions())
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

		// Update with deletionTimestamp set.
		updated := buildUnstructuredHostedCluster("cluster-11", instanceID, readyConditions(),
			withDeletionTimestamp(time.Now()))
		_, err := client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(ContainElement(
			monitoring.StatusEvent{
				InstanceID: instanceID,
				Status:     v1alpha1.ClusterStatusDELETING,
				Message:    "",
			},
		))

		publisher.Reset()

		// Fully delete the resource (Delete event).
		err = client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Delete(ctx, "cluster-11", metav1.DeleteOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(ContainElement(
			monitoring.StatusEvent{
				InstanceID: instanceID,
				Status:     v1alpha1.ClusterStatusDELETED,
				Message:    "",
			},
		))
	})

	// ── TC-MON-UT-012: dcm-instance-id secondary index returns correct resources ──

	It("TC-MON-UT-012: dcm-instance-id secondary index returns correct resources", func() {
		hc1 := buildUnstructuredHostedCluster("cluster-a", "inst-012a", readyConditions())
		hc2 := buildUnstructuredHostedCluster("cluster-b", "inst-012b", provisioningConditions())

		keys1, err := monitoring.InstanceIDIndexFunc(hc1)
		Expect(err).NotTo(HaveOccurred())
		Expect(keys1).To(ConsistOf("inst-012a"))

		keys2, err := monitoring.InstanceIDIndexFunc(hc2)
		Expect(err).NotTo(HaveOccurred())
		Expect(keys2).To(ConsistOf("inst-012b"))
	})

	// ── TC-MON-UT-013: FAILED CloudEvent message includes failure reason ──

	It("TC-MON-UT-013: FAILED CloudEvent message includes failure reason", func() {
		instanceID := "inst-013"
		failureMsg := "etcd cluster is degraded"
		hc := buildUnstructuredHostedCluster("cluster-13", instanceID, failedConditionsWithMessage(failureMsg))
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
		Expect(event.Status).To(Equal(v1alpha1.ClusterStatusFAILED))
		Expect(event.Message).To(ContainSubstring(failureMsg))
	})

	// ── TC-MON-UT-014: UNAVAILABLE to READY recovery transition ──

	It("TC-MON-UT-014: UNAVAILABLE to READY recovery transition", func() {
		instanceID := "inst-014"
		hc := buildUnstructuredHostedCluster("cluster-14", instanceID, unavailableConditions())
		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for UNAVAILABLE event.
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).Should(HaveLen(1))
		Expect(publisher.Events()[0].Status).To(Equal(v1alpha1.ClusterStatusUNAVAILABLE))

		// Update to READY.
		updated := buildUnstructuredHostedCluster("cluster-14", instanceID, readyConditions())
		_, err := client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Should publish READY event.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(2))

		Expect(publisher.Events()[1].Status).To(Equal(v1alpha1.ClusterStatusREADY))
	})

	// ── TC-MON-UT-015: UNAVAILABLE CloudEvent message includes context ──

	It("TC-MON-UT-015: UNAVAILABLE CloudEvent message includes context", func() {
		instanceID := "inst-015"
		unavailableMsg := "Kube API server is not ready"
		hc := buildUnstructuredHostedCluster("cluster-15", instanceID,
			unavailableConditionsWithMessage(unavailableMsg))
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
		Expect(event.Status).To(Equal(v1alpha1.ClusterStatusUNAVAILABLE))
		Expect(event.Message).To(ContainSubstring(unavailableMsg))
	})

	// ── TC-MON-UT-016: Missing dcm-instance-id label is handled gracefully ──

	It("TC-MON-UT-016: Missing dcm-instance-id label is handled gracefully", func() {
		// Resource has managed-by and service-type labels but NOT dcm-instance-id.
		hc := buildUnstructuredHostedCluster("cluster-16", "temp-id", readyConditions(), withoutInstanceIDLabel())

		// For the label selector to match, we need managed-by and service-type.
		labels := hc.GetLabels()
		labels[cluster.LabelManagedBy] = cluster.ValueManagedBy
		labels[cluster.LabelServiceType] = cluster.ValueServiceType
		hc.SetLabels(labels)

		client := newDynamicFakeClient(hc)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// No event should be published for resource without instance-id.
		Consistently(func() int {
			return len(publisher.Events())
		}, 2*time.Second, 200*time.Millisecond).Should(Equal(0))
	})

	// ── TC-MON-UT-017: Duplicate dcm-instance-id in monitoring path ──

	It("TC-MON-UT-017: Duplicate dcm-instance-id in monitoring path", func() {
		duplicateID := "inst-017-dup"
		hc1 := buildUnstructuredHostedCluster("cluster-17a", duplicateID, readyConditions())
		hc2 := buildUnstructuredHostedCluster("cluster-17b", duplicateID, provisioningConditions())
		client := newDynamicFakeClient(hc1, hc2)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Debouncer key is now instanceID. Two HCs with same instanceID
		// submit to the same debouncer key; within the debounce window,
		// the second HC's event replaces the first's timer. Only 1 fires.
		Eventually(func() int {
			return len(publisher.Events())
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(1))

		Expect(publisher.Events()[0].InstanceID).To(Equal(duplicateID))
	})
})
