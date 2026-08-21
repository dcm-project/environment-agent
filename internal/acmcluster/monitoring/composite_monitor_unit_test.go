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

var _ = Describe("Status Monitor — Composite Status Tests", func() {
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

	// ── TC-MON-UT-020: NP Ready=False + HC READY → UNAVAILABLE composite ──

	It("TC-MON-UT-020: NP Ready=False + HC READY → UNAVAILABLE composite", func() {
		instanceID := "inst-020"
		hc := buildUnstructuredHostedCluster("cluster-20", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-20", instanceID, npNotReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		Eventually(func() v1alpha1.ClusterStatus {
			return lastEventForInstance(publisher.Events(), instanceID).Status
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(v1alpha1.ClusterStatusUNAVAILABLE))
	})

	// ── TC-MON-UT-021: NP UpdatingVersion + HC READY → PROVISIONING composite ──

	It("TC-MON-UT-021: NP UpdatingVersion + HC READY → PROVISIONING composite", func() {
		instanceID := "inst-021"
		hc := buildUnstructuredHostedCluster("cluster-21", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-21", instanceID, npUpdatingVersionConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		Eventually(func() v1alpha1.ClusterStatus {
			return lastEventForInstance(publisher.Events(), instanceID).Status
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(v1alpha1.ClusterStatusPROVISIONING))
	})

	// ── TC-MON-UT-022: NP + HC both READY → READY composite ──

	It("TC-MON-UT-022: NP + HC both READY → READY composite", func() {
		instanceID := "inst-022"
		hc := buildUnstructuredHostedCluster("cluster-22", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-22", instanceID, npReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		Eventually(func() v1alpha1.ClusterStatus {
			return lastEventForInstance(publisher.Events(), instanceID).Status
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(v1alpha1.ClusterStatusREADY))
	})

	// ── TC-MON-UT-023: NP deletion with HC present → NOT DELETED ──

	It("TC-MON-UT-023: NP deletion with HC present → NOT DELETED", func() {
		instanceID := "inst-023"
		hc := buildUnstructuredHostedCluster("cluster-23", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-23", instanceID, npReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial composite event (READY).
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

		publisher.Reset()

		// Delete the NodePool.
		err := client.Resource(util.NodePoolGVR).Namespace(testNamespace).Delete(ctx, "cluster-23", metav1.DeleteOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Wait for any potential event from the NP deletion.
		// DELETED should NOT be published; composite should revert to HC-only (READY).
		Consistently(func() bool {
			for _, e := range publisher.Events() {
				if e.InstanceID == instanceID && e.Status == v1alpha1.ClusterStatusDELETED {
					return true
				}
			}
			return false
		}, 2*time.Second, 200*time.Millisecond).Should(BeFalse())
	})

	// ── TC-MON-UT-024: NP no conditions → HC status prevails ──

	It("TC-MON-UT-024: NP no conditions → HC status prevails", func() {
		instanceID := "inst-024"
		hc := buildUnstructuredHostedCluster("cluster-24", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-24", instanceID, nil) // no conditions
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// NP excluded from composite (no conditions) → HC status READY prevails.
		Eventually(func() v1alpha1.ClusterStatus {
			return lastEventForInstance(publisher.Events(), instanceID).Status
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(v1alpha1.ClusterStatusREADY))
	})

	// ── TC-MON-UT-025: HC FAILED + NP READY → FAILED composite ──

	It("TC-MON-UT-025: HC FAILED + NP READY → FAILED composite", func() {
		instanceID := "inst-025"
		hc := buildUnstructuredHostedCluster("cluster-25", instanceID, failedConditions())
		np := buildUnstructuredNodePool("cluster-25", instanceID, npReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		Eventually(func() v1alpha1.ClusterStatus {
			return lastEventForInstance(publisher.Events(), instanceID).Status
		}, 5*time.Second, 100*time.Millisecond).Should(Equal(v1alpha1.ClusterStatusFAILED))
	})

	// ── TC-MON-UT-026: UNAVAILABLE from NP → message has "NodePool:" prefix ──

	It("TC-MON-UT-026: UNAVAILABLE from NP → message has NodePool prefix", func() {
		instanceID := "inst-026"
		hc := buildUnstructuredHostedCluster("cluster-26", instanceID, readyConditions())
		np := buildUnstructuredNodePool("cluster-26", instanceID, npNotReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		Eventually(func() monitoring.StatusEvent {
			return lastEventForInstance(publisher.Events(), instanceID)
		}, 5*time.Second, 100*time.Millisecond).Should(And(
			HaveField("Status", Equal(v1alpha1.ClusterStatusUNAVAILABLE)),
			HaveField("Message", HavePrefix("NodePool: ")),
		))
	})

	// ── TC-MON-UT-027: NP change without composite change → no publish ──

	It("TC-MON-UT-027: NP change without composite change → no publish", func() {
		instanceID := "inst-027"
		hc := buildUnstructuredHostedCluster("cluster-27", instanceID, failedConditions())
		np := buildUnstructuredNodePool("cluster-27", instanceID, npNotReadyConditions())
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial composite event (FAILED — HC dominates).
		Eventually(func() []monitoring.StatusEvent {
			return publisher.Events()
		}, 5*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

		initialCount := len(publisher.Events())

		// Update NP to READY — composite should still be FAILED (HC dominates).
		updatedNP := buildUnstructuredNodePool("cluster-27", instanceID, npReadyConditions())
		_, err := client.Resource(util.NodePoolGVR).Namespace(testNamespace).Update(ctx, updatedNP, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// No new event should be published (composite unchanged).
		Consistently(func() int {
			return len(publisher.Events())
		}, 2*time.Second, 200*time.Millisecond).Should(Equal(initialCount))
	})

	// ── TC-MON-UT-028: NP startup re-lists alongside HCs ──

	It("TC-MON-UT-028: NP startup re-lists alongside HCs", func() {
		hc1 := buildUnstructuredHostedCluster("cluster-28a", "inst-028a", readyConditions())
		np1 := buildUnstructuredNodePool("cluster-28a", "inst-028a", npReadyConditions())
		hc2 := buildUnstructuredHostedCluster("cluster-28b", "inst-028b", readyConditions())
		np2 := buildUnstructuredNodePool("cluster-28b", "inst-028b", npNotReadyConditions())
		client := newDynamicFakeClient(hc1, np1, hc2, np2)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for events from both instances.
		Eventually(func() map[string]v1alpha1.ClusterStatus {
			ids := make(map[string]v1alpha1.ClusterStatus)
			for _, e := range publisher.Events() {
				ids[e.InstanceID] = e.Status
			}
			return ids
		}, 5*time.Second, 100*time.Millisecond).Should(And(
			// inst-028a: HC READY + NP READY → READY
			HaveKeyWithValue("inst-028a", v1alpha1.ClusterStatusREADY),
			// inst-028b: HC READY + NP not-ready → UNAVAILABLE
			HaveKeyWithValue("inst-028b", v1alpha1.ClusterStatusUNAVAILABLE),
		))
	})

	// ── TC-MON-UT-029: NP message update without status change → fresh message on next publish ──

	It("TC-MON-UT-029: NP message update without status change → fresh message on next publish", func() {
		instanceID := "inst-029"
		hc := buildUnstructuredHostedCluster("cluster-29", instanceID, failedConditions())
		np := buildUnstructuredNodePool("cluster-29", instanceID, npNotReadyConditionsWithMessage("0 of 3 machines are ready"))
		client := newDynamicFakeClient(hc, np)
		cfg := defaultMonitorConfig()
		monitor := monitoring.New(client, cfg, publisher, slog.Default())

		go func() {
			defer GinkgoRecover()
			_ = monitor.Start(ctx)
		}()

		// Wait for initial composite FAILED (HC dominates).
		Eventually(func() bool {
			for _, e := range publisher.Events() {
				if e.InstanceID == instanceID && e.Status == v1alpha1.ClusterStatusFAILED {
					return true
				}
			}
			return false
		}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())

		publisher.Reset()

		// Update NP: still UNAVAILABLE but message changes.
		updatedNP := buildUnstructuredNodePool("cluster-29", instanceID, npNotReadyConditionsWithMessage("1 of 3 machines are ready"))
		_, err := client.Resource(util.NodePoolGVR).Namespace(testNamespace).Update(ctx, updatedNP, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// No event expected (composite still FAILED, HC dominates).
		Consistently(func() int {
			return len(publisher.Events())
		}, 500*time.Millisecond, 100*time.Millisecond).Should(Equal(0))

		// Recover HC to READY → composite becomes UNAVAILABLE (NP dominates).
		updatedHC := buildUnstructuredHostedCluster("cluster-29", instanceID, readyConditions())
		_, err = client.Resource(util.HostedClusterGVR).Namespace(testNamespace).Update(ctx, updatedHC, metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// The published message should use the UPDATED NP message, not the stale one.
		Eventually(func() string {
			for _, e := range publisher.Events() {
				if e.InstanceID == instanceID && e.Status == v1alpha1.ClusterStatusUNAVAILABLE {
					return e.Message
				}
			}
			return ""
		}, 5*time.Second, 100*time.Millisecond).Should(Equal("NodePool: 1 of 3 machines are ready"))
	})
})
