package monitoring_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Warning Events", func() {
		var (
			client    *fake.Clientset
			publisher *mockStatusPublisher
			monitor   *monitoring.StatusMonitor
			logger    *slog.Logger
			cfg       monitoring.MonitorConfig
		)

		BeforeEach(func() {
			client = fake.NewClientset()
			publisher = newMockPublisher()
			logger = slog.New(slog.NewTextHandler(io.Discard, nil))
			cfg = defaultMonitorConfig()
			cfg.DebounceMs = 50
			monitor = monitoring.NewStatusMonitor(client, cfg, publisher, logger)
		})

		startMonitor := func(ctx context.Context) {
			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
		}

		waitProvisioning := func(instanceID string) {
			Eventually(func() bool {
				for _, e := range publisher.Events() {
					if e.InstanceID == instanceID && e.Status == v1alpha1.PROVISIONING {
						return true
					}
				}
				return false
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())
		}

		createPVCWarning := func(ctx context.Context, eventName, pvcName, eventType, reason, message string) {
			_, err := client.CoreV1().Events("default").Create(ctx, &corev1.Event{
				ObjectMeta: metav1.ObjectMeta{
					Name:      eventName,
					Namespace: "default",
				},
				InvolvedObject: corev1.ObjectReference{
					Kind:      "PersistentVolumeClaim",
					Namespace: "default",
					Name:      pvcName,
				},
				Type:    eventType,
				Reason:  reason,
				Message: message,
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		DescribeTable("publishes FAILED for terminal Warning reasons",
			func(reason string) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				startMonitor(ctx)

				pvcName := fmt.Sprintf("fail-%s", reason)
				instanceID := fmt.Sprintf("id-%s", reason)
				_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
					testPVC(pvcName, instanceID, corev1.ClaimPending), metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				waitProvisioning(instanceID)

				createPVCWarning(ctx, pvcName+".term", pvcName, corev1.EventTypeWarning, reason,
					fmt.Sprintf("failure detail for %s", reason))

				Eventually(func() bool {
					for _, e := range publisher.Events() {
						if e.InstanceID == instanceID && e.Status == v1alpha1.FAILED {
							Expect(e.Message).To(ContainSubstring(reason))
							Expect(e.Message).To(ContainSubstring("failure detail"))
							return true
						}
					}
					return false
				}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())
			},
			Entry("FailedBinding", "FailedBinding"),
			Entry("ProvisioningFailed", "ProvisioningFailed"),
			Entry("FailedProvisioning", "FailedProvisioning"),
			Entry("AllocationFailed", "AllocationFailed"),
		)

		It("ignores Warning Events for non-DCM PVCs", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "default"},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					},
					Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
				}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			createPVCWarning(ctx, "foreign.fail", "foreign", corev1.EventTypeWarning, "ProvisioningFailed", "")

			Consistently(publisher.Events, 400*time.Millisecond, 50*time.Millisecond).Should(BeEmpty())
		})

		It("ignores Normal events even with a terminal reason", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("normal-pvc", "normal-123", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			waitProvisioning("normal-123")

			eventsBefore := len(publisher.Events())
			createPVCWarning(ctx, "normal-pvc.evt", "normal-pvc", corev1.EventTypeNormal, "ProvisioningFailed", "should be ignored")

			Consistently(func() int {
				return len(publisher.Events())
			}, 400*time.Millisecond, 50*time.Millisecond).Should(Equal(eventsBefore))

			for _, e := range publisher.Events() {
				Expect(e.Status).NotTo(Equal(v1alpha1.FAILED))
			}
		})

		It("ignores Warning Events for non-PVC involved objects", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("pod-evt-pvc", "pod-evt-123", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			waitProvisioning("pod-evt-123")

			eventsBefore := len(publisher.Events())
			_, err = client.CoreV1().Events("default").Create(ctx, &corev1.Event{
				ObjectMeta: metav1.ObjectMeta{Name: "pod.fail", Namespace: "default"},
				InvolvedObject: corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: "default",
					Name:      "some-pod",
				},
				Type:   corev1.EventTypeWarning,
				Reason: "ProvisioningFailed",
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Consistently(func() int {
				return len(publisher.Events())
			}, 400*time.Millisecond, 50*time.Millisecond).Should(Equal(eventsBefore))
		})

		It("ignores unknown Warning reasons (stays PROVISIONING)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("backoff-pvc", "backoff-123", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			waitProvisioning("backoff-123")

			eventsBefore := len(publisher.Events())
			createPVCWarning(ctx, "backoff-pvc.evt", "backoff-pvc", corev1.EventTypeWarning, "BackOff", "retrying")

			Consistently(func() int {
				return len(publisher.Events())
			}, 400*time.Millisecond, 50*time.Millisecond).Should(Equal(eventsBefore))

			for _, e := range publisher.Events() {
				Expect(e.Status).NotTo(Equal(v1alpha1.FAILED))
			}
		})

		It("keeps FAILED when PVC is still Pending after a terminal Warning", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			instanceID := "fail-latch-123"
			pvcName := "fail-latch-pvc"
			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC(pvcName, instanceID, corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			waitProvisioning(instanceID)

			createPVCWarning(ctx, pvcName+".term", pvcName, corev1.EventTypeWarning, "FailedBinding",
				"no matching volume")

			Eventually(func() bool {
				for _, e := range publisher.Events() {
					if e.InstanceID == instanceID && e.Status == v1alpha1.FAILED {
						return true
					}
				}
				return false
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			eventsAfterFailed := len(publisher.Events())

			// PVC informer update while still Pending must not downgrade to PROVISIONING.
			pvc.Annotations = map[string]string{"touch": "1"}
			_, err = client.CoreV1().PersistentVolumeClaims("default").Update(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Consistently(func() int {
				return len(publisher.Events())
			}, 400*time.Millisecond, 50*time.Millisecond).Should(Equal(eventsAfterFailed))

			var last monitoring.StatusEvent
			for _, e := range publisher.Events() {
				if e.InstanceID == instanceID {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.FAILED))
		})

		It("publishes RUNNING when PVC becomes Bound after terminal Warning", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			instanceID := "fail-recover-123"
			pvcName := "fail-recover-pvc"
			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC(pvcName, instanceID, corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			waitProvisioning(instanceID)

			createPVCWarning(ctx, pvcName+".term", pvcName, corev1.EventTypeWarning, "FailedBinding", "no volume")

			Eventually(func() bool {
				for _, e := range publisher.Events() {
					if e.InstanceID == instanceID && e.Status == v1alpha1.FAILED {
						return true
					}
				}
				return false
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			pvc.Status.Phase = corev1.ClaimBound
			_, err = client.CoreV1().PersistentVolumeClaims("default").Update(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				for _, e := range publisher.Events() {
					if e.InstanceID == instanceID && e.Status == v1alpha1.RUNNING {
						return true
					}
				}
				return false
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())
		})

		It("ignores Warning Events when the PVC does not exist", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startMonitor(ctx)

			// Give informers time to sync with an empty cache.
			Consistently(publisher.Events, 200*time.Millisecond, 50*time.Millisecond).Should(BeEmpty())

			createPVCWarning(ctx, "ghost.fail", "missing-pvc", corev1.EventTypeWarning, "ProvisioningFailed", "no such pvc")

			Consistently(publisher.Events, 400*time.Millisecond, 50*time.Millisecond).Should(BeEmpty())
		})
	})
})
