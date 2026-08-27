package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func testPVC(name, instanceID string, phase corev1.PersistentVolumeClaimPhase) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    dcm.Labels(instanceID),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: phase},
	}
}

func defaultMonitorConfig() monitoring.MonitorConfig {
	return monitoring.MonitorConfig{
		Namespace:          "default",
		DebounceMs:         100,
		ResyncPeriod:       time.Hour,
		PublishMaxAttempts: 5,
	}
}

var _ = Describe("Status Monitor", func() {
	Describe("Informer Setup", func() {
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
			logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg = defaultMonitorConfig()
			monitor = monitoring.NewStatusMonitor(client, cfg, publisher, logger)
		})

		It("should detect PVC changes in the configured namespace (TC-I070)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("test-pvc", "abc-123", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() []monitoring.StatusEvent {
				return publisher.Events()
			}, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			events := publisher.Events()
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.PROVISIONING))
			Expect(events[len(events)-1].InstanceID).To(Equal("abc-123"))
		})

		It("should publish RUNNING when PVC becomes Bound (TC-I061)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			pvc := testPVC("bound-pvc", "bound-123", corev1.ClaimPending)
			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			pvc.Status.Phase = corev1.ClaimBound
			pvc.Spec.VolumeName = "pv-xyz"
			pvc.ResourceVersion = "2"
			_, err = client.CoreV1().PersistentVolumeClaims("default").UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() v1alpha1.StorageStatus {
				events := publisher.Events()
				if len(events) == 0 {
					return ""
				}
				return events[len(events)-1].Status
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(v1alpha1.RUNNING))
		})

		It("should only process PVCs with DCM labels (TC-I074)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "non-dcm-pvc",
					Namespace: "default",
					Labels:    map[string]string{"app": "other"},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Consistently(publisher.Events, 400*time.Millisecond, 50*time.Millisecond).Should(BeEmpty())
		})

		It("should publish DELETED when PVC is removed (TC-I062)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("delete-pvc", "del-123", corev1.ClaimBound), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			err = client.CoreV1().PersistentVolumeClaims("default").Delete(ctx, "delete-pvc", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() v1alpha1.StorageStatus {
				events := publisher.Events()
				if len(events) == 0 {
					return ""
				}
				return events[len(events)-1].Status
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(v1alpha1.DELETED))
		})

		It("should publish DELETING when PVC is terminating (TC-U034, AC-K8S-160)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("term-pvc", "term-123", corev1.ClaimBound), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "term-pvc", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			now := metav1.Now()
			pvc.DeletionTimestamp = &now
			_, err = client.CoreV1().PersistentVolumeClaims("default").Update(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				for _, e := range publisher.Events() {
					if e.InstanceID == "term-123" && e.Status == v1alpha1.DELETING {
						return true
					}
				}
				return false
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())
		})

		It("should publish lifecycle statuses in order (TC-I064)", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			const instanceID = "lifecycle-123"
			hasSubsequence := func(want ...v1alpha1.StorageStatus) bool {
				var seq []v1alpha1.StorageStatus
				for _, e := range publisher.Events() {
					if e.InstanceID == instanceID {
						seq = append(seq, e.Status)
					}
				}
				j := 0
				for _, s := range seq {
					if j < len(want) && s == want[j] {
						j++
					}
				}
				return j == len(want)
			}

			pvc := testPVC("lifecycle-pvc", instanceID, corev1.ClaimPending)
			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				return hasSubsequence(v1alpha1.PROVISIONING)
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			pvc, err = client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "lifecycle-pvc", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			pvc.Status.Phase = corev1.ClaimBound
			_, err = client.CoreV1().PersistentVolumeClaims("default").UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				return hasSubsequence(v1alpha1.PROVISIONING, v1alpha1.RUNNING)
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			now := metav1.Now()
			pvc.DeletionTimestamp = &now
			_, err = client.CoreV1().PersistentVolumeClaims("default").Update(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				return hasSubsequence(v1alpha1.PROVISIONING, v1alpha1.RUNNING, v1alpha1.DELETING)
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			err = client.CoreV1().PersistentVolumeClaims("default").Delete(ctx, "lifecycle-pvc", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				return hasSubsequence(
					v1alpha1.PROVISIONING, v1alpha1.RUNNING, v1alpha1.DELETING, v1alpha1.DELETED)
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())
		})

		It("should publish FAILED when PVC phase becomes Lost", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			pvc := testPVC("lost-pvc", "lost-123", corev1.ClaimPending)
			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			pvc.Status.Phase = corev1.ClaimLost
			pvc.ResourceVersion = "2"
			_, err = client.CoreV1().PersistentVolumeClaims("default").UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				for _, e := range publisher.Events() {
					if e.InstanceID == "lost-123" && e.Status == v1alpha1.FAILED {
						Expect(e.Message).To(ContainSubstring("lost"))
						return true
					}
				}
				return false
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())
		})
	})
})
