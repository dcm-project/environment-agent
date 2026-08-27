package monitoring_test

import (
	"context"
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
	Describe("Lifecycle", func() {
		var (
			client    *fake.Clientset
			publisher *mockStatusPublisher
			logger    *slog.Logger
			cfg       monitoring.MonitorConfig
		)

		BeforeEach(func() {
			client = fake.NewClientset()
			publisher = newMockPublisher()
			logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg = defaultMonitorConfig()
		})

		It("should not process events until Start is called (TC-I049)", func() {
			monitor := monitoring.NewStatusMonitor(client, cfg, publisher, logger)

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(),
				testPVC("pre-start", "abc-123", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Consistently(publisher.Events, 300*time.Millisecond, 50*time.Millisecond).Should(BeEmpty())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())
		})

		It("should stop watchers when context is cancelled (TC-I050)", func() {
			monitor := monitoring.NewStatusMonitor(client, cfg, publisher, logger)
			ctx, cancel := context.WithCancel(context.Background())

			errCh := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				errCh <- monitor.Start(ctx)
			}()

			// Seed a PVC so Start completes cache sync and publishes once.
			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(),
				testPVC("seed", "seed-id", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			cancel()
			Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))

			eventsBefore := len(publisher.Events())
			_, err = client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(),
				testPVC("post-stop", "post-stop", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Consistently(func() int {
				return len(publisher.Events())
			}, 400*time.Millisecond, 50*time.Millisecond).Should(Equal(eventsBefore))
		})

		It("should flush debounced status on shutdown when monitor ctx is cancelled", func() {
			cfg.DebounceMs = 10_000
			cfg.ShutdownPublishTimeout = 50 * time.Millisecond
			publisher := newCancellingMockPublisher()
			monitor := monitoring.NewStatusMonitor(client, cfg, publisher, logger)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				errCh <- monitor.Start(ctx)
			}()

			// Allow initial cache sync on an empty namespace before adding a PVC.
			time.Sleep(500 * time.Millisecond)

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(),
				testPVC("flush-on-stop", "flush-id", corev1.ClaimBound), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Informer should queue a debounced RUNNING event; long debounce prevents publish.
			Consistently(func() int {
				return len(publisher.Events())
			}, 300*time.Millisecond, 50*time.Millisecond).Should(Equal(0))

			// Wait past ShutdownPublishTimeout so a boot-time flush context would already
			// be expired; flush must still publish using a shutdown-scoped context.
			time.Sleep(100 * time.Millisecond)

			cancel()
			Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).Should(HaveLen(1))
			Expect(publisher.Events()[0].InstanceID).To(Equal("flush-id"))
			Expect(publisher.Events()[0].Status).To(Equal(v1alpha1.RUNNING))
		})

		It("should re-evaluate resources on cache resync without republishing unchanged status (TC-I051)", func() {
			// client-go SharedInformer minimum resync is 1s; values below are raised with a warning.
			cfg.ResyncPeriod = time.Second
			monitor := monitoring.NewStatusMonitor(client, cfg, publisher, logger)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("resync-pvc", "resync-test", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			Eventually(publisher.Events, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())
			// Wait across more than one resync period; identical PROVISIONING must not flood.
			Consistently(func() int {
				return len(publisher.Events())
			}, 2500*time.Millisecond, 100*time.Millisecond).Should(Equal(1))

			pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "resync-pvc", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			pvc.Status.Phase = corev1.ClaimBound
			pvc.Spec.VolumeName = "pv-resync"
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
			Expect(publisher.Events()).To(HaveLen(2))
		})

		It("should publish events for all existing PVCs on initial sync (TC-I052)", func() {
			for _, id := range []string{"inst-1", "inst-2", "inst-3"} {
				_, err := client.CoreV1().PersistentVolumeClaims("default").Create(context.Background(),
					testPVC("pvc-"+id, id, corev1.ClaimPending), metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
			}

			monitor := monitoring.NewStatusMonitor(client, cfg, publisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			Eventually(func() map[string]bool {
				ids := make(map[string]bool)
				for _, e := range publisher.Events() {
					ids[e.InstanceID] = true
				}
				return ids
			}, 2*time.Second, 50*time.Millisecond).Should(And(
				HaveKey("inst-1"),
				HaveKey("inst-2"),
				HaveKey("inst-3"),
			))
		})
	})
})
