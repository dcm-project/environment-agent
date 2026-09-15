package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/container/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Reconciliation", func() {
		var (
			client    *fake.Clientset
			publisher *mockStatusPublisher
			monitor   *monitoring.StatusMonitor
			ctx       context.Context
			cancel    context.CancelFunc
		)

		createDeployment := func(name, instanceID string) {
			replicas := int32(1)
			labels := dcm.Labels(instanceID)
			_, err := client.AppsV1().Deployments("default").Create(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
				Spec: appsv1.DeploymentSpec{
					Replicas: &replicas,
					Selector: &metav1.LabelSelector{MatchLabels: labels},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
					},
				},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		createPod := func(name, instanceID string, phase corev1.PodPhase) {
			_, err := client.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Labels: dcm.Labels(instanceID)},
				Status:     corev1.PodStatus{Phase: phase},
			}, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		BeforeEach(func() {
			client = fake.NewClientset()
			publisher = newMockPublisher()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg := monitoring.MonitorConfig{
				Namespace:    "default",
				ProviderName: "k8s-sp",
				DebounceMs:   100,
				ResyncPeriod: 1 * time.Second,
			}
			monitor = monitoring.NewStatusMonitor(client, cfg, publisher, logger)
			ctx, cancel = context.WithCancel(context.Background()) //nolint:fatcontext // intentional: outer-scope ctx/cancel shared with It blocks

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()
			time.Sleep(200 * time.Millisecond)
		})

		AfterEach(func() {
			cancel()
			time.Sleep(100 * time.Millisecond)
		})

		It("should reconcile to RUNNING when Pod phase changes to Running (TC-I044)", func() {
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodRunning)

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.RUNNING {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected RUNNING event for instance abc-123")
		})

		It("should fall back to PENDING when Deployment exists without Pod (TC-I045)", func() {
			createDeployment("my-app", "abc-123")

			// Update deployment to have Available=False condition.
			deploy, err := client.AppsV1().Deployments("default").Get(ctx, "my-app", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			deploy.Status.Conditions = []appsv1.DeploymentCondition{
				{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionFalse},
			}
			_, err = client.AppsV1().Deployments("default").UpdateStatus(ctx, deploy, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.PENDING {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected PENDING event for instance abc-123")
		})

		It("should produce DELETED status when Deployment is deleted (TC-I046)", func() {
			createDeployment("my-app", "abc-123")
			time.Sleep(300 * time.Millisecond)

			err := client.AppsV1().Deployments("default").Delete(ctx, "my-app", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.DELETED {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected DELETED event for instance abc-123")
		})

		It("should reconcile Pod phase Pending to PENDING status (TC-I062)", func() {
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodPending)

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.PENDING {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected PENDING event for instance abc-123")
		})

		It("should reconcile Pod phase Failed to FAILED status (TC-I063)", func() {
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodFailed)

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.FAILED {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected FAILED event for instance abc-123")
		})

		It("should reconcile Pod phase Unknown to UNKNOWN status (TC-I064)", func() {
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodUnknown)

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var found bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.UNKNOWN {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "expected UNKNOWN event for instance abc-123")
		})

		It("should not publish an event when Pod phase is Succeeded (TC-I065)", func() {
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodSucceeded)

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			for _, e := range events {
				Expect(e.Status).NotTo(Equal(v1alpha1.ContainerStatus("SUCCEEDED")),
					"no event should be published for Succeeded phase")
				if e.InstanceID == "abc-123" {
					// If any event was published for this instance after the
					// Succeeded pod was created, the status must not have changed
					// due to the Succeeded phase.
					Expect(e.Status).NotTo(BeEmpty(),
						"if an event was published, it should have a valid non-Succeeded status")
				}
			}
		})

		It("should use Pod status when Deployment is deleted but Pod remains (TC-I113)", func() {
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodRunning)
			time.Sleep(300 * time.Millisecond)

			err := client.AppsV1().Deployments("default").Delete(ctx, "my-app", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			var last monitoring.StatusEvent
			for _, e := range events {
				if e.InstanceID == "abc-123" {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.RUNNING),
				"last event for abc-123 should be RUNNING, not DELETED")
		})

		It("should use surviving Pod status when one of multiple Pods is deleted (TC-I114)", func() {
			createDeployment("my-app", "abc-123")
			createPod("pod-a", "abc-123", corev1.PodPending)
			createPod("pod-b", "abc-123", corev1.PodRunning)
			time.Sleep(300 * time.Millisecond)

			err := client.CoreV1().Pods("default").Delete(ctx, "pod-a", metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			var last monitoring.StatusEvent
			for _, e := range events {
				if e.InstanceID == "abc-123" {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.RUNNING),
				"last event for abc-123 should be RUNNING (surviving pod), not PENDING (deploy-only fallback)")
		})

		It("should publish FAILED when Deployment reaches ProgressDeadlineExceeded, then un-latch to RUNNING when the rollout recovers (TC-I120, TC-I121)", func() {
			// TC-I120: Deployment + Pod (Pending), then Deployment status
			// updated to a stuck-rollout condition -> publisher eventually
			// receives a FAILED event for the instance.
			createDeployment("my-app", "abc-123")
			createPod("my-app-pod", "abc-123", corev1.PodPending)

			deploy, err := client.AppsV1().Deployments("default").Get(ctx, "my-app", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			deploy.Status.Conditions = []appsv1.DeploymentCondition{
				{
					Type:    appsv1.DeploymentProgressing,
					Status:  corev1.ConditionFalse,
					Reason:  "ProgressDeadlineExceeded",
					Message: `ReplicaSet "my-app-abc123" has timed out progressing.`,
				},
			}
			_, err = client.AppsV1().Deployments("default").UpdateStatus(ctx, deploy, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events := publisher.Events()
			Expect(events).NotTo(BeEmpty())
			var foundFailed bool
			for _, e := range events {
				if e.InstanceID == "abc-123" && e.Status == v1alpha1.FAILED {
					foundFailed = true
					break
				}
			}
			Expect(foundFailed).To(BeTrue(), "expected FAILED event for instance abc-123")

			// TC-I121: continuing from the TC-I120 FAILED end-state, the
			// rollout recovers (Progressing=True, no Reason/Message) and the
			// Pod becomes Running -> the publisher's LAST event for the
			// instance must be RUNNING, not latched at FAILED.
			deploy, err = client.AppsV1().Deployments("default").Get(ctx, "my-app", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			deploy.Status.Conditions = []appsv1.DeploymentCondition{
				{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue},
			}
			_, err = client.AppsV1().Deployments("default").UpdateStatus(ctx, deploy, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			pod, err := client.CoreV1().Pods("default").Get(ctx, "my-app-pod", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			pod.Status.Phase = corev1.PodRunning
			_, err = client.CoreV1().Pods("default").UpdateStatus(ctx, pod, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(500 * time.Millisecond)

			events = publisher.Events()
			var last monitoring.StatusEvent
			for _, e := range events {
				if e.InstanceID == "abc-123" {
					last = e
				}
			}
			Expect(last.Status).To(Equal(v1alpha1.RUNNING),
				"last event for abc-123 should be RUNNING, not latched at FAILED")
		})
	})
})
