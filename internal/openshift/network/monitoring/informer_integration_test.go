package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/network/monitoring"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func testLoadBalancerService(name, instanceID string, hasExternalIP bool) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    dcm.Labels(instanceID),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Ports: []corev1.ServicePort{
				{Port: 80, Protocol: corev1.ProtocolTCP},
			},
		},
	}
	if hasExternalIP {
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
			{IP: "203.0.113.42"},
		}
	}
	return svc
}

func testClusterIPService(name, instanceID string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    dcm.Labels(instanceID),
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "10.96.0.100",
			Ports: []corev1.ServicePort{
				{Port: 80, Protocol: corev1.ProtocolTCP},
			},
		},
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

var _ = Describe("Network Status Monitor", func() {
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

		It("should publish PENDING when LoadBalancer Service is created without external IP", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().Services("default").Create(ctx,
				testLoadBalancerService("test-lb", "lb-123", false), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() []monitoring.StatusEvent {
				return publisher.Events()
			}, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			events := publisher.Events()
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.PENDING))
			Expect(events[len(events)-1].InstanceID).To(Equal("lb-123"))
			Expect(events[len(events)-1].Message).To(ContainSubstring("waiting for external IP"))
		})

		It("should publish READY when LoadBalancer gets external IP assigned", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			svc := testLoadBalancerService("ready-lb", "ready-123", false)
			_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() []monitoring.StatusEvent {
				return publisher.Events()
			}, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			// Update to add external IP
			svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.10"}}
			_, err = client.CoreV1().Services("default").Update(ctx, svc, metav1.UpdateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() monitoring.StatusEvent {
				events := publisher.Events()
				if len(events) == 0 {
					return monitoring.StatusEvent{}
				}
				return events[len(events)-1]
			}, 2*time.Second, 50*time.Millisecond).Should(And(
				HaveField("Status", v1alpha1.READY),
				HaveField("InstanceID", "ready-123"),
			))
		})

		It("should publish READY immediately for ClusterIP services", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().Services("default").Create(ctx,
				testClusterIPService("cluster-svc", "cluster-456"), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() []monitoring.StatusEvent {
				return publisher.Events()
			}, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			events := publisher.Events()
			Expect(events[len(events)-1].Status).To(Equal(v1alpha1.READY))
			Expect(events[len(events)-1].InstanceID).To(Equal("cluster-456"))
		})

		It("should publish DELETED when Service is removed", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			svc := testLoadBalancerService("delete-lb", "delete-789", false)
			_, err := client.CoreV1().Services("default").Create(ctx, svc, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() []monitoring.StatusEvent {
				return publisher.Events()
			}, 2*time.Second, 50*time.Millisecond).ShouldNot(BeEmpty())

			err = client.CoreV1().Services("default").Delete(ctx, svc.Name, metav1.DeleteOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() monitoring.StatusEvent {
				events := publisher.Events()
				if len(events) == 0 {
					return monitoring.StatusEvent{}
				}
				return events[len(events)-1]
			}, 2*time.Second, 50*time.Millisecond).Should(And(
				HaveField("Status", v1alpha1.DELETED),
				HaveField("InstanceID", "delete-789"),
			))
		})
	})
})
