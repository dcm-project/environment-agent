package monitoring_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/network/monitoring"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Resilience", func() {
		It("should retry publishing with exponential backoff on transient failure", func() {
			failPublisher := &retryTrackingPublisher{}
			client := fake.NewClientset()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			cfg := defaultMonitorConfig()
			cfg.DebounceMs = 50

			monitor := monitoring.NewStatusMonitor(client, cfg, failPublisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().Services("default").Create(ctx,
				testService("retry-svc", "retry-test"), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int32 {
				return failPublisher.attempts.Load()
			}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))
		})

		It("should continue operating when NATS publish fails", func() {
			logBuf := &safeBuffer{}
			failPublisher := &retryTrackingPublisher{failAlways: true}
			client := fake.NewClientset()
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			cfg := defaultMonitorConfig()
			cfg.DebounceMs = 50
			cfg.PublishMaxAttempts = 3

			monitor := monitoring.NewStatusMonitor(client, cfg, failPublisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err := client.CoreV1().Services("default").Create(ctx,
				testService("nats-down", "nats-down"), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				return logBuf.String()
			}, 2*time.Second, 50*time.Millisecond).Should(ContainSubstring("error"))
		})

		It("should resume publishing after NATS reconnects", func() {
			ns := startEmbeddedNATSServer()
			DeferCleanup(ns.Shutdown)

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			publisher, err := monitoring.NewNATSPublisher(ns.ClientURL(), testProviderName, logger)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = publisher.Close() })

			// Subscribe to verify delivery after reconnect
			nc, err := nats.Connect(ns.ClientURL())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(nc.Close)

			received := make(chan []byte, 10)
			_, err = nc.Subscribe("dcm.network", func(msg *nats.Msg) {
				received <- msg.Data
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(nc.Flush()).To(Succeed())

			// Verify initial publish works
			err = publisher.Publish(context.Background(), monitoring.StatusEvent{
				InstanceID: "reconnect-1",
				Status:     v1alpha1.READY,
				Message:    "before disconnect",
			})
			Expect(err).NotTo(HaveOccurred())
			Eventually(received, 2*time.Second).Should(Receive())

			// Shut down NATS — publishes may be buffered by the reconnect buffer
			natsURL := ns.ClientURL()
			ns.Shutdown()

			// Restart NATS on the same URL
			ns2 := startEmbeddedNATSServerOnURL(natsURL)
			DeferCleanup(ns2.Shutdown)

			// Re-subscribe on new server
			nc2, err := nats.Connect(natsURL)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(nc2.Close)

			received2 := make(chan []byte, 10)
			_, err = nc2.Subscribe("dcm.network", func(msg *nats.Msg) {
				received2 <- msg.Data
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(nc2.Flush()).To(Succeed())

			// Wait for publisher to reconnect, then verify publish resumes
			Eventually(func() error {
				return publisher.Publish(context.Background(), monitoring.StatusEvent{
					InstanceID: "reconnect-3",
					Status:     v1alpha1.READY,
					Message:    "after reconnect",
				})
			}, 5*time.Second, 200*time.Millisecond).Should(Succeed())

			var payload []byte
			Eventually(received2, 5*time.Second).Should(Receive(&payload))

			var ce map[string]any
			Expect(json.Unmarshal(payload, &ce)).To(Succeed())
			ceData, ok := ce["data"].(map[string]any)
			Expect(ok).To(BeTrue())
			Expect(ceData).To(HaveKeyWithValue("id", "reconnect-3"))
		})

		It("should drain in-flight messages on shutdown within bounded time", func() {
			ns := startEmbeddedNATSServer()
			DeferCleanup(ns.Shutdown)

			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			publisher, err := monitoring.NewNATSPublisher(ns.ClientURL(), testProviderName, logger)
			Expect(err).NotTo(HaveOccurred())

			// Subscribe to verify messages are drained
			nc, err := nats.Connect(ns.ClientURL())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(nc.Close)

			received := make(chan []byte, 10)
			_, err = nc.Subscribe("dcm.network", func(msg *nats.Msg) {
				received <- msg.Data
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(nc.Flush()).To(Succeed())

			// Publish a message, then immediately close
			err = publisher.Publish(context.Background(), monitoring.StatusEvent{
				InstanceID: "drain-test",
				Status:     v1alpha1.PENDING,
				Message:    "should be drained",
			})
			Expect(err).NotTo(HaveOccurred())

			// Close should complete within a bounded time and drain the message
			done := make(chan error, 1)
			go func() {
				done <- publisher.Close()
			}()

			Eventually(done, 5*time.Second).Should(Receive(Not(HaveOccurred())))

			// The message should have been delivered before close returned
			Eventually(received, 2*time.Second).Should(Receive())
		})

		It("logs publish errors when the real NATS connection is unavailable", func() {
			ns := startEmbeddedNATSServer()
			DeferCleanup(ns.Shutdown)

			logBuf := &safeBuffer{}
			logger := slog.New(slog.NewJSONHandler(logBuf, nil))
			publisher, err := monitoring.NewNATSPublisher(ns.ClientURL(), testProviderName, logger)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = publisher.Close() })
			ns.Shutdown()

			client := fake.NewClientset()
			cfg := defaultMonitorConfig()
			cfg.DebounceMs = 50
			cfg.PublishMaxAttempts = 3

			monitor := monitoring.NewStatusMonitor(client, cfg, publisher, logger)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer GinkgoRecover()
				_ = monitor.Start(ctx)
			}()

			_, err = client.CoreV1().Services("default").Create(ctx,
				testService("nats-real-down", "nats-real-down"), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				return logBuf.String()
			}, 2*time.Second, 50*time.Millisecond).Should(ContainSubstring("error"))
		})
	})
})

func startEmbeddedNATSServerOnURL(natsURL string) *server.Server {
	// Parse host:port from nats://host:port
	hostPort := natsURL[len("nats://"):]
	var host string
	var port int
	for i := len(hostPort) - 1; i >= 0; i-- {
		if hostPort[i] == ':' {
			host = hostPort[:i]
			_, err := fmt.Sscanf(hostPort[i+1:], "%d", &port)
			Expect(err).NotTo(HaveOccurred())
			break
		}
	}
	opts := &server.Options{
		Host:   host,
		Port:   port,
		NoLog:  true,
		NoSigs: true,
	}
	ns, err := server.NewServer(opts)
	Expect(err).NotTo(HaveOccurred())
	go ns.Start()
	Expect(ns.ReadyForConnections(5 * time.Second)).To(BeTrue())
	return ns
}

func startEmbeddedNATSServer() *server.Server {
	opts := &server.Options{
		Host:   "127.0.0.1",
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}
	ns, err := server.NewServer(opts)
	Expect(err).NotTo(HaveOccurred())
	go ns.Start()
	Expect(ns.ReadyForConnections(5 * time.Second)).To(BeTrue())
	return ns
}

func testService(name, instanceID string) *corev1.Service {
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
