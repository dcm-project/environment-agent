package monitoring_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	"github.com/nats-io/nats-server/v2/server"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var _ = Describe("Status Monitor", func() {
	Describe("Resilience", func() {
		It("should retry publishing with exponential backoff on transient failure (TC-I100)", func() {
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

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("retry-pvc", "retry-test", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() int32 {
				return failPublisher.attempts.Load()
			}, 2*time.Second, 50*time.Millisecond).Should(BeNumerically(">=", 2))
		})

		It("should continue operating when NATS publish fails (TC-I090)", func() {
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

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("nats-down", "nats-down", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				return logBuf.String()
			}, 2*time.Second, 50*time.Millisecond).Should(ContainSubstring("error"))
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

			_, err = client.CoreV1().PersistentVolumeClaims("default").Create(ctx,
				testPVC("nats-real-down", "nats-real-down", corev1.ClaimPending), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string {
				return logBuf.String()
			}, 2*time.Second, 50*time.Millisecond).Should(ContainSubstring("error"))
		})
	})
})

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

type retryTrackingPublisher struct {
	attempts   atomic.Int32
	failAlways bool
}

func (p *retryTrackingPublisher) Publish(_ context.Context, _ monitoring.StatusEvent) error {
	n := p.attempts.Add(1)
	if p.failAlways || n <= 3 {
		return context.DeadlineExceeded
	}
	return nil
}

func (p *retryTrackingPublisher) Close() error {
	return nil
}

const testProviderName = "storage"

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
