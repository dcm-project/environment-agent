package retry_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/retry"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

// Note: handler-deadline enforcement on the *main-topic* consume path
// (formerly IT-RCM-100/IT-RCM-110) now lives in messaging.Client.handleMainMessage
// (HandlerTimeout wraps the ctx passed to the router), since retry.Processor
// no longer has its own main-topic consume loop (dead code removed — see
// ProcessorConfig doc comment). Those cases are covered in
// internal/messaging/client_integration_test.go. Only the transition-path
// case (IT-RCM-115, via ProcessOnTransition) remains here.
var _ = Describe("Handler Processing Deadline", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		topics    messaging.TopicNames
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("deadline-test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		_ = testJS.DeleteStream(context.Background(), topics.RetryStream())
		testConn.Close()
	})

	It("enforces handler deadline on transition-path forwarding (IT-RCM-115)", func() {
		handlerTimeout := 1 * time.Second

		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		// Retry topic — transition processing reads from here
		_, err := testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topics.RetryStream(), Subjects: []string{topics.Retry},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topics.RetryStream(), jetstream.ConsumerConfig{
			Durable: topics.RetryConsumer(), AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		_, err = testJS.Publish(ctx, topics.Retry, routingtest.BuildCreateCE("res-trans-dl", "database"))
		Expect(err).NotTo(HaveOccurred())

		var ctxCancelled atomic.Bool
		hangingFwd := &hangingFakeSPForwarder{
			onCancel: func() { ctxCancelled.Store(true) },
		}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     hangingFwd,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{HandlerTimeout: handlerTimeout},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			Topics:        topics,
		})
		defer processor.Stop()

		// Transition to Ready triggers forwardRequest with HandlerTimeout
		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())

		Expect(ctxCancelled.Load()).To(BeTrue(), "SP context should have been cancelled by HandlerTimeout")
	})
})

// hangingFakeSPForwarder blocks forever on CreateResource, calling onCancel when context is cancelled.
type hangingFakeSPForwarder struct {
	onCancel func()
}

func (f *hangingFakeSPForwarder) CreateResource(ctx context.Context, _ string, _ bool, _ routing.CreateResourceRequest) error {
	<-ctx.Done()
	if f.onCancel != nil {
		f.onCancel()
	}
	return ctx.Err()
}

func (f *hangingFakeSPForwarder) DeleteResource(ctx context.Context, _ string, _ bool, _ routing.DeleteResourceRequest) error {
	<-ctx.Done()
	return ctx.Err()
}
