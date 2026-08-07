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
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/retry"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

var _ = Describe("Handler Processing Deadline", Label("integration"), func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		testConn    *nats.Conn
		testJS      jetstream.JetStream
		responseSub *nats.Subscription
		topicName   string
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("deadline-test-%s", uuid.New().String()[:8])
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName, Subjects: []string{topicName},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName, jetstream.ConsumerConfig{
			Durable: topicName + "-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: "dcm-responses", Subjects: []string{"dcm.agents.responses"},
		})
		Expect(err).NotTo(HaveOccurred())

		responseSub, err = testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())
		Expect(testConn.Flush()).To(Succeed())
	})

	AfterEach(func() {
		cancel()
		if responseSub != nil {
			_ = responseSub.Unsubscribe()
		}
		_ = testJS.DeleteStream(context.Background(), topicName)
		_ = testJS.DeleteStream(context.Background(), "dcm-responses")
		testConn.Close()
	})

	It("aborts hung SP call after handler deadline elapses (IT-RCM-100)", func() {
		handlerTimeout := 1 * time.Second

		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		// SP that blocks forever — simulates a hung call
		var ctxCancelled atomic.Bool
		hangingForwarder := &hangingFakeSPForwarder{
			onCancel: func() { ctxCancelled.Store(true) },
		}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     hangingForwarder,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{HandlerTimeout: handlerTimeout},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		// Start consuming — the processor wraps handler calls with deadline
		Expect(processor.Start(ctx)).To(Succeed())
		defer processor.Stop()

		_, err := testJS.Publish(ctx, topicName, routingtest.BuildCreateCE("res-hd1", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Wait for the deadline to elapse plus buffer for CI
		Eventually(ctxCancelled.Load, handlerTimeout+2*time.Second).Should(BeTrue())

		// Message must NOT be acked — eligible for redelivery
		cons, err := testJS.Consumer(ctx, topicName, topicName+"-consumer")
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending + uint64(info.NumAckPending)).To(BeNumerically(">", 0))
	})

	It("applies configurable handler deadline to SP call context (IT-RCM-110)", func() {
		handlerTimeout := 2500 * time.Millisecond

		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		// SP that records the context deadline
		var observedDeadline time.Time
		var deadlineSet atomic.Bool
		deadlineForwarder := &deadlineCaptureFakeSPForwarder{
			onCall: func(callCtx context.Context) {
				if dl, ok := callCtx.Deadline(); ok {
					observedDeadline = dl
					deadlineSet.Store(true)
				}
			},
		}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     deadlineForwarder,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{HandlerTimeout: handlerTimeout},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		// Start consuming — the processor wraps handler calls with deadline
		Expect(processor.Start(ctx)).To(Succeed())
		defer processor.Stop()

		before := time.Now()
		_, err := testJS.Publish(ctx, topicName, routingtest.BuildCreateCE("res-hd2", "database"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(deadlineSet.Load, 5*time.Second).Should(BeTrue())

		// Deadline should be approximately handlerTimeout from invocation
		Expect(observedDeadline).To(BeTemporally("~", before.Add(handlerTimeout), 1*time.Second))
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
			Name: topicName + "-retry", Subjects: []string{topicName + ".retry"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName+"-retry", jetstream.ConsumerConfig{
			Durable: topicName + "-retry-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-trans-dl", "database"))
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
			TopicName:     topicName,
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

// deadlineCaptureFakeSPForwarder captures the context deadline on CreateResource calls.
type deadlineCaptureFakeSPForwarder struct {
	onCall func(ctx context.Context)
}

func (f *deadlineCaptureFakeSPForwarder) CreateResource(ctx context.Context, _ string, _ bool, _ routing.CreateResourceRequest) error {
	if f.onCall != nil {
		f.onCall(ctx)
	}
	return nil
}

func (f *deadlineCaptureFakeSPForwarder) DeleteResource(_ context.Context, _ string, _ bool, _ routing.DeleteResourceRequest) error {
	return nil
}
