package retry_test

import (
	"context"
	"fmt"
	"log/slog"
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

var _ = Describe("Idempotency-Key Forwarding", Label("integration"), func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		testConn    *nats.Conn
		testJS      jetstream.JetStream
		responseSub *nats.Subscription
		topicName   string
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("idem-test-%s", uuid.New().String()[:8])
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
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName + "-retry", Subjects: []string{topicName + ".retry"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName, jetstream.ConsumerConfig{
			Durable: topicName + "-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName+"-retry", jetstream.ConsumerConfig{
			Durable: topicName + "-retry-consumer", AckPolicy: jetstream.AckExplicitPolicy,
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
		_ = testJS.DeleteStream(context.Background(), topicName+"-retry")
		_ = testJS.DeleteStream(context.Background(), "dcm-responses")
		testConn.Close()
	})

	It("forwards Idempotency-Key to external SP (IT-RCM-120)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		fwdr := &routingtest.FakeSPForwarder{}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		Expect(processor.Start(ctx)).To(Succeed())
		defer processor.Stop()

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-1", "res-ext1", "database")
		_, err := testJS.Publish(ctx, topicName, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(1))

		calls := fwdr.GetCreateCalls()
		Expect(calls[0].Req.EventID).To(Equal("ce-idem-1"))
	})

	It("keeps Idempotency-Key stable across in-line retry attempts (IT-RCM-130)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		fwdr := &routingtest.FakeSPForwarder{
			CreateErr: &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"},
			FailFirst: 2,
		}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		Expect(processor.Start(ctx)).To(Succeed())
		defer processor.Stop()

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-2", "res-ext2", "database")
		_, err := testJS.Publish(ctx, topicName, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return fwdr.CreateCallCount() }, 10*time.Second).Should(Equal(3))

		calls := fwdr.GetCreateCalls()
		Expect(calls).To(HaveLen(3))
		for _, c := range calls {
			Expect(c.Req.EventID).To(Equal("ce-idem-2"))
		}
	})

	It("keeps Idempotency-Key stable across retry-topic reprocessing (IT-RCM-140)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		fwdr := &routingtest.FakeSPForwarder{}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-3", "res-ext3", "database")
		_, err := testJS.Publish(ctx, topicName+".retry", ceBytes)
		Expect(err).NotTo(HaveOccurred())

		healthTracker.SetState(providerID, v1alpha1.Ready, time.Now())
		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(BeNumerically(">=", 1))

		for _, c := range fwdr.GetCreateCalls() {
			Expect(c.Req.EventID).To(Equal("ce-idem-3"))
		}
	})

	It("makes Idempotency-Key available to embedded SPs via request (IT-RCM-150)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		routingtest.RegisterEmbeddedSP(ctx, registry, healthTracker, st, "embedded-sp", "container", v1alpha1.Ready)

		fwdr := &routingtest.FakeSPForwarder{}

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		Expect(processor.Start(ctx)).To(Succeed())
		defer processor.Stop()

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-4", "res-embed1", "container")
		_, err := testJS.Publish(ctx, topicName, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(1))

		calls := fwdr.GetCreateCalls()
		Expect(calls[0].Req.EventID).To(Equal("ce-idem-4"))
	})
})
