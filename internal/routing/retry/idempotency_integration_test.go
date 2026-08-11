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
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/retry"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

// Idempotency-key forwarding on the main-topic consume path is covered in
// internal/messaging/client_integration_test.go. Only the retry-topic
// reprocessing case (IT-RCM-140, via ProcessOnTransition) belongs here.
var _ = Describe("Idempotency-Key Forwarding", Label("integration"), func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		testConn    *nats.Conn
		testJS      jetstream.JetStream
		responseSub *nats.Subscription
		topicName   string
		topics      messaging.TopicNames
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("idem-test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topics.RetryStream(), Subjects: []string{topics.Retry},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topics.RetryStream(), jetstream.ConsumerConfig{
			Durable: topics.RetryConsumer(), AckPolicy: jetstream.AckExplicitPolicy,
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
		_ = testJS.DeleteStream(context.Background(), topics.RetryStream())
		_ = testJS.DeleteStream(context.Background(), "dcm-responses")
		testConn.Close()
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
			Topics:        topics,
		})

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-3", "res-ext3", "database")
		_, err := testJS.Publish(ctx, topics.Retry, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		healthTracker.SetState(providerID, v1alpha1.Ready, time.Now())
		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(BeNumerically(">=", 1))

		for _, c := range fwdr.GetCreateCalls() {
			Expect(c.Req.EventID).To(Equal("ce-idem-3"))
		}
	})
})
