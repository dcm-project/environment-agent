package retry_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
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

var _ = Describe("Delivery Safety Net", Label("integration"), func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		testConn    *nats.Conn
		testJS      jetstream.JetStream
		responseSub *nats.Subscription
		topicName   string
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("maxdlv-test-%s", uuid.New().String()[:8])
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
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

	It("publishes terminal error CE when MaxDeliver exceeded (IT-RCM-080)", func() {
		maxDeliver := 3

		_, err := testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName, Subjects: []string{topicName},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName, jetstream.ConsumerConfig{
			Durable:    topicName + "-consumer",
			AckPolicy:  jetstream.AckExplicitPolicy,
			MaxDeliver: maxDeliver,
			AckWait:    1 * time.Second,
		})
		Expect(err).NotTo(HaveOccurred())

		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		fwdr := &routingtest.FakeSPForwarder{
			CreateErr: &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"},
		}
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{MaxDeliver: maxDeliver},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		// Start consuming — the processor actively reads from the main topic
		Expect(processor.Start(ctx)).To(Succeed())
		defer processor.Stop()

		_, err = testJS.Publish(ctx, topicName, routingtest.BuildCreateCE("res-md1", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Poll softly for the terminal error CE (don't use ExpectResponseCE inside Eventually)
		Eventually(func() string {
			msg, err := responseSub.NextMsg(1 * time.Second)
			if err != nil {
				return ""
			}
			var ce cloudevents.Event
			if json.Unmarshal(msg.Data, &ce) != nil {
				return ""
			}
			if ce.Type() != "dcm.agent.error" {
				return ""
			}
			var data routing.ErrorData
			if json.Unmarshal(ce.Data(), &data) != nil {
				return ""
			}
			return data.Error
		}, 15*time.Second).Should(Equal("MAX_DELIVERY_EXCEEDED"))

		// SP should not receive more than MaxDeliver calls
		Expect(fwdr.CreateCallCount()).To(BeNumerically("<=", maxDeliver))

		// Message must be terminated — no pending/ack-pending left
		cons, err := testJS.Consumer(ctx, topicName, topicName+"-consumer")
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending + uint64(info.NumAckPending)).To(Equal(uint64(0)))
	})

	It("creates consumers with configured MaxDeliver limit (IT-RCM-090)", func() {
		configuredMaxDeliver := 7

		_, err := testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName, Subjects: []string{topicName},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName + "-retry", Subjects: []string{topicName + ".retry"},
		})
		Expect(err).NotTo(HaveOccurred())

		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		fwdr := &routingtest.FakeSPForwarder{}
		pub := &routingtest.NATSPublisher{JS: testJS}
		denyList := routing.NewResourceSet(100)

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{MaxDeliver: configuredMaxDeliver},
			Logger:        slog.Default(),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})

		// Ensure consumers are created with configured MaxDeliver
		Expect(processor.EnsureConsumers(ctx)).To(Succeed())

		mainCons, err := testJS.Consumer(ctx, topicName, topicName+"-consumer")
		Expect(err).NotTo(HaveOccurred())
		mainInfo, err := mainCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(mainInfo.Config.MaxDeliver).To(Equal(configuredMaxDeliver))

		retryCons, err := testJS.Consumer(ctx, topicName+"-retry", topicName+"-retry-consumer")
		Expect(err).NotTo(HaveOccurred())
		retryInfo, err := retryCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(retryInfo.Config.MaxDeliver).To(Equal(configuredMaxDeliver))
	})
})
