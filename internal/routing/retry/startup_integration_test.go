package retry_test

import (
	"bytes"
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

var _ = Describe("Retry/Cancel Startup", Label("integration"), func() {
	var (
		ctx           context.Context
		cancel        context.CancelFunc
		testConn      *nats.Conn
		testJS        jetstream.JetStream
		responseSub   *nats.Subscription
		topicName     string
		processor     *retry.Processor
		registry      *provider.Registry
		healthTracker *provider.InMemoryHealthTracker
		st            *routingtest.FakeStore
		fwdr          *routingtest.FakeSPForwarder
		pub           *routingtest.NATSPublisher
		denyList      *routing.ResourceSet
		logBuf        *bytes.Buffer
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("startup-test-%s", uuid.New().String()[:8])
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		// Retry stream
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName + "-retry", Subjects: []string{topicName + ".retry"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName+"-retry", jetstream.ConsumerConfig{
			Durable: topicName + "-retry-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		// Cancel stream (needed for IT-RCM-070)
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName + "-cancel", Subjects: []string{topicName + ".cancel"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName+"-cancel", jetstream.ConsumerConfig{
			Durable: topicName + "-cancel-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		// Main stream + consumer (needed for IT-RCM-070)
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName, Subjects: []string{topicName},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName, jetstream.ConsumerConfig{
			Durable: topicName + "-consumer", AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		// Responses stream
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: "dcm-responses", Subjects: []string{"dcm.agents.responses"},
		})
		Expect(err).NotTo(HaveOccurred())

		responseSub, err = testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())
		Expect(testConn.Flush()).To(Succeed())

		registry = provider.NewRegistry()
		healthTracker = provider.NewInMemoryHealthTracker()
		st = routingtest.NewFakeStore()
		fwdr = &routingtest.FakeSPForwarder{}
		pub = &routingtest.NATSPublisher{JS: testJS}
		denyList = routing.NewResourceSet(100)
		logBuf = &bytes.Buffer{}

		processor = retry.NewProcessor(retry.ProcessorDeps{
			Registry:      registry,
			HealthTracker: healthTracker,
			Store:         st,
			Forwarder:     fwdr,
			Publisher:     pub,
			JSProvider:    func() jetstream.JetStream { return testJS },
			DenyList:      denyList,
			Config:        retry.ProcessorConfig{},
			Logger:        slog.New(slog.NewTextHandler(logBuf, nil)),
			AgentName:     "agent-prod-1",
			TopicName:     topicName,
		})
	})

	AfterEach(func() {
		cancel()
		if responseSub != nil {
			_ = responseSub.Unsubscribe()
		}
		_ = testJS.DeleteStream(context.Background(), topicName+"-retry")
		_ = testJS.DeleteStream(context.Background(), topicName+"-cancel")
		_ = testJS.DeleteStream(context.Background(), topicName)
		_ = testJS.DeleteStream(context.Background(), "dcm-responses")
		testConn.Close()
	})

	It("re-reads retry on restart without extra request-queued CEs (IT-RCM-050)", func() {
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		// Seed retry topic with messages from "prior session"
		_, err := testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-prior-1", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-prior-2", "database"))
		Expect(err).NotTo(HaveOccurred())

		Expect(processor.ProcessOnRestart(ctx)).To(Succeed())

		// No additional request-queued CEs (initial ones were sent in the prior session)
		routingtest.ExpectNoResponseCE(responseSub, 2*time.Second)

		// Messages must remain held (pending on durable consumer)
		cons, err := testJS.Consumer(ctx, topicName+"-retry", topicName+"-retry-consumer")
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending + uint64(info.NumAckPending)).To(BeNumerically(">", 0))
	})

	It("drains cancel topic into deny list, filters main AND retry (IT-RCM-070)", func() {
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		// Seed cancel topic
		_, err := testJS.Publish(ctx, topicName+".cancel", routingtest.BuildCancelCE("res-456", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".cancel", routingtest.BuildCancelCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Seed main topic: cancelled resources + one non-cancelled positive control
		_, err = testJS.Publish(ctx, topicName, routingtest.BuildCreateCE("res-456", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName, routingtest.BuildCreateCE("res-allowed", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName, routingtest.BuildCreateCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Seed retry topic with creates for cancelled resources
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-456", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Trigger full startup sequence (cancel drain → main → retry)
		Expect(processor.ProcessOnRestart(ctx)).To(Succeed())

		// Positive control: non-cancelled resource must be forwarded
		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(1))
		calls := fwdr.GetCreateCalls()
		Expect(calls[0].Req.ResourceID).To(Equal("res-allowed"))

		// Cancelled resources must NOT have been forwarded
		Consistently(func() int {
			return fwdr.CreateCallCount()
		}, 2*time.Second, 100*time.Millisecond).Should(Equal(1))

		// Deny list must be populated
		Expect(denyList.Contains("res-456")).To(BeTrue())
		Expect(denyList.Contains("res-789")).To(BeTrue())
	})
})
