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
	"github.com/dcm-project/environment-agent/internal/messaging"
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
		topics        messaging.TopicNames
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
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		// Retry stream — agent-owned.
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topics.RetryStream(), Subjects: []string{topics.Retry},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topics.RetryStream(), jetstream.ConsumerConfig{
			Durable: topics.RetryConsumer(), AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())

		// Main + cancel consumers on the (shared, CP-owned in production; created
		// once in suite BeforeSuite here) dcm-agent-requests stream (needed for IT-RCM-070).
		_, err = testJS.CreateOrUpdateConsumer(ctx, messaging.RequestStreamName, jetstream.ConsumerConfig{
			Durable: topics.CancelConsumer(), FilterSubject: topics.Cancel, AckPolicy: jetstream.AckExplicitPolicy,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, messaging.RequestStreamName, jetstream.ConsumerConfig{
			Durable: topics.MainConsumer(), FilterSubject: topics.Main, AckPolicy: jetstream.AckExplicitPolicy,
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
			Topics:        topics,
		})
	})

	AfterEach(func() {
		cancel()
		if responseSub != nil {
			_ = responseSub.Unsubscribe()
		}
		_ = testJS.DeleteConsumer(context.Background(), messaging.RequestStreamName, topics.MainConsumer())
		_ = testJS.DeleteConsumer(context.Background(), messaging.RequestStreamName, topics.CancelConsumer())
		_ = testJS.DeleteStream(context.Background(), topics.RetryStream())
		_ = testJS.DeleteStream(context.Background(), "dcm-responses")
		testConn.Close()
	})

	It("re-reads retry on restart without extra request-queued CEs (IT-RCM-050)", func() {
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		// Seed retry topic with messages from "prior session"
		_, err := testJS.Publish(ctx, topics.Retry, routingtest.BuildCreateCE("res-prior-1", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topics.Retry, routingtest.BuildCreateCE("res-prior-2", "database"))
		Expect(err).NotTo(HaveOccurred())

		Expect(processor.ProcessOnRestart(ctx)).To(Succeed())

		// No additional request-queued CEs (initial ones were sent in the prior session)
		routingtest.ExpectNoResponseCE(responseSub, 2*time.Second)

		// Messages must remain held (pending on durable consumer)
		cons, err := testJS.Consumer(ctx, topics.RetryStream(), topics.RetryConsumer())
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending + uint64(info.NumAckPending)).To(BeNumerically(">", 0))

		// Drain summary logged for auditability.
		Expect(logBuf.String()).To(ContainSubstring("drained retry topic on restart"))
	})

	It("drains cancel topic into deny list, filters main AND retry (IT-RCM-070)", func() {
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		// Seed cancel topic
		_, err := testJS.Publish(ctx, topics.Cancel, routingtest.BuildCancelCE("res-456", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topics.Cancel, routingtest.BuildCancelCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Seed main topic: cancelled resources + one non-cancelled positive control
		_, err = testJS.Publish(ctx, topics.Main, routingtest.BuildCreateCE("res-456", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topics.Main, routingtest.BuildCreateCE("res-allowed", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topics.Main, routingtest.BuildCreateCE("res-789", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Seed retry topic with creates for cancelled resources
		_, err = testJS.Publish(ctx, topics.Retry, routingtest.BuildCreateCE("res-456", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topics.Retry, routingtest.BuildCreateCE("res-789", "database"))
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

		// Drain summaries logged for each phase (cancel, main, retry).
		logged := logBuf.String()
		Expect(logged).To(ContainSubstring("drained cancel topic on restart"))
		Expect(logged).To(ContainSubstring("drained main topic on restart"))
		Expect(logged).To(ContainSubstring("drained retry topic on restart"))
	})
})
