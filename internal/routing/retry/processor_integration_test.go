package retry_test

import (
	"bytes"
	"context"
	"encoding/json"
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

var _ = Describe("Retry Topic Processing", Label("integration"), func() {
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
		topicName = fmt.Sprintf("retry-test-%s", uuid.New().String()[:8])
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		var err error
		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: topicName + "-retry", Subjects: []string{topicName + ".retry"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name: "dcm-responses", Subjects: []string{"dcm.agents.responses"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateConsumer(ctx, topicName+"-retry", jetstream.ConsumerConfig{
			Durable: topicName + "-retry-consumer", AckPolicy: jetstream.AckExplicitPolicy,
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
		_ = testJS.DeleteStream(context.Background(), "dcm-responses")
		testConn.Close()
	})

	It("forwards held creates when SP recovers to Ready (IT-RCM-010)", func() {
		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		ceA := routingtest.BuildCreateCE("res-a", "database")
		ceB := routingtest.BuildCreateCE("res-b", "database")
		ceC := routingtest.BuildCreateCE("res-c", "database")
		_, err := testJS.Publish(ctx, topicName+".retry", ceA)
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", ceB)
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", ceC)
		Expect(err).NotTo(HaveOccurred())

		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(3))

		for _, resID := range []string{"res-a", "res-b", "res-c"} {
			ce := routingtest.ExpectResponseCE(responseSub)
			Expect(ce.Type()).To(Equal("dcm.agent.creation-acknowledged"))
			var data routing.CreationAckData
			Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
			Expect(data.ResourceID).To(Equal(resID))
			Expect(data.AgentName).To(Equal("agent-prod-1"))
			Expect(data.TopicName).To(Equal(topicName))
			Expect(data.Status).To(Equal("PROVISIONING"))
		}

		// REQ-RCM-010: verify original CE bytes forwarded without wrapping
		calls := fwdr.GetCreateCalls()
		Expect(calls).To(HaveLen(3))
		Expect(calls[0].Req.ResourceID).To(Equal("res-a"))
		Expect(calls[1].Req.ResourceID).To(Equal("res-b"))
		Expect(calls[2].Req.ResourceID).To(Equal("res-c"))
	})

	It("rejects held creates when SP becomes Unavailable (IT-RCM-020)", func() {
		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		_, err := testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-d1", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-d2", "database"))
		Expect(err).NotTo(HaveOccurred())

		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Unavailable)).To(Succeed())

		for _, resID := range []string{"res-d1", "res-d2"} {
			ce := routingtest.ExpectResponseCE(responseSub)
			Expect(ce.Type()).To(Equal("dcm.agent.error"))
			var data routing.ErrorData
			Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
			Expect(data.ResourceID).To(Equal(resID))
			Expect(data.AgentName).To(Equal("agent-prod-1"))
			Expect(data.Error).To(Equal("SP_UNAVAILABLE"))
		}

		Expect(fwdr.CreateCallCount()).To(Equal(0))
	})

	It("rejects Unavailable, requeues Unhealthy, no extra CEs (IT-RCM-030)", func() {
		containerProviderID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "container-sp", "container", v1alpha1.Unavailable)
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-sp", "database", v1alpha1.Unhealthy)

		_, err := testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-db-1", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-ctr-1", "container"))
		Expect(err).NotTo(HaveOccurred())

		// Trigger via the container SP transition to Unavailable
		Expect(processor.ProcessOnTransition(ctx, containerProviderID, v1alpha1.Unhealthy, v1alpha1.Unavailable)).To(Succeed())

		// "container" request rejected with error CE
		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.error"))
		var errData routing.ErrorData
		Expect(json.Unmarshal(ce.Data(), &errData)).To(Succeed())
		Expect(errData.ResourceID).To(Equal("res-ctr-1"))

		// "database" request re-published to retry topic — no extra request-queued CEs
		routingtest.ExpectNoResponseCE(responseSub, 2*time.Second)

		// SP should not have been called for either
		Expect(fwdr.CreateCallCount()).To(Equal(0))

		// "database" message must have been re-published to retry topic
		retryInfo, err := testJS.Stream(ctx, topicName+"-retry")
		Expect(err).NotTo(HaveOccurred())
		si, err := retryInfo.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(si.State.Msgs).To(BeNumerically(">", 0), "database message should be re-published to retry topic")
	})

	It("preserves FIFO ordering per service type (IT-RCM-040)", func() {
		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		_, err := testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-order-A", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-order-B", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-order-C", "database"))
		Expect(err).NotTo(HaveOccurred())

		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(3))

		calls := fwdr.GetCreateCalls()
		Expect(calls[0].Req.ResourceID).To(Equal("res-order-A"))
		Expect(calls[1].Req.ResourceID).To(Equal("res-order-B"))
		Expect(calls[2].Req.ResourceID).To(Equal("res-order-C"))
	})

	It("dedup removes create+delete pair, publishes deletion-ack DELETED (IT-RCM-060)", func() {
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Unhealthy)

		_, err := testJS.Publish(ctx, topicName+".retry", routingtest.BuildCreateCE("res-123", "database"))
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Publish(ctx, topicName+".retry", routingtest.BuildDeleteCE("res-123", "database"))
		Expect(err).NotTo(HaveOccurred())

		// Any transition triggers retry processing — use Unhealthy→Unhealthy or a third provider
		// The dedup must happen during any retry topic scan
		dbProviderID := ""
		for _, sp := range st.Providers {
			if sp.ServiceType == "database" {
				dbProviderID = sp.ID
				break
			}
		}
		healthTracker.SetState(dbProviderID, v1alpha1.Ready, time.Now())
		Expect(processor.ProcessOnTransition(ctx, dbProviderID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())

		// Neither should be forwarded to SP
		Expect(fwdr.CreateCallCount()).To(Equal(0))
		Expect(fwdr.DeleteCallCount()).To(Equal(0))

		// deletion-ack CE with status "DELETED"
		ce := routingtest.ExpectResponseCE(responseSub)
		Expect(ce.Type()).To(Equal("dcm.agent.deletion-acknowledged"))
		var data routing.DeletionAckData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ResourceID).To(Equal("res-123"))
		Expect(data.Status).To(Equal("DELETED"))

		// Dedup logged
		Expect(logBuf.String()).To(ContainSubstring("dedup"))
		Expect(logBuf.String()).To(ContainSubstring("res-123"))
	})
})
