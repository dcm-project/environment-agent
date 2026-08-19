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
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/retry"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

// Verifies the transient in-flight lock (REQ-RTE-210/AC-RTE-210) is effective
// ACROSS the router and retry-processor paths when they share one
// InFlightSet instance, the way cmd/environment-agent/main.go wires them in
// production.
var _ = Describe("Cross-Path In-Flight Guard", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		topics    messaging.TopicNames
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("inflight-test-%s", uuid.New().String()[:8])
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
	})

	AfterEach(func() {
		cancel()
		_ = testJS.DeleteStream(context.Background(), topics.RetryStream())
		testConn.Close()
	})

	It("blocks the retry-topic path from double-dispatching a resourceId the router is concurrently forwarding (IT-RCM-160)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		pub := &routingtest.NATSPublisher{JS: testJS}
		gated := &routingtest.GatedSPForwarder{}

		providerID := routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)

		router := routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: gated, Publisher: pub, DenyList: routing.NewResourceSet(100),
			Config: config.RoutingConfig{RetryMaxAttempts: 1},
			Logger: slog.Default(), AgentName: "agent-prod-1",
			TopicName: topics.Main, RetryTopic: topics.Retry,
		})

		processor := retry.NewProcessor(retry.ProcessorDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: gated, Publisher: pub, JSProvider: func() jetstream.JetStream { return testJS },
			DenyList: routing.NewResourceSet(100), InFlightSet: router.InFlightSet(),
			Config: retry.ProcessorConfig{}, Logger: slog.Default(), AgentName: "agent-prod-1",
			Topics: topics,
		})

		// Simulate: this resourceId's message also lives on the retry topic
		// (e.g. a prior Unhealthy period) while the SAME resourceId's main-
		// topic message is concurrently being forwarded by the router.
		_, err := testJS.Publish(ctx, topics.Retry, routingtest.BuildCreateCE("res-cross-race", "database"))
		Expect(err).NotTo(HaveOccurred())

		mainDone := make(chan error, 1)
		go func() {
			mainDone <- router.HandleRequest(ctx, routingtest.BuildCreateCE("res-cross-race", "database"))
		}()

		Eventually(gated.CallCount, time.Second).Should(Equal(1))

		// Retry-topic path races the same resourceId while the router's
		// forward is still in flight — must be requeued (Nak'd), not
		// double-dispatched to the SP.
		Expect(processor.ProcessOnTransition(ctx, providerID, v1alpha1.Unhealthy, v1alpha1.Ready)).To(Succeed())
		Expect(gated.CallCount()).To(Equal(1), "SP must not be called twice concurrently for the same resourceId")

		retryCons, err := testJS.Consumer(ctx, topics.RetryStream(), topics.RetryConsumer())
		Expect(err).NotTo(HaveOccurred())
		info, err := retryCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending+uint64(info.NumAckPending)).To(BeNumerically(">", 0),
			"retry-topic message must be Nak'd (deferred), not lost, when blocked by the in-flight guard")

		gated.Release()
		Expect(<-mainDone).NotTo(HaveOccurred())
	})
})
