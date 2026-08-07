package messaging_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/routingtest"
)

// This file covers cases formerly exercised through retry.Processor.Start
// (dead code removed — see internal/routing/retry/processor.go doc comment
// on ProcessorConfig): MaxDeliver-exceeded handling, MaxDeliver consumer
// config, handler-deadline enforcement, and idempotency-key forwarding on
// the main-topic consume path. That path is now exclusively
// messaging.Client.handleMainMessage → router.HandleRequest → SP forwarder,
// so it's tested here against a real Client + Router, rather than in the
// retry package.

// hangingFakeSPForwarder blocks forever on CreateResource, calling onCancel
// when its context is cancelled (mirrors retry package's test double).
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

var _ = Describe("Main-Topic Delivery Safety Net", Label("integration"), func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		testConn    *nats.Conn
		testJS      jetstream.JetStream
		responseSub *nats.Subscription
		topicName   string
		topics      messaging.TopicNames
		logger      *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("maxdlv-test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		responseSub, err = testConn.SubscribeSync(cloudevent.SubjectResponses)
		Expect(err).NotTo(HaveOccurred())
		Expect(testConn.Flush()).To(Succeed())
	})

	AfterEach(func() {
		cancel()
		if responseSub != nil {
			_ = responseSub.Unsubscribe()
		}
		deleteTestArtifacts(testJS, topics)
		testConn.Close()
	})

	It("publishes terminal error CE when MaxDeliver exceeded (IT-RCM-080)", func() {
		maxDeliver := 3

		client := messaging.NewClient(messaging.ClientConfig{
			URL:        testNATSServer.ClientURL(),
			TopicName:  topicName,
			AgentName:  "agent-prod-1",
			MaxDeliver: maxDeliver,
			AckWait:    1 * time.Second,
		}, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		var attempts atomic.Int32
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			attempts.Add(1)
			return fmt.Errorf("simulated handler failure")
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-md1"})

		Eventually(func() string {
			msg, err := responseSub.NextMsg(1 * time.Second)
			if err != nil {
				return ""
			}
			var ce cloudevents.Event
			if json.Unmarshal(msg.Data, &ce) != nil || ce.Type() != cloudevent.TypeError {
				return ""
			}
			var data routing.ErrorData
			if json.Unmarshal(ce.Data(), &data) != nil {
				return ""
			}
			return data.Error
		}, 15*time.Second).Should(Equal(routing.ErrorMaxDeliveryExceeded))

		// Handler should not be called more than MaxDeliver times
		Expect(int(attempts.Load())).To(BeNumerically("<=", maxDeliver))

		// Message must be terminated — no pending/ack-pending left
		cons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending + uint64(info.NumAckPending)).To(Equal(uint64(0)))
	})

	It("creates main and retry consumers with configured MaxDeliver limit (IT-RCM-090)", func() {
		configuredMaxDeliver := 7

		client := messaging.NewClient(messaging.ClientConfig{
			URL:        testNATSServer.ClientURL(),
			TopicName:  topicName,
			AgentName:  "agent-prod-1",
			MaxDeliver: configuredMaxDeliver,
		}, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		mainCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		mainInfo, err := mainCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(mainInfo.Config.MaxDeliver).To(Equal(configuredMaxDeliver))

		retryCons, err := testJS.Consumer(ctx, topics.RetryStream(), topics.RetryConsumer())
		Expect(err).NotTo(HaveOccurred())
		retryInfo, err := retryCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(retryInfo.Config.MaxDeliver).To(Equal(configuredMaxDeliver))

		// Cancel consumer intentionally has NO MaxDeliver limit — cancels must
		// never be dropped by delivery-count exhaustion. JetStream normalizes
		// "unlimited" to -1 server-side.
		cancelCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.CancelConsumer())
		Expect(err).NotTo(HaveOccurred())
		cancelInfo, err := cancelCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cancelInfo.Config.MaxDeliver).To(Equal(-1))
	})
})

var _ = Describe("Handler Processing Deadline", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		topics    messaging.TopicNames
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("deadline-test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteTestArtifacts(testJS, topics)
		testConn.Close()
	})

	newRouter := func(fwdr routing.SPForwarder, pub routing.Publisher) *routing.Router {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)
		return routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fwdr, Publisher: pub, DenyList: routing.NewResourceSet(100),
			Config: config.RoutingConfig{RetryMaxAttempts: 1},
			Logger: logger, AgentName: "agent-prod-1",
			TopicName: topics.Main, RetryTopic: topics.Retry,
		})
	}

	It("aborts hung SP call after handler deadline elapses (IT-RCM-100)", func() {
		handlerTimeout := 1 * time.Second

		client := messaging.NewClient(messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "agent-prod-1",
			HandlerTimeout: handlerTimeout,
		}, logger)

		var ctxCancelled atomic.Bool
		hangingForwarder := &hangingFakeSPForwarder{onCancel: func() { ctxCancelled.Store(true) }}
		router := newRouter(hangingForwarder, client)
		client.SetMainHandler(router.HandleRequest)
		client.SetCancelHandler(router.HandleCancel)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-hd1", "service_type": "database"})

		// Wait for the deadline to elapse plus buffer for CI
		Eventually(ctxCancelled.Load, handlerTimeout+2*time.Second).Should(BeTrue())

		// Message must NOT be acked — eligible for redelivery
		cons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending + uint64(info.NumAckPending)).To(BeNumerically(">", 0))
	})

	It("applies configurable handler deadline to SP call context (IT-RCM-110)", func() {
		handlerTimeout := 2500 * time.Millisecond

		client := messaging.NewClient(messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "agent-prod-1",
			HandlerTimeout: handlerTimeout,
		}, logger)

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
		router := newRouter(deadlineForwarder, client)
		client.SetMainHandler(router.HandleRequest)
		client.SetCancelHandler(router.HandleCancel)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		before := time.Now()
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-hd2", "service_type": "database"})

		Eventually(deadlineSet.Load, 5*time.Second).Should(BeTrue())

		// Deadline should be approximately handlerTimeout from invocation
		Expect(observedDeadline).To(BeTemporally("~", before.Add(handlerTimeout), 1*time.Second))
	})
})

var _ = Describe("Idempotency-Key Forwarding", Label("integration"), func() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
		testConn    *nats.Conn
		testJS      jetstream.JetStream
		responseSub *nats.Subscription
		topicName   string
		topics      messaging.TopicNames
		logger      *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("idem-test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("agent-prod-1", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())

		responseSub, err = testConn.SubscribeSync(cloudevent.SubjectResponses)
		Expect(err).NotTo(HaveOccurred())
		Expect(testConn.Flush()).To(Succeed())
	})

	AfterEach(func() {
		cancel()
		if responseSub != nil {
			_ = responseSub.Unsubscribe()
		}
		deleteTestArtifacts(testJS, topics)
		testConn.Close()
	})

	It("forwards Idempotency-Key (CE id) to external SP (IT-RCM-120)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)
		fwdr := &routingtest.FakeSPForwarder{}

		client := messaging.NewClient(messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "agent-prod-1",
		}, logger)
		router := routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fwdr, Publisher: client, DenyList: routing.NewResourceSet(100),
			Config: config.RoutingConfig{RetryMaxAttempts: 1},
			Logger: logger, AgentName: "agent-prod-1",
			TopicName: topics.Main, RetryTopic: topics.Retry,
		})
		client.SetMainHandler(router.HandleRequest)
		client.SetCancelHandler(router.HandleCancel)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-1", "res-ext1", "database")
		_, err := testJS.Publish(ctx, topics.Main, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(1))

		calls := fwdr.GetCreateCalls()
		Expect(calls[0].Req.EventID).To(Equal("ce-idem-1"))
	})

	It("keeps Idempotency-Key stable across in-line retry attempts (IT-RCM-130)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		routingtest.RegisterSP(ctx, registry, healthTracker, st, "db-provider", "database", v1alpha1.Ready)
		fwdr := &routingtest.FakeSPForwarder{
			CreateErr: &routing.SPResponseError{StatusCode: 503, Message: "Service Unavailable"},
			FailFirst: 2,
		}

		client := messaging.NewClient(messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "agent-prod-1",
		}, logger)
		router := routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fwdr, Publisher: client, DenyList: routing.NewResourceSet(100),
			Config: config.RoutingConfig{
				RetryMaxAttempts: 3, RetryBackoff: 10 * time.Millisecond, RetryMaxBackoff: 50 * time.Millisecond,
			},
			Logger: logger, AgentName: "agent-prod-1",
			TopicName: topics.Main, RetryTopic: topics.Retry,
		})
		client.SetMainHandler(router.HandleRequest)
		client.SetCancelHandler(router.HandleCancel)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-2", "res-ext2", "database")
		_, err := testJS.Publish(ctx, topics.Main, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return fwdr.CreateCallCount() }, 10*time.Second).Should(Equal(3))

		calls := fwdr.GetCreateCalls()
		Expect(calls).To(HaveLen(3))
		for _, c := range calls {
			Expect(c.Req.EventID).To(Equal("ce-idem-2"))
		}
	})

	It("makes Idempotency-Key available to embedded SPs via request (IT-RCM-150)", func() {
		registry := provider.NewRegistry()
		healthTracker := provider.NewInMemoryHealthTracker()
		st := routingtest.NewFakeStore()
		routingtest.RegisterEmbeddedSP(ctx, registry, healthTracker, st, "embedded-sp", "container", v1alpha1.Ready)
		fwdr := &routingtest.FakeSPForwarder{}

		client := messaging.NewClient(messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "agent-prod-1",
		}, logger)
		router := routing.NewRouter(routing.RouterDeps{
			Registry: registry, HealthTracker: healthTracker, Store: st,
			Forwarder: fwdr, Publisher: client, DenyList: routing.NewResourceSet(100),
			Config: config.RoutingConfig{RetryMaxAttempts: 1},
			Logger: logger, AgentName: "agent-prod-1",
			TopicName: topics.Main, RetryTopic: topics.Retry,
		})
		client.SetMainHandler(router.HandleRequest)
		client.SetCancelHandler(router.HandleCancel)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		ceBytes := routingtest.BuildCreateCEWithID("ce-idem-4", "res-embed1", "container")
		_, err := testJS.Publish(ctx, topics.Main, ceBytes)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int { return fwdr.CreateCallCount() }, 5*time.Second).Should(Equal(1))

		calls := fwdr.GetCreateCalls()
		Expect(calls[0].Req.EventID).To(Equal("ce-idem-4"))
	})
})
