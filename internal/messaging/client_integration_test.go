package messaging_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/messaging"
)

// shutdownWaitBound generously bounds how long Client.Stop is allowed to
// take in tests — must exceed the client's internal shutdownDrainTimeout
// (5s) plus the simulated in-flight handler delay used below.
const shutdownWaitBound = 8 * time.Second

// captureHandler records slog records for assertion.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

func findRecord(records []slog.Record, msg string) (slog.Record, bool) {
	for _, r := range records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

func setNoopHandlers(c *messaging.Client) {
	c.SetMainHandler(func(_ context.Context, _ []byte) error { return nil })
	c.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
}

// deleteTestArtifacts removes the per-test durable consumers created on the
// shared (simulated CP-owned) request stream, plus the agent-owned retry
// stream. It does NOT delete messaging.RequestStreamName or the shared
// response stream — those are suite-scoped and shared across tests.
func deleteTestArtifacts(js jetstream.JetStream, topics messaging.TopicNames) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = js.DeleteConsumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
	_ = js.DeleteConsumer(ctx, messaging.RequestStreamName, topics.CancelConsumer())
	_ = js.DeleteStream(ctx, topics.RetryStream())
}

func publishCE(ctx context.Context, js jetstream.JetStream, subject, ceType, source string, payload any) {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource(source)
	event.SetType(ceType)
	event.SetTime(time.Now())
	_ = event.SetData(cloudevents.ApplicationJSON, payload)
	data, err := json.Marshal(event)
	Expect(err).NotTo(HaveOccurred())
	_, err = js.Publish(ctx, subject, data)
	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("Topic Management", Label("integration"), func() {
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
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("creates agent-owned retry stream and CP-facing durable consumers at startup (IT-MSG-010)", func() {
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		// Retry stream is agent-owned.
		retryStream, err := testJS.Stream(ctx, topics.RetryStream())
		Expect(err).NotTo(HaveOccurred())
		Expect(retryStream.CachedInfo().Config.Subjects).To(ContainElement(topics.Retry))

		// Main/Cancel are durable consumers on the control-plane-owned
		// RequestStreamName (F2) — the agent must NOT create streams for them.
		mainCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		mainInfo, err := mainCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(mainInfo.Config.FilterSubject).To(Equal(topics.Main))

		cancelCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.CancelConsumer())
		Expect(err).NotTo(HaveOccurred())
		cancelInfo, err := cancelCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cancelInfo.Config.FilterSubject).To(Equal(topics.Cancel))
	})

	It("creates deterministic durable consumer names derived from topic (IT-MSG-020)", func() {
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		_, err := testJS.Consumer(ctx, messaging.RequestStreamName, topicName+"-consumer")
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Consumer(ctx, messaging.RequestStreamName, topicName+"-cancel-consumer")
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.Consumer(ctx, topics.RetryStream(), topicName+"-retry-consumer")
		Expect(err).NotTo(HaveOccurred())
	})

	It("reuses existing topics on restart without error (IT-MSG-050)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		client1 := messaging.NewClient(cfg, logger)
		setNoopHandlers(client1)
		Expect(client1.Start(ctx)).To(Succeed())
		client1.Stop()

		client2 := messaging.NewClient(cfg, logger)
		setNoopHandlers(client2)
		Expect(client2.Start(ctx)).To(Succeed())
		client2.Stop()
	})

	It("reuses existing consumers on restart — no duplicates (IT-MSG-030)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		client1 := messaging.NewClient(cfg, logger)
		setNoopHandlers(client1)
		Expect(client1.Start(ctx)).To(Succeed())

		mainCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		initialInfo, err := mainCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		client1.Stop()

		client2 := messaging.NewClient(cfg, logger)
		setNoopHandlers(client2)
		Expect(client2.Start(ctx)).To(Succeed())
		defer client2.Stop()

		mainCons, err = testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		reusedInfo, err := mainCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		// Same Created timestamp proves CreateOrUpdateConsumer reused the
		// existing durable consumer rather than creating a duplicate.
		Expect(reusedInfo.Created).To(Equal(initialInfo.Created))
	})
})

var _ = Describe("Message Durability", Label("integration"), func() {
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
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("redelivers unacknowledged message after restart (IT-MSG-040)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
			// Short AckWait: client1.Stop() races the handler's async
			// NakWithDelay, so redelivery must not depend on that Nak
			// reaching the server before the connection closes.
			AckWait: 2 * time.Second,
		}

		received := make(chan []byte, 1)
		client1 := messaging.NewClient(cfg, logger)
		client1.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client1.SetMainHandler(func(_ context.Context, msg []byte) error {
			received <- msg
			return fmt.Errorf("simulated failure")
		})
		Expect(client1.Start(ctx)).To(Succeed())

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/test", map[string]string{"key": "value"})

		Eventually(received, 5*time.Second).Should(Receive())
		client1.Stop()

		redelivered := make(chan []byte, 1)
		client2 := messaging.NewClient(cfg, logger)
		client2.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client2.SetMainHandler(func(_ context.Context, msg []byte) error {
			redelivered <- msg
			return nil
		})
		Expect(client2.Start(ctx)).To(Succeed())
		defer client2.Stop()

		Eventually(redelivered, 5*time.Second).Should(Receive())
	})

	It("lets an in-flight handler finish and ack before Stop closes the connection (IT-MSG-140)", func() {
		cfg := messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "test-agent",
			AckWait: 10 * time.Second, // long enough that only a successful ack (not AckWait expiry) prevents redelivery
		}

		handlerStarted := make(chan struct{}, 1)
		var handlerCalls atomic.Int32
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			handlerCalls.Add(1)
			handlerStarted <- struct{}{}
			time.Sleep(500 * time.Millisecond) // simulate in-flight work at the moment Stop() is called
			return nil
		})
		Expect(client.Start(ctx)).To(Succeed())

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/test", map[string]string{"key": "value"})
		Eventually(handlerStarted, 5*time.Second).Should(Receive())

		// Stop while the handler is still sleeping — the old ConsumeContext.
		// Stop()-based shutdown would tear down the subscription immediately,
		// racing the handler's in-flight Ack; Drain must let it finish first.
		stopDone := make(chan struct{})
		go func() { client.Stop(); close(stopDone) }()
		Eventually(stopDone, shutdownWaitBound).Should(BeClosed(), "Stop must return once the in-flight handler finishes (bounded by shutdownDrainTimeout)")

		Expect(handlerCalls.Load()).To(Equal(int32(1)))

		// Check the durable consumer's ack-pending state directly, rather
		// than the handler completing (which doesn't prove the Ack reached
		// the server before the connection closed).
		mainCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())
		info, err := mainCons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.NumPending+uint64(info.NumAckPending)).To(Equal(uint64(0)),
			"message must have been acked before the connection closed, not left ack-pending for redelivery")
	})
})

var _ = Describe("Topic Advertising", Label("integration"), func() {
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
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("returns the dcm.agent.-prefixed main subject — not retry or cancel (IT-MSG-060)", func() {
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		Expect(client.TopicName()).To(Equal(topics.Main))
		Expect(client.TopicName()).To(Equal("dcm.agent." + topicName))
	})
})

var _ = Describe("Message Consumption", Label("integration"), func() {
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
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("invokes handler on main topic message and publishes response CE (IT-MSG-070)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		handlerCalled := make(chan []byte, 1)
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			handlerCalled <- msg
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]string
			_ = json.Unmarshal(event.Data(), &p)
			respEvent := cloudevents.NewEvent()
			respEvent.SetID(uuid.New().String())
			respEvent.SetSource("dcm/agents/test-agent")
			respEvent.SetType("dcm.agent.creation-acknowledged")
			respEvent.SetTime(time.Now())
			_ = respEvent.SetData(cloudevents.ApplicationJSON, map[string]any{
				"agent_name":  "test-agent",
				"topic_name":  topics.Main,
				"resource_id": p["resource_id"],
				"status":      "PROVISIONING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish(cloudevent.SubjectResponses, data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync(cloudevent.SubjectResponses)
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-001"})

		Eventually(handlerCalled, 5*time.Second).Should(Receive())

		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())
		Expect(respEvent.Type()).To(Equal("dcm.agent.creation-acknowledged"))
	})

	It("cancel message updates deny list — blocks subsequent create for same resource_id (IT-MSG-080)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		mainReceived := make(chan string, 5)
		cancelReceived := make(chan string, 5)
		denied := &sync.Map{}

		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var payload map[string]string
			_ = json.Unmarshal(event.Data(), &payload)
			denied.Store(payload["resource_id"], struct{}{})
			cancelReceived <- payload["resource_id"]
			return nil
		})
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var payload map[string]string
			_ = json.Unmarshal(event.Data(), &payload)
			if _, found := denied.Load(payload["resource_id"]); found {
				return nil
			}
			mainReceived <- payload["resource_id"]
			return nil
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		// Cancel res-001
		publishCE(ctx, testJS, topics.Cancel, cloudevent.TypeRequestCancel, "dcm/control-plane",
			map[string]string{"resource_id": "res-001"})

		Eventually(cancelReceived, 5*time.Second).Should(Receive(Equal("res-001")))

		// Create for same resource_id — should be filtered by deny list
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-001"})

		// Create for different resource_id — should go through (positive control)
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-002"})

		// res-002 should arrive but res-001 should be filtered
		Eventually(mainReceived, 5*time.Second).Should(Receive(Equal("res-002")))
		Consistently(mainReceived, 2*time.Second).ShouldNot(Receive(Equal("res-001")))
	})

	It("drains cancel topic before processing main topic (IT-MSG-090)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		// Publish cancel for "res-cancel-1" and main for "res-main-1" (different IDs to avoid deny-list)
		// before the client (and thus its durable consumers) exists — the CP
		// requests stream already exists (suite-level), so these messages sit
		// pending until a consumer with a matching FilterSubject is created.
		publishCE(ctx, testJS, topics.Cancel, cloudevent.TypeRequestCancel, "dcm/control-plane",
			map[string]string{"resource_id": "res-cancel-1"})
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-main-1"})

		// Start client — cancel must be processed before main
		order := make(chan string, 10)
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error {
			order <- "cancel"
			return nil
		})
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			order <- "main"
			return nil
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		// Verify ordering: cancel processed before main
		var first, second string
		Eventually(order, 10*time.Second).Should(Receive(&first))
		Eventually(order, 10*time.Second).Should(Receive(&second))
		Expect(first).To(Equal("cancel"))
		Expect(second).To(Equal("main"))
	})

	It("drain completes within 5s timeout — main processing begins after (IT-MSG-095)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		// Pre-populate cancel messages
		for i := 0; i < 5; i++ {
			publishCE(ctx, testJS, topics.Cancel, cloudevent.TypeRequestCancel, "dcm/control-plane",
				map[string]string{"resource_id": fmt.Sprintf("res-drain-%d", i)})
		}

		// Publish a main message with distinct resource_id (won't be in deny list)
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-main-not-cancelled"})

		mainProcessed := make(chan struct{}, 1)
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error {
			return nil
		})
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			mainProcessed <- struct{}{}
			return nil
		})

		startTime := time.Now()
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		// Main processing must begin within drain timeout (5s) + some margin
		Eventually(mainProcessed, 7*time.Second).Should(Receive())
		Expect(time.Since(startTime)).To(BeNumerically("<=", 7*time.Second))
	})

	It("DeferConsume holds off live consumption until StartConsuming is called (IT-MSG-130)", func() {
		cfg := messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "test-agent",
			DeferConsume: true,
		}

		client := messaging.NewClient(cfg, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		// Durable consumers must exist immediately (a restart-drain caller,
		// e.g. retry.Processor.ProcessOnRestart, needs to Fetch from them)...
		mainCons, err := testJS.Consumer(ctx, messaging.RequestStreamName, topics.MainConsumer())
		Expect(err).NotTo(HaveOccurred())

		// ...but publishing to main must NOT be picked up by any live
		// Consume() loop while consumption is deferred: fetch it ourselves
		// (simulating a restart-drain's raw Fetch, race-free because no
		// Consume() is running yet) instead of a handler callback.
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-deferred-1"})

		Consistently(func() (uint64, error) {
			info, infoErr := mainCons.Info(ctx)
			if infoErr != nil {
				return 0, infoErr
			}
			return info.NumPending, nil
		}, 1*time.Second, 100*time.Millisecond).Should(Equal(uint64(1)),
			"message must remain pending — no live Consume() should claim it before StartConsuming")

		mainCalled := make(chan struct{}, 1)
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			mainCalled <- struct{}{}
			return nil
		})
		client.StartConsuming()

		Eventually(mainCalled, 5*time.Second).Should(Receive(),
			"StartConsuming must begin live consumption of the previously-pending message")
	})

	It("onSetupReady fires against real JetStream and StartConsuming from inside it begins live consumption (IT-MSG-131)", func() {
		// Wires the callback exactly like main.go does (drain-equivalent
		// work, then StartConsuming, both synchronously) against a real
		// NATS/JetStream server.
		cfg := messaging.ClientConfig{
			URL: testNATSServer.ClientURL(), TopicName: topicName, AgentName: "test-agent",
			DeferConsume: true,
		}
		client := messaging.NewClient(cfg, logger)

		mainCalled := make(chan struct{}, 1)
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			mainCalled <- struct{}{}
			return nil
		})
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })

		onSetupReadyFired := make(chan struct{}, 1)
		client.SetOnSetupReady(func() {
			onSetupReadyFired <- struct{}{}
			client.StartConsuming()
		})

		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		Eventually(onSetupReadyFired, 5*time.Second).Should(Receive(),
			"onSetupReady must fire once setupStreamsAndConsume has created real JetStream streams/durable consumers")

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-onsetupready-1"})

		Eventually(mainCalled, 5*time.Second).Should(Receive(),
			"StartConsuming called from within onSetupReady must actually begin live consumption against the real durable consumer")
	})

	It("extracts resource_id from nested CE payload — struct ignores extra fields (IT-MSG-071)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		handlerCalled := make(chan []byte, 5)
		cancelProcessed := make(chan struct{}, 1)
		denied := &sync.Map{}
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]any
			_ = json.Unmarshal(event.Data(), &p)
			if id, ok := p["resource_id"].(string); ok {
				denied.Store(id, struct{}{})
			}
			cancelProcessed <- struct{}{}
			return nil
		})
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]any
			_ = json.Unmarshal(event.Data(), &p)
			id, _ := p["resource_id"].(string)
			if _, found := denied.Load(id); found {
				return nil
			}
			handlerCalled <- msg
			respEvent := cloudevents.NewEvent()
			respEvent.SetID(uuid.New().String())
			respEvent.SetSource("dcm/agents/test-agent")
			respEvent.SetType("dcm.agent.creation-acknowledged")
			respEvent.SetTime(time.Now())
			_ = respEvent.SetData(cloudevents.ApplicationJSON, map[string]any{
				"agent_name":  "test-agent",
				"topic_name":  topics.Main,
				"resource_id": id,
				"status":      "PROVISIONING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish(cloudevent.SubjectResponses, data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync(cloudevent.SubjectResponses)
		Expect(err).NotTo(HaveOccurred())

		// Nested payload — resource_id at top level, extra nested object
		nestedPayload := map[string]any{
			"resource_id": "res-nested",
			"spec":        map[string]any{"replicas": 3, "image": "nginx:latest"},
		}
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane", nestedPayload)

		Eventually(handlerCalled, 5*time.Second).Should(Receive())

		// Verify response CE contains the extracted resource_id
		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())
		var respPayload map[string]any
		Expect(json.Unmarshal(respEvent.Data(), &respPayload)).To(Succeed())
		Expect(respPayload["resource_id"]).To(Equal("res-nested"))

		// Also verify nested cancel populates deny list
		cancelPayload := map[string]any{
			"resource_id": "res-nested-cancel",
			"metadata":    map[string]any{"reason": "user-requested"},
		}
		publishCE(ctx, testJS, topics.Cancel, cloudevent.TypeRequestCancel, "dcm/control-plane", cancelPayload)

		// Wait for cancel handler to confirm processing (no sleep)
		Eventually(cancelProcessed, 5*time.Second).Should(Receive())

		// Main message for cancelled resource_id should be filtered
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-nested-cancel"})

		// Positive control — different resource_id goes through
		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-not-cancelled"})

		Eventually(handlerCalled, 5*time.Second).Should(Receive())
		Consistently(func() int { return len(handlerCalled) }, 1*time.Second).Should(Equal(0))
	})
})

var _ = Describe("Connection Resilience", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		topicName string
		logger    *slog.Logger
	)

	BeforeEach(func() {
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern
	})

	AfterEach(func() {
		cancel()
	})

	It("HTTP health returns 200 without NATS — IsConnected false→true on reconnect (IT-MSG-100)", func() {
		// Use a dedicated port for this test's NATS server lifecycle
		const reconnectPort = 14222
		reconnectURL := fmt.Sprintf("nats://127.0.0.1:%d", reconnectPort)

		// Create client pointing to not-yet-started NATS server
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       reconnectURL,
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		setNoopHandlers(client)

		// Start must NOT block — non-blocking per REQ-MSG-110
		startDone := make(chan error, 1)
		go func() { startDone <- client.Start(ctx) }()
		Eventually(startDone, 5*time.Second).Should(Receive(Not(HaveOccurred())))

		// IsConnected must be false when NATS is unreachable
		Expect(client.IsConnected()).To(BeFalse())

		// Start a NATS server on the expected port — client should auto-reconnect
		opts := natstest.DefaultTestOptions
		opts.Port = reconnectPort
		opts.JetStream = true
		tmpDir, err := os.MkdirTemp("", "nats-reconnect-*")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = os.RemoveAll(tmpDir) }()
		opts.StoreDir = tmpDir
		reconnectServer := natstest.RunServer(&opts)
		defer reconnectServer.Shutdown()

		// Pre-create the CP-owned request stream so the client's async
		// consumer-creation backoff (F2) isn't the bottleneck for this test.
		createRequestStream(reconnectURL)

		// IsConnected should transition to true after reconnection
		Eventually(func() bool { return client.IsConnected() }, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

		client.Stop()
	})

	It("processes messages after delayed NATS start — setup completes on connect (IT-MSG-105)", func() {
		const reconnectPort = 14222
		reconnectURL := fmt.Sprintf("nats://127.0.0.1:%d", reconnectPort)

		topics := messaging.DeriveTopicNames("test-agent", topicName)
		mainReceived := make(chan string, 5)
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       reconnectURL,
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]any
			_ = json.Unmarshal(event.Data(), &p)
			if id, ok := p["resource_id"].(string); ok {
				mainReceived <- id
			}
			return nil
		})
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })

		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		Expect(client.IsConnected()).To(BeFalse())

		// Start NATS server — client connects and runs setup (streams+consumers)
		opts := natstest.DefaultTestOptions
		opts.Port = reconnectPort
		opts.JetStream = true
		tmpDir, err := os.MkdirTemp("", "nats-setup-recovery-*")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = os.RemoveAll(tmpDir) }()
		opts.StoreDir = tmpDir
		reconnectServer := natstest.RunServer(&opts)
		defer reconnectServer.Shutdown()

		// Pre-create the CP-owned request stream (see IT-MSG-100 comment).
		createRequestStream(reconnectURL)

		Eventually(func() bool { return client.IsConnected() }, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

		// Connect a separate client to publish a test message
		pubConn, err := nats.Connect(reconnectURL)
		Expect(err).NotTo(HaveOccurred())
		defer pubConn.Close()
		pubJS, err := jetstream.New(pubConn)
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, pubJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-setup-recovery"})

		Eventually(mainReceived, 10*time.Second).Should(Receive(Equal("res-setup-recovery")))
	})

	It("creates consumers once the CP request stream appears mid-retry (IT-MSG-107)", func() {
		// NATS is already up; only RequestStreamName (CP-owned) is missing,
		// exercising createRequestConsumer's inner retry loop (F2). Uses a
		// dynamic port to avoid reuse races with IT-MSG-100/105's hardcoded one.
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		tmpDir, err := os.MkdirTemp("", "nats-stream-race-*")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = os.RemoveAll(tmpDir) }()
		opts.StoreDir = tmpDir
		server := natstest.RunServer(&opts)
		defer server.Shutdown()
		reconnectURL := server.ClientURL()

		topics := messaging.DeriveTopicNames("test-agent", topicName)
		mainReceived := make(chan string, 5)
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       reconnectURL,
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]any
			_ = json.Unmarshal(event.Data(), &p)
			if id, ok := p["resource_id"].(string); ok {
				mainReceived <- id
			}
			return nil
		})
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })

		// Start's first setup attempt blocks synchronously on
		// createRequestConsumer's inner retry (up to requestStreamRetryTimeout)
		// since the CP stream doesn't exist yet — run it on a goroutine so this
		// test can create the stream *while* that attempt is still retrying,
		// rather than only after Start returns.
		startDone := make(chan error, 1)
		go func() { startDone <- client.Start(ctx) }()
		defer client.Stop()

		// Give the retry loop a couple of iterations to actually run against
		// the missing stream before it appears, so this genuinely exercises
		// the retry path rather than winning a race against a fast first
		// attempt.
		time.Sleep(3 * time.Second)

		createRequestStream(reconnectURL)

		// Start's synchronous first attempt should now succeed (within
		// requestStreamRetryInterval of the stream appearing), well before
		// its requestStreamRetryTimeout bound.
		Eventually(startDone, 30*time.Second).Should(Receive(Not(HaveOccurred())))

		pubConn, err := nats.Connect(reconnectURL)
		Expect(err).NotTo(HaveOccurred())
		defer pubConn.Close()
		pubJS, err := jetstream.New(pubConn)
		Expect(err).NotTo(HaveOccurred())

		// Publishing only needs the stream to exist (already true) — it
		// doesn't need to wait for the agent's consumer, which is still
		// catching up in the background (bounded by
		// requestStreamRetryInterval). The message just sits in the stream
		// until the consumer is created and starts pulling.
		publishCE(ctx, pubJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-stream-race"})

		Eventually(mainReceived, 15*time.Second).Should(Receive(Equal("res-stream-race")))
	})
})

// createRequestStream connects to the given NATS URL and creates the
// (simulated CP-owned) dcm-agent-requests stream, matching the shared
// suite-level setup used against testNATSServer.
func createRequestStream(url string) {
	conn, err := nats.Connect(url)
	Expect(err).NotTo(HaveOccurred())
	defer conn.Close()
	js, err := jetstream.New(conn)
	Expect(err).NotTo(HaveOccurred())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: messaging.RequestStreamName, Subjects: []string{"dcm.agent.>"},
	})
	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("Acknowledgment", Label("integration"), func() {
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
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("acks on nil handler return, naks on error (IT-MSG-110)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		callCount := make(chan int, 10)
		handlerBlock := make(chan struct{})
		var invocation atomic.Int32
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			current := int(invocation.Add(1))
			callCount <- current
			if current == 1 {
				<-handlerBlock
				return fmt.Errorf("simulated error")
			}
			return nil // Second invocation succeeds (ack)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		publishCE(ctx, testJS, topics.Main, "dcm.test.ack", "dcm/test",
			map[string]string{"key": "ack-test"})

		// First delivery — handler is blocked (message in-flight, unacknowledged)
		Eventually(callCount, 5*time.Second).Should(Receive(Equal(1)))

		// Unblock handler — returns error → nak → JetStream redelivers
		close(handlerBlock)

		// Second delivery after nak — handler returns nil → ack
		Eventually(callCount, 10*time.Second).Should(Receive(Equal(2)))
	})
})

var _ = Describe("CloudEvent Correlation", Label("integration"), func() {
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
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("response CE conforms to CloudEvents v1.0 with agent_name and topic_name in data (IT-MSG-120)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]string
			_ = json.Unmarshal(event.Data(), &p)
			respEvent := cloudevents.NewEvent()
			respEvent.SetID(uuid.New().String())
			respEvent.SetSource("dcm/agents/test-agent")
			respEvent.SetType("dcm.agent.creation-acknowledged")
			respEvent.SetTime(time.Now())
			_ = respEvent.SetData(cloudevents.ApplicationJSON, map[string]any{
				"agent_name":  "test-agent",
				"topic_name":  topics.Main,
				"resource_id": p["resource_id"],
				"status":      "PROVISIONING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish(cloudevent.SubjectResponses, data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync(cloudevent.SubjectResponses)
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-corr"})

		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())

		// CloudEvents v1.0 compliance
		Expect(respEvent.SpecVersion()).To(Equal("1.0"))
		Expect(respEvent.ID()).NotTo(BeEmpty())
		Expect(respEvent.Source()).NotTo(BeEmpty())
		Expect(respEvent.Type()).NotTo(BeEmpty())
		Expect(respEvent.Time().IsZero()).To(BeFalse())

		// Correlation fields in data
		var payload map[string]interface{}
		Expect(json.Unmarshal(respEvent.Data(), &payload)).To(Succeed())
		Expect(payload).To(HaveKey("agent_name"))
		Expect(payload).To(HaveKey("topic_name"))
		Expect(payload["agent_name"]).To(Equal("test-agent"))
		Expect(payload["topic_name"]).To(Equal(topics.Main))
	})

	It("delete request produces deletion-acknowledged response with DELETING status (IT-MSG-072)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var p map[string]string
			_ = json.Unmarshal(event.Data(), &p)
			respEvent := cloudevents.NewEvent()
			respEvent.SetID(uuid.New().String())
			respEvent.SetSource("dcm/agents/test-agent")
			respEvent.SetType("dcm.agent.deletion-acknowledged")
			respEvent.SetTime(time.Now())
			_ = respEvent.SetData(cloudevents.ApplicationJSON, map[string]any{
				"agent_name":  "test-agent",
				"topic_name":  topics.Main,
				"resource_id": p["resource_id"],
				"status":      "DELETING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish(cloudevent.SubjectResponses, data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync(cloudevent.SubjectResponses)
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestDelete, "dcm/control-plane",
			map[string]string{"resource_id": "res-del-001"})

		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())
		Expect(respEvent.Type()).To(Equal("dcm.agent.deletion-acknowledged"))

		var respPayload map[string]interface{}
		Expect(json.Unmarshal(respEvent.Data(), &respPayload)).To(Succeed())
		Expect(respPayload["status"]).To(Equal("DELETING"))
		Expect(respPayload["resource_id"]).To(Equal("res-del-001"))
		Expect(respPayload["agent_name"]).To(Equal("test-agent"))
		Expect(respPayload["topic_name"]).To(Equal(topics.Main))
	})

	It("handler failure causes nak and redelivery (IT-MSG-073)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		var deliveryCount atomic.Int32
		client := messaging.NewClient(cfg, logger)
		client.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client.SetMainHandler(func(_ context.Context, _ []byte) error {
			count := deliveryCount.Add(1)
			if count <= 2 {
				return fmt.Errorf("simulated handler failure")
			}
			return nil
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		publishCE(ctx, testJS, topics.Main, cloudevent.TypeRequestCreate, "dcm/control-plane",
			map[string]string{"resource_id": "res-nak-001"})

		// Message should be redelivered because handler returns error
		Eventually(deliveryCount.Load, 10*time.Second, 100*time.Millisecond).
			Should(BeNumerically(">=", int32(2)))
	})
})

var _ = Describe("Lifecycle Logging", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		topics    messaging.TopicNames
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		topics = messaging.DeriveTopicNames("test-agent", topicName)
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

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

	It("logs messaging ready on consumption start and messaging stopped on shutdown (IT-MSG-172)", func() {
		ch := &captureHandler{}
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}, slog.New(ch))
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())

		var readyRec slog.Record
		Eventually(func() bool {
			var ok bool
			readyRec, ok = findRecord(ch.all(), "messaging ready, main/cancel consumption started")
			return ok
		}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
		Expect(readyRec.Level).To(Equal(slog.LevelInfo))

		client.Stop()

		stoppedRec, ok := findRecord(ch.all(), "messaging stopped")
		Expect(ok).To(BeTrue())
		Expect(stoppedRec.Level).To(Equal(slog.LevelInfo))
	})
})
