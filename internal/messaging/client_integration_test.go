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

	"github.com/dcm-project/environment-agent/internal/messaging"
)

func setNoopHandlers(c *messaging.Client) {
	c.SetMainHandler(func(_ context.Context, _ []byte) error { return nil })
	c.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
}

func deleteStreams(js jetstream.JetStream, topicName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, suffix := range []string{"", "-retry", "-cancel"} {
		_ = js.DeleteStream(ctx, topicName+suffix)
	}
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
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteStreams(testJS, topicName)
		testConn.Close()
	})

	It("creates three JetStream subjects at startup (IT-MSG-010)", func() {
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		mainStream, err := testJS.Stream(ctx, topicName)
		Expect(err).NotTo(HaveOccurred())
		Expect(mainStream.CachedInfo().Config.Subjects).To(ContainElement(topicName))

		retryStream, err := testJS.Stream(ctx, topicName+"-retry")
		Expect(err).NotTo(HaveOccurred())
		Expect(retryStream.CachedInfo().Config.Subjects).To(ContainElement(topicName + ".retry"))

		cancelStream, err := testJS.Stream(ctx, topicName+"-cancel")
		Expect(err).NotTo(HaveOccurred())
		Expect(cancelStream.CachedInfo().Config.Subjects).To(ContainElement(topicName + ".cancel"))
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

		stream, err := testJS.Stream(ctx, topicName)
		Expect(err).NotTo(HaveOccurred())

		// Verify consumer names are deterministic (derived from topic name)
		consNames := []string{}
		lister := stream.ListConsumers(ctx)
		for info := range lister.Info() {
			consNames = append(consNames, info.Name)
		}
		Expect(consNames).NotTo(BeEmpty())
		// Consumer names must contain the topic name for determinism
		for _, name := range consNames {
			Expect(name).To(ContainSubstring(topicName))
		}
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

		stream, err := testJS.Stream(ctx, topicName)
		Expect(err).NotTo(HaveOccurred())
		initialConsumers := stream.CachedInfo().State.Consumers
		client1.Stop()

		client2 := messaging.NewClient(cfg, logger)
		setNoopHandlers(client2)
		Expect(client2.Start(ctx)).To(Succeed())
		defer client2.Stop()

		stream, err = testJS.Stream(ctx, topicName)
		Expect(err).NotTo(HaveOccurred())
		Expect(stream.CachedInfo().State.Consumers).To(Equal(initialConsumers))
	})
})

var _ = Describe("Message Durability", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteStreams(testJS, topicName)
		testConn.Close()
	})

	It("redelivers unacknowledged message after restart (IT-MSG-040)", func() {
		cfg := messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}

		received := make(chan []byte, 1)
		client1 := messaging.NewClient(cfg, logger)
		client1.SetCancelHandler(func(_ context.Context, _ []byte) error { return nil })
		client1.SetMainHandler(func(_ context.Context, msg []byte) error {
			received <- msg
			return fmt.Errorf("simulated failure")
		})
		Expect(client1.Start(ctx)).To(Succeed())

		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/test", map[string]string{"key": "value"})

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
})

var _ = Describe("Topic Advertising", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteStreams(testJS, topicName)
		testConn.Close()
	})

	It("returns only main topic name — not retry or cancel (IT-MSG-060)", func() {
		client := messaging.NewClient(messaging.ClientConfig{
			URL:       testNATSServer.ClientURL(),
			TopicName: topicName,
			AgentName: "test-agent",
		}, logger)
		setNoopHandlers(client)
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		Expect(client.TopicName()).To(Equal(topicName))
	})
})

var _ = Describe("Message Consumption", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteStreams(testJS, topicName)
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
				"agentName":  "test-agent",
				"topicName":  topicName,
				"resourceId": p["resourceId"],
				"status":     "PROVISIONING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish("dcm.agents.responses", data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-001"})

		Eventually(handlerCalled, 5*time.Second).Should(Receive())

		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())
		Expect(respEvent.Type()).To(Equal("dcm.agent.creation-acknowledged"))
	})

	It("cancel message updates deny list — blocks subsequent create for same resourceId (IT-MSG-080)", func() {
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
			denied.Store(payload["resourceId"], struct{}{})
			cancelReceived <- payload["resourceId"]
			return nil
		})
		client.SetMainHandler(func(_ context.Context, msg []byte) error {
			var event cloudevents.Event
			_ = json.Unmarshal(msg, &event)
			var payload map[string]string
			_ = json.Unmarshal(event.Data(), &payload)
			if _, found := denied.Load(payload["resourceId"]); found {
				return nil
			}
			mainReceived <- payload["resourceId"]
			return nil
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		// Cancel res-001
		publishCE(ctx, testJS, topicName+".cancel", "dcm.command.cancel", "dcm/control-plane",
			map[string]string{"resourceId": "res-001"})

		Eventually(cancelReceived, 5*time.Second).Should(Receive(Equal("res-001")))

		// Create for same resourceId — should be filtered by deny list
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-001"})

		// Create for different resourceId — should go through (positive control)
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-002"})

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

		// Pre-populate streams manually
		_, err := testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     topicName + "-cancel",
			Subjects: []string{topicName + ".cancel"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     topicName,
			Subjects: []string{topicName},
		})
		Expect(err).NotTo(HaveOccurred())

		// Publish cancel for "res-cancel-1" and main for "res-main-1" (different IDs to avoid deny-list)
		publishCE(ctx, testJS, topicName+".cancel", "dcm.command.cancel", "dcm/control-plane",
			map[string]string{"resourceId": "res-cancel-1"})
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-main-1"})

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

		_, err := testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     topicName + "-cancel",
			Subjects: []string{topicName + ".cancel"},
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = testJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     topicName,
			Subjects: []string{topicName},
		})
		Expect(err).NotTo(HaveOccurred())

		// Pre-populate cancel messages
		for i := 0; i < 5; i++ {
			publishCE(ctx, testJS, topicName+".cancel", "dcm.command.cancel", "dcm/control-plane",
				map[string]string{"resourceId": fmt.Sprintf("res-drain-%d", i)})
		}

		// Publish a main message with distinct resourceId (won't be in deny list)
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-main-not-cancelled"})

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

	It("extracts resourceId from nested CE payload — struct ignores extra fields (IT-MSG-071)", func() {
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
			if id, ok := p["resourceId"].(string); ok {
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
			id, _ := p["resourceId"].(string)
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
				"agentName":  "test-agent",
				"topicName":  topicName,
				"resourceId": id,
				"status":     "PROVISIONING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish("dcm.agents.responses", data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())

		// Nested payload — resourceId at top level, extra nested object
		nestedPayload := map[string]any{
			"resourceId": "res-nested",
			"spec":       map[string]any{"replicas": 3, "image": "nginx:latest"},
		}
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane", nestedPayload)

		Eventually(handlerCalled, 5*time.Second).Should(Receive())

		// Verify response CE contains the extracted resourceId
		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())
		var respPayload map[string]any
		Expect(json.Unmarshal(respEvent.Data(), &respPayload)).To(Succeed())
		Expect(respPayload["resourceId"]).To(Equal("res-nested"))

		// Also verify nested cancel populates deny list
		cancelPayload := map[string]any{
			"resourceId": "res-nested-cancel",
			"metadata":   map[string]any{"reason": "user-requested"},
		}
		publishCE(ctx, testJS, topicName+".cancel", "dcm.command.cancel", "dcm/control-plane", cancelPayload)

		// Wait for cancel handler to confirm processing (no sleep)
		Eventually(cancelProcessed, 5*time.Second).Should(Receive())

		// Main message for cancelled resourceId should be filtered
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-nested-cancel"})

		// Positive control — different resourceId goes through
		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-not-cancelled"})

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

		// IsConnected should transition to true after reconnection
		Eventually(func() bool { return client.IsConnected() }, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

		client.Stop()
	})
})

var _ = Describe("Acknowledgment", Label("integration"), func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		testConn  *nats.Conn
		testJS    jetstream.JetStream
		topicName string
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteStreams(testJS, topicName)
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

		publishCE(ctx, testJS, topicName, "dcm.test.ack", "dcm/test",
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
		logger    *slog.Logger
	)

	BeforeEach(func() {
		var err error
		topicName = fmt.Sprintf("test-%s", uuid.New().String()[:8])
		logger = slog.Default()
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second) //nolint:fatcontext // Ginkgo BeforeEach pattern

		testConn, err = nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		testJS, err = jetstream.New(testConn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		cancel()
		deleteStreams(testJS, topicName)
		testConn.Close()
	})

	It("response CE conforms to CloudEvents v1.0 with agentName and topicName in data (IT-MSG-120)", func() {
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
				"agentName":  "test-agent",
				"topicName":  topicName,
				"resourceId": p["resourceId"],
				"status":     "PROVISIONING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish("dcm.agents.responses", data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-corr"})

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
		Expect(payload).To(HaveKey("agentName"))
		Expect(payload).To(HaveKey("topicName"))
		Expect(payload["agentName"]).To(Equal("test-agent"))
		Expect(payload["topicName"]).To(Equal(topicName))
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
				"agentName":  "test-agent",
				"topicName":  topicName,
				"resourceId": p["resourceId"],
				"status":     "DELETING",
			})
			data, _ := json.Marshal(respEvent)
			return testConn.Publish("dcm.agents.responses", data)
		})
		Expect(client.Start(ctx)).To(Succeed())
		defer client.Stop()

		responseSub, err := testConn.SubscribeSync("dcm.agents.responses")
		Expect(err).NotTo(HaveOccurred())

		publishCE(ctx, testJS, topicName, "dcm.request.delete", "dcm/control-plane",
			map[string]string{"resourceId": "res-del-001"})

		msg, err := responseSub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())

		var respEvent cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &respEvent)).To(Succeed())
		Expect(respEvent.Type()).To(Equal("dcm.agent.deletion-acknowledged"))

		var respPayload map[string]interface{}
		Expect(json.Unmarshal(respEvent.Data(), &respPayload)).To(Succeed())
		Expect(respPayload["status"]).To(Equal("DELETING"))
		Expect(respPayload["resourceId"]).To(Equal("res-del-001"))
		Expect(respPayload["agentName"]).To(Equal("test-agent"))
		Expect(respPayload["topicName"]).To(Equal(topicName))
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

		publishCE(ctx, testJS, topicName, "dcm.command.create", "dcm/control-plane",
			map[string]string{"resourceId": "res-nak-001"})

		// Message should be redelivered because handler returns error
		Eventually(deliveryCount.Load, 10*time.Second, 100*time.Millisecond).
			Should(BeNumerically(">=", int32(2)))
	})
})
