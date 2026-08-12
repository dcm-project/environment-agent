package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// This test exercises main.go's actual composition-root wiring end to end,
// unlike UT-SPR-100/101 which construct ProviderService/monitor.Monitor
// directly. A real embedded NATS/JetStream server is required because the
// only externally-observable effect of the embedded SP's synchronous
// Ready->Unhealthy transition is a health CE published to dcm.agents.health
// (health.CEPublisher.OnTransition).
var _ = Describe("run wiring: embedded SP transitions during RegisterEmbedded", Label("integration"), func() {
	var (
		testNATSServer *natsserver.Server
		storeDir       string
	)

	BeforeEach(func() {
		var err error
		storeDir, err = os.MkdirTemp("", "m9-nats-test-*")
		Expect(err).NotTo(HaveOccurred())

		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = storeDir
		testNATSServer = natstest.RunServer(&opts)

		conn, err := nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		js, err := jetstream.New(conn)
		Expect(err).NotTo(HaveOccurred())
		setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// dcm-agent-requests: control-plane-owned stream (REQ-MSG-048) that
		// messaging.Client.Start needs before setup completes. Must not
		// also pre-create "dcm-health" (the agent creates that one itself
		// in initInternalStreams), or JetStream rejects the subject overlap.
		_, err = js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
			Name: messaging.RequestStreamName, Subjects: []string{"dcm.agent.>"},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if testNATSServer != nil {
			testNATSServer.Shutdown()
		}
		if storeDir != "" {
			_ = os.RemoveAll(storeDir)
		}
	})

	It("publishes a health CE for the embedded SP's synchronous initialCheck transition (IT-SPR-172)", func() {
		GinkgoT().Setenv("AGENT_SERVER_ADDRESS", ":0")
		GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", GinkgoT().TempDir()+"/registrations.json")
		GinkgoT().Setenv("AGENT_NAME", "m9-test-agent")
		GinkgoT().Setenv("AGENT_ENVIRONMENT", "test")
		GinkgoT().Setenv("AGENT_COST", "medium")
		GinkgoT().Setenv("DCM_REGISTRATION_URL", "http://localhost:8080")
		GinkgoT().Setenv("AGENT_MESSAGING_URL", testNATSServer.ClientURL())
		GinkgoT().Setenv("AGENT_EMBEDDED_SPS", "widget")
		GinkgoT().Setenv("AGENT_EMBEDDED_SP_WIDGET_HEALTH", "unhealthy")
		GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "1")

		// Independent subscriber, not reusing run()'s internals, so it
		// observes exactly what a real NATS consumer would see.
		sub, err := nats.Connect(testNATSServer.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		defer sub.Close()
		healthCh := make(chan *nats.Msg, 4)
		subscription, err := sub.ChanSubscribe(cloudevent.SubjectHealth, healthCh)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = subscription.Unsubscribe() }()

		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan int, 1)
		go func() { runDone <- run(ctx) }()
		// Deferred (not just at the bottom): Ginkgo's Fail panics on an
		// Eventually failure, and without this the run(ctx) goroutine would
		// keep running for the rest of the test binary's lifetime.
		defer func() {
			cancel()
			Eventually(runDone, 5*time.Second).Should(Receive())
		}()

		var msg *nats.Msg
		Eventually(healthCh, 10*time.Second, 50*time.Millisecond).Should(Receive(&msg),
			"a health CE must be published for the embedded SP's Ready->Unhealthy transition "+
				"that fires synchronously during RegisterEmbedded's initialCheck — if main.go's "+
				"SetOnTransition/SetOnChange wiring were reverted to run AFTER RegisterEmbedded, "+
				"this transition is silently dropped and no CE is ever published, "+
				"so this Eventually would time out")

		var ce cloudevents.Event
		Expect(json.Unmarshal(msg.Data, &ce)).To(Succeed())
		Expect(ce.Type()).To(Equal(cloudevent.TypeHealthDegraded))

		var data routing.HealthEventData
		Expect(json.Unmarshal(ce.Data(), &data)).To(Succeed())
		Expect(data.ServiceType).To(Equal("widget"))
	})
})
