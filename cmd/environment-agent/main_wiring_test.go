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

// This test exercises main.go's actual composition-root wiring end to end —
// unlike UT-SPR-100/101 in internal/provider/service, which construct
// ProviderService/monitor.Monitor directly and prove the general ordering
// property in isolation. Those unit tests give no regression protection
// against main.go itself accidentally reverting to the buggy ordering
// (SetOnTransition wired after RegisterEmbedded): they'd keep passing either
// way, since they never touch main.go's run() at all.
//
// A real embedded NATS/JetStream server is required here because the only
// externally-observable side effect of the embedded SP's synchronous
// Ready->Unhealthy transition (forced via AGENT_EMBEDDED_SP_WIDGET_HEALTH)
// reaching healthMonitor's onTransition callback is a
// dcm.agent.health.service-type-degraded CloudEvent published to
// dcm.agents.health (health.CEPublisher.OnTransition) — the other two
// effects wired into that same callback either don't fire for a plain
// Unhealthy transition (registrar.NotifyServiceTypeChange only fires for
// Unavailable-involving transitions) or aren't independently observable from
// outside run() (retryProcessor.RunTransition).
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
		// dcm-agent-requests: control-plane-owned stream (F2) that
		// messaging.Client.Start needs before it will consider setup
		// complete and fire onSetupReady/populate c.js. The agent creates
		// its own "dcm-health" stream for the health CE subject internally
		// (initInternalStreams) — must not be pre-created here too, or
		// JetStream rejects it with "subjects overlap with an existing
		// stream".
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

		// Independent subscriber, not reusing any of run()'s own internals,
		// so it observes exactly what a real NATS consumer of the health
		// subject would see.
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
		// Unconditional cleanup (not just on the happy path at the bottom of
		// this It): if the Eventually below fails, Ginkgo's Fail panics
		// before reaching that point, and without this defer the agent's
		// run(ctx) goroutine — HTTP listener, health monitor, registrar,
		// messaging client all included — would keep running for the rest
		// of the test binary's lifetime instead of being torn down.
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
