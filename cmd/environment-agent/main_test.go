package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main Suite")
}

var _ = Describe("run", Label("unit"), func() {
	It("exits cleanly on cancelled context", func() {
		GinkgoT().Setenv("AGENT_SERVER_ADDRESS", ":0")

		GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", GinkgoT().TempDir()+"/registrations.json")
		GinkgoT().Setenv("AGENT_NAME", "test-agent")
		GinkgoT().Setenv("AGENT_ENVIRONMENT", "test")
		GinkgoT().Setenv("AGENT_COST", "medium")
		GinkgoT().Setenv("DCM_REGISTRATION_URL", "http://localhost:8080")
		GinkgoT().Setenv("AGENT_MESSAGING_URL", "nats://localhost:4222")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(run(ctx)).To(Equal(0))
	})

	// run() calls net.Listen before LoadPersisted, so the TCP port is
	// briefly bound even though Serve() never runs on corrupt persisted
	// data; this test proves that window never becomes observable to a
	// client (AC-SPR-109).
	It("fails fast on corrupt persisted data without ever serving HTTP (IT-SPR-171)", func() {
		// Reserve a free port synchronously to know the exact address to
		// probe after run() returns (run() does its own net.Listen).
		probeLn, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		addr := probeLn.Addr().String()
		Expect(probeLn.Close()).To(Succeed())

		GinkgoT().Setenv("AGENT_SERVER_ADDRESS", addr)

		persistPath := GinkgoT().TempDir() + "/registrations.json"
		Expect(os.WriteFile(persistPath, []byte("{not valid json"), 0o600)).To(Succeed())
		GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", persistPath)
		GinkgoT().Setenv("AGENT_NAME", "test-agent")
		GinkgoT().Setenv("AGENT_ENVIRONMENT", "test")
		GinkgoT().Setenv("AGENT_COST", "medium")
		GinkgoT().Setenv("DCM_REGISTRATION_URL", "http://localhost:8080")
		GinkgoT().Setenv("AGENT_MESSAGING_URL", "nats://localhost:4222")

		Expect(run(context.Background())).To(Equal(1),
			"run() must fail fast (non-zero exit) on corrupt persisted data")

		client := &http.Client{Timeout: 500 * time.Millisecond}
		_, getErr := client.Get("http://" + addr + "/api/v1alpha1/health")
		Expect(getErr).To(HaveOccurred(),
			"the HTTP server must never have started serving on the address run() bound — "+
				"a successful GET here would mean Serve() ran despite the fail-fast LoadPersisted error")
	})

	// Regression test for a gap found during review: ValidateTopicName alone
	// accepts dots (they're valid subject tokens), but the same base name is
	// also used to derive JetStream stream/consumer names, which reject
	// dots — without ValidateJetStreamSafeName wired into setupMessaging,
	// this would not fail here at all; it would pass validation and then
	// hang in createRequestConsumer's 30s retry loop (REQ-MSG-051) before
	// falling into permanent, silent background retries.
	It("fails fast on a dotted topic name instead of hanging in JetStream setup retries (IT-MSG-011)", func() {
		GinkgoT().Setenv("AGENT_SERVER_ADDRESS", ":0")
		GinkgoT().Setenv("AGENT_SP_PERSISTENCE_PATH", GinkgoT().TempDir()+"/registrations.json")
		GinkgoT().Setenv("AGENT_NAME", "agent-prod.1")
		GinkgoT().Setenv("AGENT_ENVIRONMENT", "test")
		GinkgoT().Setenv("AGENT_COST", "medium")
		GinkgoT().Setenv("DCM_REGISTRATION_URL", "http://localhost:8080")
		GinkgoT().Setenv("AGENT_MESSAGING_URL", "nats://localhost:4222")

		done := make(chan int, 1)
		go func() { done <- run(context.Background()) }()

		Eventually(done, 2*time.Second, 10*time.Millisecond).Should(Receive(Equal(1)),
			"run() must fail fast on a dotted AGENT_NAME (invalid for JetStream stream/consumer "+
				"names per REQ-MSG-011) rather than ever reaching the 30s createRequestConsumer retry loop")
	})
})
