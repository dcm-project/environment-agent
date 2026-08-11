package main

import (
	"context"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main Suite")
}

var _ = Describe("run", Label("unit"), func() {
	It("exits cleanly on cancelled context", func() {
		Expect(os.Setenv("AGENT_SERVER_ADDRESS", ":0")).To(Succeed())
		DeferCleanup(os.Unsetenv, "AGENT_SERVER_ADDRESS")

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
})
