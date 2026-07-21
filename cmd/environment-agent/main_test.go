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

var _ = Describe("run", func() {
	It("exits cleanly on cancelled context", func() {
		Expect(os.Setenv("AGENT_SERVER_ADDRESS", ":0")).To(Succeed())
		DeferCleanup(os.Unsetenv, "AGENT_SERVER_ADDRESS")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(run(ctx)).To(Equal(0))
	})
})
