package network_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded/network"
)

var _ = Describe("Enabled", Label("unit"), func() {
	It("returns true when network is listed in AGENT_EMBEDDED_SPS", func() {
		Expect(network.Enabled([]string{"container", "network"})).To(BeTrue())
	})

	It("returns false when network is not listed", func() {
		Expect(network.Enabled([]string{"container", "cluster"})).To(BeFalse())
		Expect(network.Enabled(nil)).To(BeFalse())
	})
})
