package config_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

var _ = Describe("Configuration", func() {
	agentDefaults := shared.Agent{MessagingURL: "nats://test:4222"}

	clearEnv := func() {
		_ = os.Unsetenv("SP_NAME")
		_ = os.Unsetenv("SP_VM_NAMESPACE")
		_ = os.Unsetenv("SP_KUBECONFIG")
	}

	BeforeEach(func() {
		clearEnv()
	})

	AfterEach(func() {
		clearEnv()
	})

	It("uses SP_VM_NAMESPACE when set", func() {
		_ = os.Setenv("SP_VM_NAMESPACE", "vms")

		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Namespace).To(Equal("vms"))
	})

	It("defaults namespace to default when SP_VM_NAMESPACE is unset", func() {
		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Namespace).To(Equal("default"))
	})
})
