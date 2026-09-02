package config_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/openshift/network/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

var _ = Describe("Configuration", Label("unit"), func() {
	clearEnv := func() {
		_ = os.Unsetenv("SP_NAME")
		_ = os.Unsetenv("SP_KUBECONFIG")
		_ = os.Unsetenv("SP_K8S_NAMESPACE")
	}

	BeforeEach(func() {
		clearEnv()
	})

	AfterEach(func() {
		clearEnv()
	})

	It("loads configuration with agent defaults", func() {
		agent := shared.Agent{
			MessagingURL: "nats://localhost:4222",
			Kubeconfig:   "/test/.kube/config",
		}

		cfg, err := config.Load(agent)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Name).To(Equal("network"))
		Expect(cfg.Namespace).To(Equal("default"))
		Expect(cfg.Kubeconfig).To(Equal("/test/.kube/config"))
		Expect(cfg.MessagingURL).To(Equal("nats://localhost:4222"))
	})

	It("applies namespace from environment", func() {
		_ = os.Setenv("SP_K8S_NAMESPACE", "production")
		agent := shared.Agent{
			MessagingURL: "nats://localhost:4222",
		}

		cfg, err := config.Load(agent)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Namespace).To(Equal("production"))
	})

	It("allows SP_NAME override", func() {
		_ = os.Setenv("SP_NAME", "custom-network")
		agent := shared.Agent{
			MessagingURL: "nats://localhost:4222",
		}

		cfg, err := config.Load(agent)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Name).To(Equal("custom-network"))
	})

	It("returns error when messaging URL is missing", func() {
		agent := shared.Agent{} // No MessagingURL

		cfg, err := config.Load(agent)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("messaging URL is required"))
		Expect(cfg).To(BeNil())
	})
})
