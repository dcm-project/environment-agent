package config_test

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/openshift/container/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

var _ = Describe("Configuration", func() {
	agentDefaults := shared.Agent{MessagingURL: "nats://test:4222"}

	clearEnv := func() {
		_ = os.Unsetenv("SP_NAME")
		_ = os.Unsetenv("SP_K8S_EXTERNAL_SVC_TYPE")
		_ = os.Unsetenv("SP_KUBECONFIG")
		_ = os.Unsetenv("SP_MONITOR_DEBOUNCE_MS")
		_ = os.Unsetenv("SP_MONITOR_RESYNC_PERIOD")
	}

	BeforeEach(func() {
		clearEnv()
	})

	AfterEach(func() {
		clearEnv()
	})

	It("loads configuration from environment variables", func() {
		_ = os.Setenv("SP_NAME", "test-sp")
		_ = os.Setenv("SP_K8S_EXTERNAL_SVC_TYPE", "LoadBalancer")
		_ = os.Setenv("SP_MONITOR_DEBOUNCE_MS", "250")
		_ = os.Setenv("SP_MONITOR_RESYNC_PERIOD", "5m")

		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Name).To(Equal("test-sp"))
		Expect(cfg.MessagingURL).To(Equal("nats://test:4222"))
		Expect(cfg.ExternalServiceType).To(Equal("LoadBalancer"))
		Expect(cfg.DebounceMs).To(Equal(250))
		Expect(cfg.ResyncPeriod).To(Equal(5 * time.Minute))
	})

	It("defaults provider name and monitoring settings", func() {
		_ = os.Setenv("SP_K8S_EXTERNAL_SVC_TYPE", "NodePort")

		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Name).To(Equal("container-sp"))
		Expect(cfg.DebounceMs).To(Equal(500))
		Expect(cfg.ResyncPeriod).To(Equal(10 * time.Minute))
	})

	It("returns error when messaging URL is missing", func() {
		_ = os.Setenv("SP_K8S_EXTERNAL_SVC_TYPE", "NodePort")

		cfg, err := config.Load(shared.Agent{})
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("messaging URL is required"))
	})

	It("returns error when SP_K8S_EXTERNAL_SVC_TYPE is not set", func() {
		cfg, err := config.Load(agentDefaults)
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("invalid SP_K8S_EXTERNAL_SVC_TYPE"))
	})

	It("rejects invalid SP_K8S_EXTERNAL_SVC_TYPE values", func() {
		_ = os.Setenv("SP_K8S_EXTERNAL_SVC_TYPE", "ClusterIP")

		cfg, err := config.Load(agentDefaults)
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("must be LoadBalancer or NodePort"))
	})
})
