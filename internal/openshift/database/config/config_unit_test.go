package config_test

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/openshift/database/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

var _ = Describe("Configuration", func() {
	agentDefaults := shared.Agent{MessagingURL: "nats://test:4222"}

	clearEnv := func() {
		_ = os.Unsetenv("SP_NAME")
		_ = os.Unsetenv("SP_DATABASE_NAMESPACE")
		_ = os.Unsetenv("SP_KUBECONFIG")
		_ = os.Unsetenv("SP_DATABASE_DEFAULT_STORAGE_CLASS")
		_ = os.Unsetenv("SP_DATABASE_IMAGE_CATALOG")
		_ = os.Unsetenv("SP_DATABASE_EXTERNAL_SVC_TYPE")
		_ = os.Unsetenv("SP_DATABASE_DEFAULT_VERSION")
		_ = os.Unsetenv("SP_MONITOR_DEBOUNCE_MS")
		_ = os.Unsetenv("SP_MONITOR_RESYNC_PERIOD")
		_ = os.Unsetenv("SP_MONITOR_PUBLISH_MAX_ATTEMPTS")
	}

	BeforeEach(func() {
		clearEnv()
	})

	AfterEach(func() {
		clearEnv()
	})

	It("loads configuration from environment vaiables", func() {
		_ = os.Setenv("SP_NAME", "test-sp")
		_ = os.Setenv("SP_DATABASE_EXTERNAL_SVC_TYPE", "LoadBalancer")
		_ = os.Setenv("SP_DATABASE_DEFAULT_STORAGE_CLASS", "az-a")
		_ = os.Setenv("SP_DATABASE_IMAGE_CATALOG", "psql-images")
		_ = os.Setenv("SP_DATABASE_DEFAULT_VERSION", "16")
		_ = os.Setenv("SP_MONITOR_DEBOUNCE_MS", "250")
		_ = os.Setenv("SP_MONITOR_RESYNC_PERIOD", "5m")
		_ = os.Setenv("SP_MONITOR_PUBLISH_MAX_ATTEMPTS", "10")

		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())

		Expect(cfg.Name).To(Equal("test-sp"))
		Expect(cfg.ExternalServiceType).To(Equal("LoadBalancer"))
		Expect(cfg.DefaultStorageClass).To(Equal("az-a"))
		Expect(cfg.ImageCatalog).To(Equal("psql-images"))
		Expect(cfg.DefaultVersion).To(Equal(16))
		Expect(cfg.DebounceMs).To(Equal(250))
		Expect(cfg.ResyncPeriod).To(Equal(5 * time.Minute))
		Expect(cfg.PublishMaxAttempts).To(Equal(10))
	})

	It("applies default values when no config is specified", func() {
		_ = os.Setenv("SP_DATABASE_EXTERNAL_SVC_TYPE", "NodePort")

		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg).NotTo(BeNil())

		Expect(cfg.Namespace).To(Equal("default"))
		Expect(cfg.DefaultVersion).To(Equal(18))
		Expect(cfg.DebounceMs).To(Equal(500))
		Expect(cfg.ResyncPeriod).To(Equal(10 * time.Minute))
		Expect(cfg.PublishMaxAttempts).To(Equal(5))
	})

	It("returns error when messaging URL is missing", func() {
		_ = os.Setenv("SP_DATABASE_EXTERNAL_SVC_TYPE", "NodePort")

		cfg, err := config.Load(shared.Agent{})
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())

		Expect(err.Error()).To(ContainSubstring("messaging URL is required"))
	})

	It("returns error when SP_DATABASE_EXTERNAL_SVC_TYPE is not set", func() {
		cfg, err := config.Load(agentDefaults)
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("invalid SP_DATABASE_EXTERNAL_SVC_TYPE"))
		Expect(err.Error()).To(ContainSubstring("must be LoadBalancer or NodePort"))
	})

	It("loads successfully with ExternalServiceType=LoadBalancer", func() {
		_ = os.Setenv("SP_DATABASE_EXTERNAL_SVC_TYPE", "LoadBalancer")

		cfg, err := config.Load(agentDefaults)
		Expect(err).ToNot(HaveOccurred())
		Expect(cfg.ExternalServiceType).To(Equal("LoadBalancer"))
	})

	It("loads successfully with ExternalServiceType=NodePort", func() {
		_ = os.Setenv("SP_DATABASE_EXTERNAL_SVC_TYPE", "NodePort")

		cfg, err := config.Load(agentDefaults)
		Expect(err).ToNot(HaveOccurred())
		Expect(cfg.ExternalServiceType).To(Equal("NodePort"))
	})
	It("rejects invalid SP_DATABASE_EXTERNAL_SVC_TYPE values", func() {
		_ = os.Setenv("SP_DATABASE_EXTERNAL_SVC_TYPE", "ClusterIP")

		cfg, err := config.Load(agentDefaults)
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("invalid SP_DATABASE_EXTERNAL_SVC_TYPE"))
		Expect(err.Error()).To(ContainSubstring("must be LoadBalancer or NodePort"))
	})
})
