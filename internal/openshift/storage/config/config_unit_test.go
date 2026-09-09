package config_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/config"
)

var _ = Describe("Configuration", func() {
	agentDefaults := shared.Agent{MessagingURL: "nats://test:4222"}

	clearEnv := func() {
		_ = os.Unsetenv("SP_NAME")
		_ = os.Unsetenv("SP_STORAGE_NAMESPACE")
		_ = os.Unsetenv("SP_KUBECONFIG")
		_ = os.Unsetenv("SP_K8S_DEFAULT_ACCESS_MODE")
	}

	BeforeEach(func() {
		clearEnv()
	})

	AfterEach(func() {
		clearEnv()
	})

	It("uses SP_STORAGE_NAMESPACE when set", func() {
		_ = os.Setenv("SP_STORAGE_NAMESPACE", "storage-ns")

		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Namespace).To(Equal("storage-ns"))
	})

	It("defaults namespace to default when SP_STORAGE_NAMESPACE is unset", func() {
		cfg, err := config.Load(agentDefaults)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Namespace).To(Equal("default"))
	})
})
