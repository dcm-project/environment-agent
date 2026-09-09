package config_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/openshift/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

var _ = Describe("Config", func() {
	requiredVars := map[string]string{
		"SP_CLUSTER_NAMESPACE": "clusters",
		"SP_PULL_SECRET":       "eyJhdXRocyI6e319",
	}

	setAllRequired := func() {
		for k, v := range requiredVars {
			GinkgoT().Setenv(k, v)
		}
	}

	agentDefaults := func() shared.Agent {
		return shared.Agent{MessagingURL: "nats://localhost:4222"}
	}

	DescribeTable("required config missing causes fail-fast",
		func(missingVar string) {
			for k, v := range requiredVars {
				if k != missingVar {
					GinkgoT().Setenv(k, v)
				}
			}
			GinkgoT().Setenv(missingVar, "")
			Expect(os.Unsetenv(missingVar)).To(Succeed())

			_, err := config.Load(agentDefaults())
			Expect(err).To(HaveOccurred(), "Load() should fail when %s is missing", missingVar)
		},
		Entry("SP_CLUSTER_NAMESPACE missing", "SP_CLUSTER_NAMESPACE"),
		Entry("SP_PULL_SECRET missing", "SP_PULL_SECRET"),
	)

	It("applies defaults when optional vars are not set", func() {
		setAllRequired()

		cfg, err := config.Load(agentDefaults())
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.MessagingURL).To(Equal("nats://localhost:4222"))
		Expect(cfg.Name).To(Equal("acm-cluster-sp"))
		Expect(cfg.Health.EnabledPlatforms).To(Equal([]string{"kubevirt", "baremetal"}))
		Expect(cfg.Monitoring.DebounceInterval.String()).To(Equal("1s"))
		Expect(cfg.Monitoring.ResyncInterval.String()).To(Equal("10m0s"))
		Expect(cfg.Monitoring.PublishRetryMax).To(Equal(3))
		Expect(cfg.Monitoring.PublishRetryInterval.String()).To(Equal("2s"))
	})

	It("returns error when messaging URL is empty", func() {
		setAllRequired()

		cfg, err := config.Load(shared.Agent{})
		Expect(err).To(HaveOccurred())
		Expect(cfg).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("messaging URL is required"))
	})
})
