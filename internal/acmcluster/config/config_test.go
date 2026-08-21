package config_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
)

var _ = Describe("Config", func() {
	// All required env vars needed for a valid Load().
	requiredVars := map[string]string{
		"DCM_REGISTRATION_URL": "http://dcm",
		"SP_ENDPOINT":          "http://sp",
		"SP_CLUSTER_NAMESPACE": "clusters",
		"SP_PULL_SECRET":       "eyJhdXRocyI6e319",
		"SP_NATS_URL":          "nats://localhost:4222",
	}

	setAllRequired := func() {
		for k, v := range requiredVars {
			GinkgoT().Setenv(k, v)
		}
	}

	DescribeTable("TC-CFG-UT-001: required config missing causes fail-fast",
		func(missingVar string) {
			for k, v := range requiredVars {
				if k != missingVar {
					GinkgoT().Setenv(k, v)
				}
			}
			// Record original value for Ginkgo restore, then unset.
			GinkgoT().Setenv(missingVar, "")
			Expect(os.Unsetenv(missingVar)).To(Succeed())

			_, err := config.Load()
			Expect(err).To(HaveOccurred(), "Load() should fail when %s is missing", missingVar)
		},
		Entry("DCM_REGISTRATION_URL missing", "DCM_REGISTRATION_URL"),
		Entry("SP_ENDPOINT missing", "SP_ENDPOINT"),
		Entry("SP_CLUSTER_NAMESPACE missing", "SP_CLUSTER_NAMESPACE"),
		Entry("SP_PULL_SECRET missing", "SP_PULL_SECRET"),
		Entry("SP_NATS_URL missing", "SP_NATS_URL"),
	)

	It("TC-CFG-UT-002: applies defaults when optional vars are not set", func() {
		setAllRequired()

		cfg, err := config.Load()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Server.BindAddress).To(Equal(":8080"))
		Expect(cfg.Server.ShutdownTimeout.String()).To(Equal("15s"))
		Expect(cfg.Registration.ProviderName).To(Equal("acm-cluster-sp"))
		Expect(cfg.Health.EnabledPlatforms).To(Equal([]string{"kubevirt", "baremetal"}))
		Expect(cfg.Monitoring.DebounceInterval.String()).To(Equal("1s"))
		Expect(cfg.Monitoring.ResyncInterval.String()).To(Equal("10m0s"))
		Expect(cfg.Monitoring.PublishRetryMax).To(Equal(3))
		Expect(cfg.Monitoring.PublishRetryInterval.String()).To(Equal("2s"))
	})
})
