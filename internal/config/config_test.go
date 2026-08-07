package config_test

import (
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/config"
)

var _ = Describe("Server Configuration", func() {
	Describe("Load", func() {
		It("parses all server config fields from environment variables (UT-HTTP-010)", func() {
			Expect(os.Setenv("AGENT_SERVER_ADDRESS", ":9090")).To(Succeed())
			DeferCleanup(os.Unsetenv, "AGENT_SERVER_ADDRESS")
			Expect(os.Setenv("AGENT_SERVER_SHUTDOWN_TIMEOUT", "30s")).To(Succeed())
			DeferCleanup(os.Unsetenv, "AGENT_SERVER_SHUTDOWN_TIMEOUT")
			Expect(os.Setenv("AGENT_SERVER_REQUEST_TIMEOUT", "1m")).To(Succeed())
			DeferCleanup(os.Unsetenv, "AGENT_SERVER_REQUEST_TIMEOUT")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())
			Expect(cfg.Server.Address).To(Equal(":9090"))
			Expect(cfg.Server.ShutdownTimeout).To(Equal(30 * time.Second))
			Expect(cfg.Server.RequestTimeout).To(Equal(1 * time.Minute))
		})

		It("defaults ADDRESS to :8080 when not set (UT-HTTP-011)", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())
			Expect(cfg.Server.Address).To(Equal(":8080"))
		})

		It("defaults SHUTDOWN_TIMEOUT to 15s when not set (UT-HTTP-012)", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())
			Expect(cfg.Server.ShutdownTimeout).To(Equal(15 * time.Second))
		})

		It("defaults REQUEST_TIMEOUT to 30s when not set (UT-HTTP-013)", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg).NotTo(BeNil())
			Expect(cfg.Server.RequestTimeout).To(Equal(30 * time.Second))
		})
	})

	Describe("Health Config", func() {
		It("parses health check config from env (UT-HMN-070)", func() {
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "20s")
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_TIMEOUT", "2s")
			GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", "5")
			GinkgoT().Setenv("AGENT_POD_CONDITIONS_ENABLED", "true")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Health.CheckInterval).To(Equal(20 * time.Second))
			Expect(cfg.Health.CheckTimeout).To(Equal(2 * time.Second))
			Expect(cfg.Health.FailureThreshold).To(Equal(5))
			Expect(cfg.Health.PodConditionsEnabled).To(Equal("true"))
		})
	})

	Describe("Validate", func() {
		It("rejects request timeout below minimum with value and range in error (UT-HTTP-020)", func() {
			cfg := &config.Config{
				Server: config.ServerConfig{
					Address:         ":8080",
					ShutdownTimeout: 15 * time.Second,
					RequestTimeout:  500 * time.Millisecond,
				},
			}

			err := cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("500ms"))
			Expect(err.Error()).To(ContainSubstring("[1s, 10m]"))
		})
	})
})
