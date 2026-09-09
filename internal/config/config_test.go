package config_test

import (
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/config"
)

var _ = Describe("Server Configuration", Label("unit"), func() {
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
			Expect(err.Error()).To(ContainSubstring("[1s, 10m0s]"))
		})
	})
})

// setValidEnv sets all required env vars to valid defaults.
func setValidEnv() {
	GinkgoT().Setenv("AGENT_NAME", "test-agent")
	GinkgoT().Setenv("AGENT_ENVIRONMENT", "test")
	GinkgoT().Setenv("AGENT_COST", "medium")
	GinkgoT().Setenv("DCM_REGISTRATION_URL", "http://localhost:8080")
	GinkgoT().Setenv("AGENT_MESSAGING_URL", "nats://localhost:4222")
}

var _ = Describe("Topic 8 Messaging AckWait Config", Label("unit"), func() {
	Describe("Load", func() {
		It("parses AckWait and CancelAckWait from env", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_ACK_WAIT", "90s")
			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "15s")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Messaging.AckWait).To(Equal(90 * time.Second))
			Expect(cfg.Messaging.CancelAckWait).To(Equal(15 * time.Second))
		})

		It("defaults AckWait to 120s and CancelAckWait to 10s", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Messaging.AckWait).To(Equal(120 * time.Second))
			Expect(cfg.Messaging.CancelAckWait).To(Equal(10 * time.Second))
		})
	})

	Describe("Validate", func() {
		It("rejects AckWait below 10s", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_ACK_WAIT", "5s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_ACK_WAIT"))
		})

		It("rejects AckWait above 5m", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_ACK_WAIT", "6m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_ACK_WAIT"))
		})

		It("accepts AckWait at lower boundary 10s", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_ACK_WAIT", "10s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		It("accepts AckWait at upper boundary 5m", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_ACK_WAIT", "5m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		It("rejects CancelAckWait below 1s", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "500ms")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_CANCEL_ACK_WAIT"))
		})

		It("rejects CancelAckWait above 1m", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "2m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_CANCEL_ACK_WAIT"))
		})

		It("accepts CancelAckWait at boundaries", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "1s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())

			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "1m")
			cfg, err = config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})
	})
})

var _ = Describe("Cancel Handler Timeout Config", Label("unit"), func() {
	Describe("Load", func() {
		It("parses CancelHandlerTimeout from env", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT", "2s")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Routing.CancelHandlerTimeout).To(Equal(2 * time.Second))
		})

		It("defaults CancelHandlerTimeout to 5s", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Routing.CancelHandlerTimeout).To(Equal(5 * time.Second))
		})
	})

	Describe("Validate", func() {
		It("rejects CancelHandlerTimeout below 500ms", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT", "100ms")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT"))
		})

		It("rejects CancelHandlerTimeout above 1m", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT", "2m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT"))
		})
	})

	Describe("ValidateCancelHandlerAckWaitInvariant", func() {
		It("rejects CancelHandlerTimeout >= CancelAckWait", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT", "10s")
			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "10s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.ValidateCancelHandlerAckWaitInvariant()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT"))
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_CANCEL_ACK_WAIT"))
		})

		It("accepts CancelHandlerTimeout < CancelAckWait", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_ROUTING_CANCEL_HANDLER_TIMEOUT", "5s")
			GinkgoT().Setenv("AGENT_MESSAGING_CANCEL_ACK_WAIT", "10s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.ValidateCancelHandlerAckWaitInvariant()).To(Succeed())
		})
	})
})

var _ = Describe("Messaging Reconnect Backoff Config", Label("unit"), func() {
	Describe("Load", func() {
		It("parses ReconnectInitialBackoff and ReconnectMaxBackoff from env", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF", "2s")
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_MAX_BACKOFF", "1m")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Messaging.ReconnectInitialBackoff).To(Equal(2 * time.Second))
			Expect(cfg.Messaging.ReconnectMaxBackoff).To(Equal(time.Minute))
		})

		It("defaults ReconnectInitialBackoff to 1s and ReconnectMaxBackoff to 30s", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Messaging.ReconnectInitialBackoff).To(Equal(1 * time.Second))
			Expect(cfg.Messaging.ReconnectMaxBackoff).To(Equal(30 * time.Second))
		})
	})

	Describe("Validate", func() {
		It("rejects ReconnectInitialBackoff below 100ms", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF", "50ms")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF"))
		})

		It("rejects ReconnectInitialBackoff above ReconnectMaxBackoff", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF", "45s")
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_MAX_BACKOFF", "30s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF"))
		})

		It("rejects ReconnectMaxBackoff above 5m", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_MAX_BACKOFF", "6m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_MESSAGING_RECONNECT_MAX_BACKOFF"))
		})

		It("accepts ReconnectInitialBackoff/MaxBackoff at boundaries", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_INITIAL_BACKOFF", "100ms")
			GinkgoT().Setenv("AGENT_MESSAGING_RECONNECT_MAX_BACKOFF", "5m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})
	})
})

var _ = Describe("Topic 6 Config", Label("unit"), func() {
	Describe("Load", func() {
		It("parses Topic 6 config fields from env (UT-XC-CFG-040)", func() {
			setValidEnv()
			GinkgoT().Setenv("DCM_REGISTRATION_INITIAL_BACKOFF", "2s")
			GinkgoT().Setenv("DCM_REGISTRATION_MAX_BACKOFF", "10m")
			GinkgoT().Setenv("AGENT_HEARTBEAT_INTERVAL", "45s")
			GinkgoT().Setenv("AGENT_MESSAGING_URL", "nats://localhost:4222")
			GinkgoT().Setenv("AGENT_TOPIC_NAME", "custom-topic")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Agent.Name).To(Equal("test-agent"))
			Expect(cfg.Agent.Environment).To(Equal("test"))
			Expect(cfg.Agent.Cost).To(Equal("medium"))
			Expect(cfg.DCM.RegistrationURL).To(Equal("http://localhost:8080"))
			Expect(cfg.DCM.InitialBackoff).To(Equal(2 * time.Second))
			Expect(cfg.DCM.MaxBackoff).To(Equal(10 * time.Minute))
			Expect(cfg.Heartbeat.Interval).To(Equal(45 * time.Second))
			Expect(cfg.Messaging.URL).To(Equal("nats://localhost:4222"))
			Expect(cfg.Messaging.TopicName).To(Equal("custom-topic"))
		})

		It("applies duration defaults (UT-XC-CFG-041)", func() {
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.DCM.InitialBackoff).To(Equal(time.Second))
			Expect(cfg.DCM.MaxBackoff).To(Equal(5 * time.Minute))
			Expect(cfg.Heartbeat.Interval).To(Equal(30 * time.Second))
		})

		It("rejects malformed duration string at parse time (UT-XC-CFG-035)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEARTBEAT_INTERVAL", "abc")
			_, err := config.Load()
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Validate", func() {
		DescribeTable("rejects absent required field (UT-XC-CFG-010, UT-XC-CFG-011)",
			func(envVar string) {
				setValidEnv()
				GinkgoT().Setenv(envVar, "")
				cfg, err := config.Load()
				Expect(err).NotTo(HaveOccurred())
				err = cfg.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(envVar))
			},
			Entry("AGENT_NAME", "AGENT_NAME"),
			Entry("AGENT_ENVIRONMENT", "AGENT_ENVIRONMENT"),
			Entry("AGENT_COST", "AGENT_COST"),
			Entry("DCM_REGISTRATION_URL", "DCM_REGISTRATION_URL"),
			Entry("AGENT_MESSAGING_URL (UT-XC-CFG-011)", "AGENT_MESSAGING_URL"),
		)

		It("accepts all required fields present (UT-XC-CFG-012)", func() {
			setValidEnv()
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		DescribeTable("rejects whitespace-only required field (UT-XC-CFG-013)",
			func(envVar string) {
				setValidEnv()
				GinkgoT().Setenv(envVar, "   ")
				cfg, err := config.Load()
				Expect(err).NotTo(HaveOccurred())
				err = cfg.Validate()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(envVar))
			},
			Entry("AGENT_NAME", "AGENT_NAME"),
			Entry("AGENT_ENVIRONMENT", "AGENT_ENVIRONMENT"),
		)

		It("rejects invalid AGENT_COST value (UT-XC-CFG-020)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_COST", "expensive")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_COST"))
			Expect(err.Error()).To(ContainSubstring("expensive"))
		})

		DescribeTable("accepts valid cost values (UT-XC-CFG-021, UT-XC-CFG-022, UT-XC-CFG-023)",
			func(cost string) {
				setValidEnv()
				GinkgoT().Setenv("AGENT_COST", cost)
				cfg, err := config.Load()
				Expect(err).NotTo(HaveOccurred())
				Expect(cfg.Validate()).To(Succeed())
			},
			Entry("low (UT-XC-CFG-021)", "low"),
			Entry("medium-low (UT-XC-CFG-023)", "medium-low"),
			Entry("medium (UT-XC-CFG-023)", "medium"),
			Entry("medium-high (UT-XC-CFG-023)", "medium-high"),
			Entry("high (UT-XC-CFG-022)", "high"),
		)

		It("rejects case-sensitive cost (UT-XC-CFG-024)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_COST", "Medium")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_COST"))
		})

		It("rejects empty cost (UT-XC-CFG-025)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_COST", "")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_COST"))
		})

		It("accepts heartbeat interval at minimum 5s (UT-XC-CFG-031)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEARTBEAT_INTERVAL", "5s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		It("accepts heartbeat interval at maximum 10m (UT-XC-CFG-032)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEARTBEAT_INTERVAL", "10m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		It("rejects heartbeat interval below minimum 5s (UT-XC-CFG-033)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEARTBEAT_INTERVAL", "4s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_HEARTBEAT_INTERVAL"))
		})

		It("rejects heartbeat interval above maximum 10m (UT-XC-CFG-034)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEARTBEAT_INTERVAL", "11m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_HEARTBEAT_INTERVAL"))
		})

		DescribeTable("integer config range (UT-XC-CFG-036)",
			func(value int, shouldPass bool) {
				setValidEnv()
				GinkgoT().Setenv("AGENT_HEALTH_FAILURE_THRESHOLD", fmt.Sprintf("%d", value))
				cfg, err := config.Load()
				Expect(err).NotTo(HaveOccurred())
				err = cfg.Validate()
				if shouldPass {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("AGENT_HEALTH_FAILURE_THRESHOLD"))
				}
			},
			Entry("0 rejected", 0, false),
			Entry("1 accepted", 1, true),
			Entry("100 accepted", 100, true),
			Entry("101 rejected", 101, false),
		)

		It("accepts timeout equal to interval (UT-XC-CFG-041)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_TIMEOUT", "10s")
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "10s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		It("accepts timeout below interval (UT-XC-CFG-042)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_TIMEOUT", "9s")
			GinkgoT().Setenv("AGENT_HEALTH_CHECK_INTERVAL", "10s")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Validate()).To(Succeed())
		})

		It("rejects initial backoff exceeding max backoff (UT-XC-CFG-032 cross-field)", func() {
			setValidEnv()
			GinkgoT().Setenv("DCM_REGISTRATION_INITIAL_BACKOFF", "10m")
			GinkgoT().Setenv("DCM_REGISTRATION_MAX_BACKOFF", "1m")
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			err = cfg.Validate()
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("SP Config", Label("unit"), func() {
	Describe("Load", func() {
		It("parses SP_DEFAULT_KUBECONFIG from env", func() {
			GinkgoT().Setenv("SP_DEFAULT_KUBECONFIG", "/etc/agent/kubeconfig")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.SP.DefaultKubeconfig).To(Equal("/etc/agent/kubeconfig"))
		})
	})
})

// writeConfigFile writes a .env-style (KEY=VALUE per line) config file and
// points AGENT_CONFIG_FILE at it. This is REQ-XC-CFG-010's MAY-level
// file-based config support.
func writeConfigFile(contents string) {
	path := GinkgoT().TempDir() + "/agent.env"
	Expect(os.WriteFile(path, []byte(contents), 0o600)).To(Succeed())
	GinkgoT().Setenv("AGENT_CONFIG_FILE", path)
}

var _ = Describe("File-Based Config", Label("unit"), func() {
	Describe("Load", func() {
		It("uses a value from the config file when the env var is not set (UT-XC-CFG-060, AC-XC-CFG-011)", func() {
			setValidEnv()
			writeConfigFile("AGENT_SERVER_ADDRESS=:9191\n")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Server.Address).To(Equal(":9191"))
		})

		It("prefers the environment variable over the config file (UT-XC-CFG-061, AC-XC-CFG-012)", func() {
			setValidEnv()
			writeConfigFile("AGENT_SERVER_ADDRESS=:9191\n")
			GinkgoT().Setenv("AGENT_SERVER_ADDRESS", ":7070")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Server.Address).To(Equal(":7070"), "env var must win over the config file")
		})

		It("ignores blank lines and '#'-prefixed comments (UT-XC-CFG-062)", func() {
			setValidEnv()
			writeConfigFile("# this is a comment\n\nAGENT_SERVER_ADDRESS=:9292\n  \n# another comment\n")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Server.Address).To(Equal(":9292"))
		})

		It("does not mutate the process environment as a side effect (UT-XC-CFG-063)", func() {
			setValidEnv()
			writeConfigFile("AGENT_SERVER_ADDRESS=:9393\n")

			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Server.Address).To(Equal(":9393"))

			_, isSet := os.LookupEnv("AGENT_SERVER_ADDRESS")
			Expect(isSet).To(BeFalse(),
				"Load must not os.Setenv file-sourced values into the real process environment "+
					"(would leak across subsequent Load() calls / tests)")
		})

		It("is a no-op when AGENT_CONFIG_FILE is not set (UT-XC-CFG-064)", func() {
			setValidEnv()
			cfg, err := config.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(cfg.Server.Address).To(Equal(":8080"))
		})

		It("returns an error when AGENT_CONFIG_FILE points at a nonexistent file (UT-XC-CFG-065)", func() {
			setValidEnv()
			GinkgoT().Setenv("AGENT_CONFIG_FILE", GinkgoT().TempDir()+"/does-not-exist.env")

			_, err := config.Load()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_CONFIG_FILE"))
		})

		It("returns an error for a malformed line missing '=' (UT-XC-CFG-066)", func() {
			setValidEnv()
			writeConfigFile("THIS_LINE_HAS_NO_EQUALS_SIGN\n")

			_, err := config.Load()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("AGENT_CONFIG_FILE"))
		})
	})
})
