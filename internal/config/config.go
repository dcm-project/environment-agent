// Package config handles environment-based configuration loading and validation.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all configuration for the environment agent.
type Config struct {
	Server    ServerConfig    `envPrefix:"AGENT_SERVER_"`
	Provider  ProviderConfig  `envPrefix:"AGENT_"`
	Health    HealthConfig    `envPrefix:"AGENT_"`
	Agent     AgentConfig     `envPrefix:"AGENT_"`
	DCM       DCMConfig       `envPrefix:"DCM_"`
	Heartbeat HeartbeatConfig `envPrefix:"AGENT_"`
	Messaging MessagingConfig `envPrefix:"AGENT_"`
}

// HealthConfig holds SP health monitoring configuration.
type HealthConfig struct {
	CheckInterval        time.Duration `env:"HEALTH_CHECK_INTERVAL" envDefault:"10s"`
	CheckTimeout         time.Duration `env:"HEALTH_CHECK_TIMEOUT" envDefault:"5s"`
	FailureThreshold     int           `env:"HEALTH_FAILURE_THRESHOLD" envDefault:"3"`
	PodConditionsEnabled string        `env:"POD_CONDITIONS_ENABLED" envDefault:"auto"`
}

// ProviderConfig holds SP registration configuration.
type ProviderConfig struct {
	EmbeddedSPs     []string `env:"EMBEDDED_SPS" envSeparator:"," envDefault:""`
	PersistencePath string   `env:"SP_PERSISTENCE_PATH" envDefault:"/var/lib/environment-agent/registrations"`
}

// AgentConfig holds the agent's identity and classification.
type AgentConfig struct {
	Name        string `env:"NAME"`
	Environment string `env:"ENVIRONMENT"`
	Cost        string `env:"COST"`
}

// DCMConfig holds DCM registration configuration.
type DCMConfig struct {
	RegistrationURL           string        `env:"REGISTRATION_URL"`
	InitialBackoff            time.Duration `env:"REGISTRATION_INITIAL_BACKOFF" envDefault:"1s"`
	MaxBackoff                time.Duration `env:"REGISTRATION_MAX_BACKOFF" envDefault:"5m"`
	PrerequisiteRetryInterval time.Duration `env:"PREREQUISITE_RETRY_INTERVAL" envDefault:"5s"`
}

// HeartbeatConfig holds heartbeat timing configuration.
type HeartbeatConfig struct {
	Interval time.Duration `env:"HEARTBEAT_INTERVAL" envDefault:"30s"`
}

// MessagingConfig holds messaging bus configuration.
type MessagingConfig struct {
	URL       string `env:"MESSAGING_URL"`
	TopicName string `env:"TOPIC_NAME"`
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Address         string        `env:"ADDRESS" envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`
	RequestTimeout  time.Duration `env:"REQUEST_TIMEOUT" envDefault:"30s"`
}

// Load parses configuration from environment variables.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks configuration values against allowed ranges.
func (c *Config) Validate() error {
	if err := validateDurationRange("AGENT_SERVER_REQUEST_TIMEOUT", c.Server.RequestTimeout, time.Second, 10*time.Minute); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_SERVER_SHUTDOWN_TIMEOUT", c.Server.ShutdownTimeout, time.Second, 5*time.Minute); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_HEALTH_CHECK_INTERVAL", c.Health.CheckInterval, time.Second, 5*time.Minute); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_HEALTH_CHECK_TIMEOUT", c.Health.CheckTimeout, 500*time.Millisecond, c.Health.CheckInterval); err != nil {
		return err
	}
	if c.Health.FailureThreshold < 1 || c.Health.FailureThreshold > 100 {
		return fmt.Errorf("AGENT_HEALTH_FAILURE_THRESHOLD: value %d is outside valid range [1, 100]", c.Health.FailureThreshold)
	}

	// Topic 6: DCM Registration & Heartbeat — append-only below this line
	if err := validateRequired("AGENT_NAME", c.Agent.Name); err != nil {
		return err
	}
	if err := validateRequired("AGENT_ENVIRONMENT", c.Agent.Environment); err != nil {
		return err
	}
	if err := validateRequired("AGENT_COST", c.Agent.Cost); err != nil {
		return err
	}
	if err := validateRequired("DCM_REGISTRATION_URL", c.DCM.RegistrationURL); err != nil {
		return err
	}
	if !isValidCost(c.Agent.Cost) {
		return fmt.Errorf("AGENT_COST: invalid value %q, must be one of: low, medium-low, medium, medium-high, high", c.Agent.Cost)
	}
	if err := validateDurationRange("DCM_REGISTRATION_INITIAL_BACKOFF", c.DCM.InitialBackoff, 100*time.Millisecond, c.DCM.MaxBackoff); err != nil {
		return err
	}
	if err := validateDurationRange("DCM_REGISTRATION_MAX_BACKOFF", c.DCM.MaxBackoff, c.DCM.InitialBackoff, time.Hour); err != nil {
		return err
	}
	if err := validateDurationRange("DCM_PREREQUISITE_RETRY_INTERVAL", c.DCM.PrerequisiteRetryInterval, time.Second, 5*time.Minute); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_HEARTBEAT_INTERVAL", c.Heartbeat.Interval, 5*time.Second, 10*time.Minute); err != nil {
		return err
	}

	// Topic 7: Messaging Integration — append-only below this line
	// AGENT_MESSAGING_URL validated when messaging subsystem is wired.
	return nil
}
