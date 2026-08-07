// Package config handles environment-based configuration loading and validation.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all configuration for the environment agent.
type Config struct {
	Server   ServerConfig   `envPrefix:"AGENT_SERVER_"`
	Provider ProviderConfig `envPrefix:"AGENT_"`
	Health   HealthConfig   `envPrefix:"AGENT_"`
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
	if err := validateDurationRange("AGENT_SERVER_REQUEST_TIMEOUT", c.Server.RequestTimeout, time.Second, 10*time.Minute, "[1s, 10m]"); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_SERVER_SHUTDOWN_TIMEOUT", c.Server.ShutdownTimeout, time.Second, 5*time.Minute, "[1s, 5m]"); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_HEALTH_CHECK_INTERVAL", c.Health.CheckInterval, time.Second, 5*time.Minute, "[1s, 5m]"); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_HEALTH_CHECK_TIMEOUT", c.Health.CheckTimeout, 500*time.Millisecond, c.Health.CheckInterval, fmt.Sprintf("[500ms, %s]", c.Health.CheckInterval)); err != nil {
		return err
	}
	if c.Health.FailureThreshold < 1 || c.Health.FailureThreshold > 100 {
		return fmt.Errorf("AGENT_HEALTH_FAILURE_THRESHOLD: value %d is outside valid range [1, 100]", c.Health.FailureThreshold)
	}
	return nil
}
