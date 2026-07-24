// Package config handles environment-based configuration loading and validation.
package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all configuration for the environment agent.
type Config struct {
	Server ServerConfig `envPrefix:"AGENT_SERVER_"`
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
	return nil
}
