// Package config handles configuration loading from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

const defaultProviderName = "container-sp"

// Config is the root configuration for the embedded container service provider.
type Config struct {
	shared.Config
	Namespace           string        `env:"SP_CONTAINER_NAMESPACE" envDefault:"default"`
	ExternalServiceType string        `env:"SP_K8S_EXTERNAL_SVC_TYPE"`
	DebounceMs          int           `env:"SP_MONITOR_DEBOUNCE_MS" envDefault:"500"`
	ResyncPeriod        time.Duration `env:"SP_MONITOR_RESYNC_PERIOD" envDefault:"10m"`
}

// Load reads container SP configuration from environment variables.
func Load(agent shared.Agent) (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("loading container SP config: %w", err)
	}
	if cfg.Name == "" {
		cfg.Name = defaultProviderName
	}
	if err := shared.Apply(&cfg.Config, agent); err != nil {
		return nil, err
	}
	if err := cfg.validateKubernetes(); err != nil {
		return nil, fmt.Errorf("loading configuration: %w", err)
	}
	return cfg, nil
}

func (c *Config) validateKubernetes() error {
	switch c.ExternalServiceType {
	case "LoadBalancer", "NodePort":
		return nil
	default:
		return fmt.Errorf(
			"invalid SP_K8S_EXTERNAL_SVC_TYPE %q: must be LoadBalancer or NodePort",
			c.ExternalServiceType,
		)
	}
}
