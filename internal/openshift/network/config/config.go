// Package config handles configuration loading from environment variables.
package config

import (
	"fmt"
	"time"

	env "github.com/caarlos0/env/v11"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

const defaultProviderName = "network"

// Config is the root configuration for the embedded network service provider.
type Config struct {
	shared.Config
	Namespace          string        `env:"SP_K8S_NAMESPACE" envDefault:"default"`
	DebounceMs         int           `env:"SP_MONITOR_DEBOUNCE_MS" envDefault:"500"`
	ResyncPeriod       time.Duration `env:"SP_MONITOR_RESYNC_PERIOD" envDefault:"10m"`
	PublishMaxAttempts int           `env:"SP_MONITOR_PUBLISH_MAX_ATTEMPTS" envDefault:"5"`
}

// Load reads network SP configuration from environment variables.
func Load(agent shared.Agent) (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("loading network SP config: %w", err)
	}
	if cfg.Name == "" {
		cfg.Name = defaultProviderName
	}
	if err := shared.Apply(&cfg.Config, agent); err != nil {
		return nil, fmt.Errorf("applying agent configuration: %w", err)
	}
	return cfg, nil
}
