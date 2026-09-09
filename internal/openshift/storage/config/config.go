// Package config handles configuration loading from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

const defaultProviderName = "storage"

// Config is the root configuration for the embedded storage service provider.
type Config struct {
	shared.Config
	Namespace           string        `env:"SP_STORAGE_NAMESPACE" envDefault:"default"`
	DefaultStorageClass string        `env:"SP_K8S_DEFAULT_STORAGE_CLASS"`
	DefaultAccessMode   string        `env:"SP_K8S_DEFAULT_ACCESS_MODE" envDefault:"ReadWriteOnce"`
	DebounceMs          int           `env:"SP_MONITOR_DEBOUNCE_MS" envDefault:"500"`
	ResyncPeriod        time.Duration `env:"SP_MONITOR_RESYNC_PERIOD" envDefault:"10m"`
	PublishMaxAttempts  int           `env:"SP_MONITOR_PUBLISH_MAX_ATTEMPTS" envDefault:"5"`
}

// Load reads storage SP configuration from environment variables.
func Load(agent shared.Agent) (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("loading storage SP config: %w", err)
	}
	if cfg.Name == "" {
		cfg.Name = defaultProviderName
	}
	if err := shared.Apply(&cfg.Config, agent); err != nil {
		return nil, fmt.Errorf("applying agent configuration: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("loading configuration: %w", err)
	}
	return cfg, nil
}

func (c *Config) validate() error {
	switch c.DefaultAccessMode {
	case "ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "":
		return nil
	default:
		return fmt.Errorf(
			"invalid SP_K8S_DEFAULT_ACCESS_MODE %q: must be ReadWriteOnce, ReadOnlyMany, ReadWriteMany, or empty (no explicit default)",
			c.DefaultAccessMode,
		)
	}
}
