// Package config handles configuration loading from environment variables
package config

import (
	"fmt"
	"time"

	env "github.com/caarlos0/env/v11"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

const defaultProviderName = "database"

// Config is the root configuration for the service provider.
type Config struct {
	shared.Config
	Namespace           string        `env:"SP_DATABASE_NAMESPACE" envDefault:"default"`
	DefaultStorageClass string        `env:"SP_DATABASE_DEFAULT_STORAGE_CLASS"`
	ImageCatalog        string        `env:"SP_DATABASE_IMAGE_CATALOG"`
	ExternalServiceType string        `env:"SP_DATABASE_EXTERNAL_SVC_TYPE"` // Must be NodePort or LoadBalancer
	DefaultVersion      int           `env:"SP_DATABASE_DEFAULT_VERSION" envDefault:"18"`
	DebounceMs          int           `env:"SP_MONITOR_DEBOUNCE_MS"   envDefault:"500"`
	ResyncPeriod        time.Duration `env:"SP_MONITOR_RESYNC_PERIOD" envDefault:"10m"`
	PublishMaxAttempts  int           `env:"SP_MONITOR_PUBLISH_MAX_ATTEMPTS" envDefault:"5"`
}

// Load reads configuration from environment variables.
// Env vars: SP_SERVER_*, SP_*, DCM_*, SP_K8S_*, (see struct tags for details)
func Load(agent shared.Agent) (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("loading configuration: %w", err)
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
	switch c.ExternalServiceType {
	case "LoadBalancer", "NodePort":
		return nil
	default:
		return fmt.Errorf(
			"invalid SP_DATABASE_EXTERNAL_SVC_TYPE %q: must be LoadBalancer or NodePort",
			c.ExternalServiceType,
		)
	}
}
