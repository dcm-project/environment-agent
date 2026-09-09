// Package config handles configuration loading for the embedded kubevirt VM service provider.
package config

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
)

const defaultProviderName = "kubevirt-vm-sp"

// Config is the root configuration for the embedded VM service provider.
type Config struct {
	shared.Config
	Namespace          string        `env:"SP_VM_NAMESPACE" envDefault:"default"`
	Timeout            time.Duration `env:"KUBERNETES_TIMEOUT" envDefault:"60s"`
	MaxRetries         int           `env:"KUBERNETES_MAX_RETRIES" envDefault:"3"`
	NATSMaxReconnect   int           `env:"NATS_MAX_RECONNECT" envDefault:"-1"`
	NATSSubject        string        `env:"NATS_SUBJECT" envDefault:"dcm.vm"`
	EventsEnabled      bool          `env:"EVENTS_ENABLED" envDefault:"true"`
	EventsResyncPeriod time.Duration `env:"EVENTS_RESYNC_PERIOD" envDefault:"30m"`
}

// Load reads VM SP configuration from environment variables.
func Load(agent shared.Agent) (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("loading VM SP config: %w", err)
	}
	if cfg.Name == "" {
		cfg.Name = defaultProviderName
	}
	if err := shared.Apply(&cfg.Config, agent); err != nil {
		return nil, err
	}
	return cfg, nil
}
