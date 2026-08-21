package runtime

import (
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
)

// LoadConfig reads SP configuration from environment variables.
// When embedded is true, DCM_REGISTRATION_URL and SP_ENDPOINT are not required;
// fallbackNATSURL is used when SP_NATS_URL is unset (typically AGENT_MESSAGING_URL).
func LoadConfig(embedded bool, fallbackNATSURL string) (*config.Config, error) {
	if !embedded {
		return config.Load()
	}
	return loadEmbeddedConfig(fallbackNATSURL)
}

// embeddedRegistrationEnv holds registration fields needed in embedded mode.
// Standalone-only required vars (DCM_REGISTRATION_URL, SP_ENDPOINT) are omitted.
type embeddedRegistrationEnv struct {
	ProviderName string `env:"SP_NAME" envDefault:"acm-cluster-sp"`
}

func loadEmbeddedConfig(fallbackNATSURL string) (*config.Config, error) {
	cfg := &config.Config{}

	if err := env.ParseWithOptions(&cfg.Health, env.Options{}); err != nil {
		return nil, fmt.Errorf("SP health config: %w", err)
	}
	if err := env.ParseWithOptions(&cfg.Cluster, env.Options{}); err != nil {
		return nil, fmt.Errorf("SP cluster config: %w", err)
	}

	envMap := env.ToMap(os.Environ())
	if envMap["SP_NATS_URL"] == "" {
		envMap["SP_NATS_URL"] = fallbackNATSURL
	}
	if envMap["SP_NATS_URL"] == "" {
		return nil, fmt.Errorf("SP_NATS_URL or agent messaging URL is required when embedded")
	}
	if err := env.ParseWithOptions(&cfg.Monitoring, env.Options{Environment: envMap}); err != nil {
		return nil, fmt.Errorf("SP monitoring config: %w", err)
	}

	var reg embeddedRegistrationEnv
	if err := env.ParseWithOptions(&reg, env.Options{}); err != nil {
		return nil, fmt.Errorf("SP registration config: %w", err)
	}
	cfg.Registration = config.RegistrationConfig{
		ProviderName: reg.ProviderName,
	}

	return cfg, nil
}
