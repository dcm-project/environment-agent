// Package config provides environment-based configuration for the service provider.
package config

import (
	"fmt"
	"time"

	env "github.com/caarlos0/env/v11"
)

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	BindAddress     string        `env:"SP_SERVER_ADDRESS"          envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"SP_SERVER_SHUTDOWN_TIMEOUT" envDefault:"15s"`
	RequestTimeout  time.Duration `env:"SP_SERVER_REQUEST_TIMEOUT"   envDefault:"30s"`
	ReadTimeout     time.Duration `env:"SP_SERVER_READ_TIMEOUT"     envDefault:"15s"`
	WriteTimeout    time.Duration `env:"SP_SERVER_WRITE_TIMEOUT"    envDefault:"15s"`
	IdleTimeout     time.Duration `env:"SP_SERVER_IDLE_TIMEOUT"     envDefault:"60s"`
}

// HealthConfig holds health endpoint settings.
type HealthConfig struct {
	CheckTimeout     time.Duration `env:"SP_HEALTH_CHECK_TIMEOUT"   envDefault:"5s"`
	EnabledPlatforms []string      `env:"SP_ENABLED_PLATFORMS"      envDefault:"kubevirt,baremetal" envSeparator:","`
}

// RegistrationConfig holds DCM registration settings.
type RegistrationConfig struct {
	DCMRegistrationURL         string        `env:"DCM_REGISTRATION_URL,required"`
	ProviderName               string        `env:"SP_NAME"                          envDefault:"acm-cluster-sp"`
	ProviderEndpoint           string        `env:"SP_ENDPOINT,required"`
	RegistrationInitialBackoff time.Duration `env:"SP_REGISTRATION_INITIAL_BACKOFF"  envDefault:"1s"`
	RegistrationMaxBackoff     time.Duration `env:"SP_REGISTRATION_MAX_BACKOFF"      envDefault:"5m"`
	VersionCheckInterval       time.Duration `env:"SP_VERSION_CHECK_INTERVAL"        envDefault:"5m"`
	ProviderDisplayName        string        `env:"SP_DISPLAY_NAME"                  envDefault:""`
	ProviderRegion             string        `env:"SP_REGION"                        envDefault:""`
	ProviderZone               string        `env:"SP_ZONE"                          envDefault:""`
}

// ClusterConfig holds ACM cluster service configuration.
type ClusterConfig struct {
	ClusterNamespace  string            `env:"SP_CLUSTER_NAMESPACE,required"`
	BaseDomain        string            `env:"SP_BASE_DOMAIN"`
	PullSecret        string            `env:"SP_PULL_SECRET,required"` // REQ-ACM-195
	PullSecretName    string            `env:"-"`
	ConsoleURIPattern string            `env:"SP_CONSOLE_URI_PATTERN" envDefault:"https://console-openshift-console.apps.{name}.{base_domain}"`
	VersionMatrixPath string            `env:"SP_VERSION_MATRIX_PATH"`
	DefaultInfraEnv   string            `env:"SP_DEFAULT_INFRA_ENV"`
	AgentNamespace    string            `env:"SP_AGENT_NAMESPACE"`
	InfraEnvLabelKey  string            `env:"SP_INFRA_ENV_LABEL_KEY" envDefault:"infraenvs.agent-install.openshift.io"`
	VersionMatrix     map[string]string `env:"-"`
}

// MonitoringConfig holds status monitoring settings.
type MonitoringConfig struct {
	NATSUrl              string        `env:"SP_NATS_URL,required"`
	DebounceInterval     time.Duration `env:"SP_STATUS_DEBOUNCE_INTERVAL"     envDefault:"1s"`
	ResyncInterval       time.Duration `env:"SP_STATUS_RESYNC_INTERVAL"       envDefault:"10m"`
	PublishRetryMax      int           `env:"SP_NATS_PUBLISH_RETRY_MAX"       envDefault:"3"`
	PublishRetryInterval time.Duration `env:"SP_NATS_PUBLISH_RETRY_INTERVAL"  envDefault:"2s"`
}

// Config is the root configuration for the service provider.
type Config struct {
	Server       ServerConfig
	Registration RegistrationConfig
	Health       HealthConfig
	Cluster      ClusterConfig
	Monitoring   MonitoringConfig
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("loading configuration: %w", err)
	}
	return cfg, nil
}
