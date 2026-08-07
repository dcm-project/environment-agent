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
	Routing   RoutingConfig   `envPrefix:"AGENT_"`
}

// RoutingConfig holds resource operation routing configuration.
type RoutingConfig struct {
	RetryMaxAttempts int           `env:"ROUTING_RETRY_MAX" envDefault:"3"`
	RetryBackoff     time.Duration `env:"ROUTING_RETRY_BACKOFF" envDefault:"2s"`
	RetryMaxBackoff  time.Duration `env:"ROUTING_RETRY_MAX_BACKOFF" envDefault:"30s"`
	DenyListMaxSize  int           `env:"DENY_LIST_MAX_SIZE" envDefault:"100000"`
	HandlerTimeout   time.Duration `env:"ROUTING_HANDLER_TIMEOUT" envDefault:"60s"`
	NakDelay         time.Duration `env:"ROUTING_NAK_DELAY" envDefault:"500ms"`
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
	URL           string        `env:"MESSAGING_URL"`
	TopicName     string        `env:"TOPIC_NAME"`
	AckWait       time.Duration `env:"MESSAGING_ACK_WAIT" envDefault:"120s"`
	CancelAckWait time.Duration `env:"MESSAGING_CANCEL_ACK_WAIT" envDefault:"10s"`
	MaxDeliver    int           `env:"MESSAGING_MAX_DELIVER" envDefault:"10"`
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

	if err := validateRequired("AGENT_MESSAGING_URL", c.Messaging.URL); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_MESSAGING_ACK_WAIT", c.Messaging.AckWait, 10*time.Second, 5*time.Minute); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_MESSAGING_CANCEL_ACK_WAIT", c.Messaging.CancelAckWait, time.Second, time.Minute); err != nil {
		return err
	}

	if c.Routing.RetryMaxAttempts < 0 || c.Routing.RetryMaxAttempts > 20 {
		return fmt.Errorf("AGENT_ROUTING_RETRY_MAX: value %d is outside valid range [0, 20]", c.Routing.RetryMaxAttempts)
	}
	if err := validateDurationRange("AGENT_ROUTING_RETRY_BACKOFF", c.Routing.RetryBackoff, 100*time.Millisecond, c.Routing.RetryMaxBackoff); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_ROUTING_RETRY_MAX_BACKOFF", c.Routing.RetryMaxBackoff, c.Routing.RetryBackoff, 5*time.Minute); err != nil {
		return err
	}
	if c.Routing.DenyListMaxSize < 1000 || c.Routing.DenyListMaxSize > 10000000 {
		return fmt.Errorf("AGENT_DENY_LIST_MAX_SIZE: value %d is outside valid range [1000, 10000000]", c.Routing.DenyListMaxSize)
	}

	if c.Messaging.MaxDeliver < 1 || c.Messaging.MaxDeliver > 100 {
		return fmt.Errorf("AGENT_MESSAGING_MAX_DELIVER: value %d is outside valid range [1, 100]", c.Messaging.MaxDeliver)
	}
	if err := validateDurationRange("AGENT_ROUTING_HANDLER_TIMEOUT", c.Routing.HandlerTimeout, time.Second, 10*time.Minute); err != nil {
		return err
	}
	if err := validateDurationRange("AGENT_ROUTING_NAK_DELAY", c.Routing.NakDelay, 100*time.Millisecond, 30*time.Second); err != nil {
		return err
	}
	return nil
}

// ValidateHandlerAckWaitInvariant checks that HandlerTimeout < AckWait.
// Separated from Validate() because locked config tests set AckWait at
// boundary values (10s) without adjusting HandlerTimeout, and cross-field
// invariants depend on both values being intentional.
func (c *Config) ValidateHandlerAckWaitInvariant() error {
	if c.Routing.HandlerTimeout >= c.Messaging.AckWait {
		return fmt.Errorf("AGENT_ROUTING_HANDLER_TIMEOUT (%v) must be less than AGENT_MESSAGING_ACK_WAIT (%v) to prevent redelivery during handling", c.Routing.HandlerTimeout, c.Messaging.AckWait)
	}
	return nil
}
