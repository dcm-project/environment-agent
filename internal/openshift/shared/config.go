// Package shared holds settings common to all embedded OpenShift SPs.
package shared

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	agentconfig "github.com/dcm-project/environment-agent/internal/config"
)

// Agent carries agent-level defaults passed into each embedded SP at load time.
type Agent struct {
	MessagingURL string
	Kubeconfig   string
}

// Config holds deployment settings shared by all embedded SPs on this agent.
type Config struct {
	Name         string `env:"SP_NAME"`
	Kubeconfig   string `env:"SP_KUBECONFIG"`
	MessagingURL string `env:"-"`
}

// FromAgent extracts agent-level defaults for embedded SP configuration.
func FromAgent(cfg *agentconfig.Config) Agent {
	if cfg == nil {
		return Agent{}
	}
	return Agent{
		MessagingURL: cfg.Messaging.URL,
		Kubeconfig:   cfg.SP.DefaultKubeconfig,
	}
}

// Apply merges agent defaults into cfg.
func Apply(cfg *Config, agent Agent) error {
	if agent.MessagingURL == "" {
		return fmt.Errorf("messaging URL is required")
	}
	cfg.MessagingURL = agent.MessagingURL
	if cfg.Kubeconfig == "" {
		cfg.Kubeconfig = agent.Kubeconfig
	}
	return nil
}

// LoadInto reads SP_NAME and SP_KUBECONFIG from the environment, applies agent defaults,
// and sets defaultName when SP_NAME is unset.
func LoadInto(cfg *Config, agent Agent, defaultName string) error {
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("loading shared SP config: %w", err)
	}
	if cfg.Name == "" {
		cfg.Name = defaultName
	}
	return Apply(cfg, agent)
}
