// Package network embeds the k8s network service provider in the agent.
package network

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	networkapp "github.com/dcm-project/environment-agent/internal/openshift/network/app"
	networkcfg "github.com/dcm-project/environment-agent/internal/openshift/network/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded network SP components.
type Bundle struct {
	App     *networkapp.App
	Handler routing.EmbeddedHandler
	Checker monitor.Checker
}

// Enabled reports whether network is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded network app when enabled.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	cfg, err := networkcfg.Load(shared.FromAgent(agentCfg))
	if err != nil {
		return nil, fmt.Errorf("loading network SP config: %w", err)
	}

	a, err := networkapp.New(ctx, cfg, logger, networkapp.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating network SP app: %w", err)
	}

	return &Bundle{
		App:     a,
		Handler: NewNetworkHandler(a.Store()),
		Checker: newHealthChecker(a.Store()),
	}, nil
}

// Start launches background workers (status monitor).
func (b *Bundle) Start(ctx context.Context) {
	if b == nil || b.App == nil {
		return
	}
	b.App.Start(ctx)
}

// Close releases app resources.
func (b *Bundle) Close() error {
	if b == nil || b.App == nil {
		return nil
	}
	return b.App.Close()
}
