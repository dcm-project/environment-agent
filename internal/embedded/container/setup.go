package container

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	containerapp "github.com/dcm-project/environment-agent/internal/openshift/container/app"
	containercfg "github.com/dcm-project/environment-agent/internal/openshift/container/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded container SP components.
type Bundle struct {
	App     *containerapp.App
	Handler routing.EmbeddedHandler
	Checker monitor.Checker

	// Operations are the resource operations this SP advertises at
	// registration time. Declared by the package (see Operations); carried
	// on the bundle so callers read everything an enabled SP provides from
	// one place.
	Operations []string
}

// Enabled reports whether container is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded container app when enabled.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	cfg, err := containercfg.Load(shared.FromAgent(agentCfg))
	if err != nil {
		return nil, fmt.Errorf("loading container SP config: %w", err)
	}

	a, err := containerapp.New(ctx, cfg, logger, containerapp.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating container SP app: %w", err)
	}

	return &Bundle{
		App:        a,
		Handler:    NewContainerHandler(a.Store()),
		Checker:    newHealthChecker(a.Store()),
		Operations: Operations,
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
