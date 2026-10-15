package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	acmapp "github.com/dcm-project/environment-agent/internal/openshift/acmcluster/app"
	acmconfig "github.com/dcm-project/environment-agent/internal/openshift/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded cluster SP components.
type Bundle struct {
	App     *acmapp.App
	Handler routing.EmbeddedHandler
	Checker monitor.Checker

	// Operations are the resource operations this SP advertises at
	// registration time. Declared by the package (see Operations); carried
	// on the bundle so callers read everything an enabled SP provides from
	// one place.
	Operations []string
}

// Enabled reports whether cluster is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded cluster app when enabled.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	acmCfg, err := acmconfig.Load(shared.FromAgent(agentCfg))
	if err != nil {
		return nil, fmt.Errorf("loading cluster SP config: %w", err)
	}
	if err := acmapp.PrepareConfig(acmCfg); err != nil {
		return nil, fmt.Errorf("preparing cluster SP config: %w", err)
	}

	a, err := acmapp.New(ctx, acmCfg, logger, acmapp.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating cluster SP app: %w", err)
	}

	return &Bundle{
		App:        a,
		Handler:    NewClusterHandler(a.ClusterService()),
		Checker:    newHealthChecker(a.HealthChecker()),
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
