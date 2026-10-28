package database

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	databaseapp "github.com/dcm-project/environment-agent/internal/openshift/database/app"
	databasecfg "github.com/dcm-project/environment-agent/internal/openshift/database/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded database SP components.
type Bundle struct {
	// application
	App *databaseapp.App

	// embedded service components
	Handler routing.EmbeddedHandler
	Checker monitor.Checker
}

// Enabled reports whether database is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded database app when enabled.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	cfg, err := databasecfg.Load(shared.FromAgent(agentCfg))
	if err != nil {
		return nil, fmt.Errorf("loading database SP config: %w", err)
	}

	a, err := databaseapp.New(ctx, cfg, logger, databaseapp.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating database SP app: %w", err)
	}

	return &Bundle{
		App:     a,
		Handler: NewDatabaseHandler(a.Store()),
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
