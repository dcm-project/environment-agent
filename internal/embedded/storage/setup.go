// Package storage embeds the k8s storage service provider in the agent.
package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	storageapp "github.com/dcm-project/environment-agent/internal/openshift/storage/app"
	storagecfg "github.com/dcm-project/environment-agent/internal/openshift/storage/config"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded storage SP components.
type Bundle struct {
	App     *storageapp.App
	Handler routing.EmbeddedHandler
	Checker monitor.Checker
}

// Enabled reports whether storage is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded storage app when enabled.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	cfg, err := storagecfg.Load(shared.FromAgent(agentCfg))
	if err != nil {
		return nil, fmt.Errorf("loading storage SP config: %w", err)
	}

	a, err := storageapp.New(ctx, cfg, logger, storageapp.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating storage SP app: %w", err)
	}

	return &Bundle{
		App:     a,
		Handler: NewStorageHandler(a.Store()),
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
