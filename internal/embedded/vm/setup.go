package vm

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	vmapp "github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/app"
	vmcfg "github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/config"
	"github.com/dcm-project/environment-agent/internal/openshift/shared"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded VM SP components.
type Bundle struct {
	App     *vmapp.App
	Handler routing.EmbeddedHandler
	Checker monitor.Checker

	// Operations are the resource operations this SP advertises at
	// registration time. Declared by the package (see Operations); carried
	// on the bundle so callers read everything an enabled SP provides from
	// one place.
	Operations []string
}

// Enabled reports whether vm is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded VM app when enabled.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	cfg, err := vmcfg.Load(shared.FromAgent(agentCfg))
	if err != nil {
		return nil, fmt.Errorf("loading VM SP config: %w", err)
	}

	a, err := vmapp.New(ctx, cfg, logger, vmapp.Options{})
	if err != nil {
		return nil, fmt.Errorf("creating VM SP app: %w", err)
	}

	return &Bundle{
		App:        a,
		Handler:    NewVMHandler(a.Client(), a.Mapper()),
		Checker:    newHealthChecker(a.Client()),
		Operations: Operations,
	}, nil
}

// Start launches background workers (event monitor).
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
