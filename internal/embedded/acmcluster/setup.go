package acmcluster

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	acmruntime "github.com/dcm-project/environment-agent/internal/acmcluster/runtime"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/routing"
)

// Bundle holds embedded ACM cluster SP components.
type Bundle struct {
	Runtime *acmruntime.Runtime
	Handler routing.EmbeddedHandler
	Checker monitor.Checker
}

// Enabled reports whether the ACM cluster SP is listed in AGENT_EMBEDDED_SPS.
func Enabled(embeddedSPs []string) bool {
	for _, st := range embeddedSPs {
		if strings.TrimSpace(st) == ServiceType {
			return true
		}
	}
	return false
}

// Setup constructs the embedded ACM cluster runtime when enabled in config.
func Setup(ctx context.Context, agentCfg *config.Config, logger *slog.Logger) (*Bundle, error) {
	if !Enabled(agentCfg.Provider.EmbeddedSPs) {
		return nil, nil
	}

	acmCfg, err := acmruntime.LoadConfig(true, agentCfg.Messaging.URL)
	if err != nil {
		return nil, fmt.Errorf("loading ACM cluster SP config: %w", err)
	}
	if err := acmruntime.PrepareConfig(acmCfg); err != nil {
		return nil, fmt.Errorf("preparing ACM cluster SP config: %w", err)
	}

	rt, err := acmruntime.New(ctx, acmCfg, logger, acmruntime.Options{
		DisableRegistration: true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating ACM cluster SP runtime: %w", err)
	}

	return &Bundle{
		Runtime: rt,
		Handler: NewHandler(rt.ClusterService(), logger),
		Checker: newHealthChecker(rt.HealthChecker()),
	}, nil
}

// Start launches background workers (status monitor).
func (b *Bundle) Start(ctx context.Context) {
	if b == nil || b.Runtime == nil {
		return
	}
	b.Runtime.Start(ctx)
}

// Close releases runtime resources.
func (b *Bundle) Close() error {
	if b == nil || b.Runtime == nil {
		return nil
	}
	return b.Runtime.Close()
}

// EmbeddedHandlers returns forwarder handlers for enabled embedded SP types.
func EmbeddedHandlers(bundle *Bundle) map[string]routing.EmbeddedHandler {
	if bundle == nil {
		return nil
	}
	return map[string]routing.EmbeddedHandler{ServiceType: bundle.Handler}
}

// EmbeddedCheckers returns health checkers for enabled embedded SP types.
func EmbeddedCheckers(bundle *Bundle) map[string]monitor.Checker {
	if bundle == nil {
		return nil
	}
	return map[string]monitor.Checker{ServiceType: bundle.Checker}
}

func newHealthChecker(checker healthChecker) monitor.Checker {
	return monitor.NewEmbeddedChecker(func() monitor.HealthCheckResult {
		h := checker.Check(context.Background())
		if h.Status == nil {
			return monitor.CheckFailed
		}
		switch strings.ToLower(*h.Status) {
		case "healthy":
			return monitor.CheckHealthy
		case "unhealthy":
			return monitor.CheckUnhealthy
		default:
			return monitor.CheckFailed
		}
	})
}
