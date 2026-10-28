package database

import (
	"context"

	"github.com/dcm-project/environment-agent/internal/health/monitor"
)

// databaseHealthChecker checks backing Kubernetes connectivity for agent SP health.
type databaseHealthChecker interface {
	CheckHealth(ctx context.Context) error
}

func newHealthChecker(checker databaseHealthChecker) monitor.Checker {
	return monitor.NewEmbeddedChecker(func(ctx context.Context) monitor.HealthCheckResult {
		if err := checker.CheckHealth(ctx); err != nil {
			return monitor.CheckUnhealthy
		}
		return monitor.CheckHealthy
	})
}
