// Package store defines interfaces for storage and health-check abstractions.
package store

import "context"

// HealthChecker verifies backing infrastructure reachability for the health endpoint.
type HealthChecker interface {
	CheckHealth(ctx context.Context) error
}
