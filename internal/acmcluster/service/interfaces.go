// Package service defines domain service interfaces for the service provider.
package service

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
)

// ClusterService defines the domain operations for cluster lifecycle management.
type ClusterService interface {
	Create(ctx context.Context, id string, cluster v1alpha1.Cluster) (*v1alpha1.Cluster, error)
	Get(ctx context.Context, id string) (*v1alpha1.Cluster, error)
	List(ctx context.Context, pageSize int, pageToken string) (*v1alpha1.ClusterList, error)
	Delete(ctx context.Context, id string) error
}

// HealthChecker performs dependency health checks and returns the SP health status.
// The returned Health always has all required fields populated.
// The method never returns an error — unhealthy dependencies are reported via
// the Health.Status field ("unhealthy"), not via a Go error.
type HealthChecker interface {
	Check(ctx context.Context) v1alpha1.Health
}
