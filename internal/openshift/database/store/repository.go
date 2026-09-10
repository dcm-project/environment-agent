// Package store defines the database repository interface and error types.
package store

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
)

// DatabaseRepository defines the storage interface for database CRUD operations
// and backing infrastructure health checks.
type DatabaseRepository interface {
	Create(ctx context.Context, spec v1alpha1.DatabaseSpec, id string) (*v1alpha1.Database, error)
	Get(ctx context.Context, databaseID string) (*v1alpha1.Database, error)
	List(ctx context.Context, maxPageSize int32, pageToken string) (*v1alpha1.DatabaseList, error)
	Delete(ctx context.Context, databaseID string) error
	CheckHealth(ctx context.Context) error
}
