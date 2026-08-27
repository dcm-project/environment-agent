// Package store defines the volume repository interface and error types.
package store

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
)

// VolumeRepository defines the storage interface for volume CRUD operations
// and backing infrastructure health checks.
type VolumeRepository interface {
	Create(ctx context.Context, spec v1alpha1.StorageSpec, id string) (*v1alpha1.Volume, error)
	Get(ctx context.Context, volumeID string) (*v1alpha1.Volume, error)
	List(ctx context.Context, maxPageSize int32, pageToken string) (*v1alpha1.VolumeList, error)
	Delete(ctx context.Context, volumeID string) error
	CheckHealth(ctx context.Context) error
}
