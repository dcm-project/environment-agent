// Package store defines the persistence interface and implementation for SP registrations.
package store

import (
	"context"
	"encoding/json"
	"time"
)

// Store defines persistence operations for SP registrations.
type Store interface {
	Save(ctx context.Context, p StoredProvider) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context) ([]StoredProvider, error)
	GetByID(ctx context.Context, id string) (*StoredProvider, error)
	GetByName(ctx context.Context, name string) (*StoredProvider, error)
}

// StoredProvider is the persistence representation of a registered provider.
type StoredProvider struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Endpoint      string          `json:"endpoint"`
	ServiceType   string          `json:"service_type"`
	SchemaVersion string          `json:"schema_version"`
	DisplayName   *string         `json:"display_name,omitempty"`
	Operations    *[]string       `json:"operations,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Type          string          `json:"type"`
	CreateTime    time.Time       `json:"create_time"`
	UpdateTime    time.Time       `json:"update_time"`
}
