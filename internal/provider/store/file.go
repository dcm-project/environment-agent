package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore implements Store using a JSON file for persistence.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore creates a FileStore that persists to the given path.
// It ensures the parent directory exists, returning an error if it cannot be created.
func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("creating store directory: %w", err)
	}
	return &FileStore{path: path}, nil
}

func (f *FileStore) Save(_ context.Context, p StoredProvider) error {
	if err := validateStoredProvider(&p); err != nil {
		return fmt.Errorf("invalid provider record: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	providers, err := f.readFile()
	if err != nil {
		return err
	}

	found := false
	for i, existing := range providers {
		if existing.Name == p.Name {
			providers[i] = p
			found = true
			break
		}
	}
	if !found {
		providers = append(providers, p)
	}

	return f.writeFile(providers)
}

func (f *FileStore) Delete(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	providers, err := f.readFile()
	if err != nil {
		return err
	}

	filtered := providers[:0]
	for _, p := range providers {
		if p.Name != name {
			filtered = append(filtered, p)
		}
	}
	return f.writeFile(filtered)
}

func (f *FileStore) List(_ context.Context) ([]StoredProvider, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readFile()
}

func (f *FileStore) GetByID(_ context.Context, id string) (*StoredProvider, error) {
	return f.findBy(func(p *StoredProvider) bool { return p.ID == id })
}

func (f *FileStore) GetByName(_ context.Context, name string) (*StoredProvider, error) {
	return f.findBy(func(p *StoredProvider) bool { return p.Name == name })
}

func (f *FileStore) findBy(match func(*StoredProvider) bool) (*StoredProvider, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	providers, err := f.readFile()
	if err != nil {
		return nil, err
	}
	for i := range providers {
		if match(&providers[i]) {
			return &providers[i], nil
		}
	}
	return nil, nil
}

func (f *FileStore) readFile() ([]StoredProvider, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []StoredProvider{}, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return []StoredProvider{}, nil
	}
	var providers []StoredProvider
	if err := json.Unmarshal(data, &providers); err != nil {
		return nil, err
	}
	for i, p := range providers {
		if err := validateStoredProvider(&p); err != nil {
			return nil, fmt.Errorf("invalid provider record at index %d: %w", i, err)
		}
	}
	return providers, nil
}

func validateStoredProvider(p *StoredProvider) error {
	for _, f := range []struct{ v, n string }{
		{p.ID, "id"},
		{p.Name, "name"},
		{p.ServiceType, "service_type"},
		{p.SchemaVersion, "schema_version"},
		{p.Type, "type"},
	} {
		if f.v == "" {
			return fmt.Errorf("missing %s", f.n)
		}
	}
	if p.Type != "embedded" && p.Type != "external" {
		return fmt.Errorf("invalid type %q", p.Type)
	}
	if p.CreateTime.IsZero() {
		return fmt.Errorf("missing create_time")
	}
	if p.UpdateTime.IsZero() {
		return fmt.Errorf("missing update_time")
	}
	return nil
}

func (f *FileStore) writeFile(providers []StoredProvider) error {
	data, err := json.MarshalIndent(providers, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
