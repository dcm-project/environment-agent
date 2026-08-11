package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// FileStore implements Store using a JSON file for persistence.
type FileStore struct {
	path   string
	mu     sync.Mutex
	logger *slog.Logger

	// syncFn is overridable in unit tests (package-internal) to observe
	// fsync call order/count without simulating an actual crash. Production
	// always uses (*os.File).Sync.
	syncFn func(*os.File) error
}

// NewFileStore creates a FileStore that persists to the given path.
// It ensures the parent directory exists, returning an error if it cannot be created.
func NewFileStore(path string, logger *slog.Logger) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("creating store directory: %w", err)
	}
	return &FileStore{path: path, logger: logger, syncFn: (*os.File).Sync}, nil
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

// writeFile persists providers durably: it fsyncs the temp file's data
// before renaming it into place, then fsyncs the parent directory so the
// rename's directory-entry update is itself durable. Without both fsyncs, a
// crash/power-loss between write and OS flush can corrupt or lose
// providers.json despite the atomic-rename pattern, undermining REQ-SPR-170's
// "survives restart" guarantee.
//
// Once os.Rename succeeds, the write is committed: the new data is already
// visible to any reader of f.path. Errors from the directory-fsync step that
// follows are therefore reported (logged) but NOT returned — returning them
// would make Save/Delete report a failure for a write that already actually
// happened, and callers (service.ProviderService) treat any Save/Delete
// error as "not persisted", rolling back in-memory registry/health state to
// match. Doing that after a successful rename would desync the in-memory
// registry from the store, which the disk now disagrees with (violating the
// slot ownership invariant). The directory fsync is defense-in-depth for a
// narrow crash window right after rename, not the point at which the data
// itself is committed.
func (f *FileStore) writeFile(providers []StoredProvider) error {
	data, err := json.MarshalIndent(providers, "", "  ")
	if err != nil {
		return err
	}

	tmp := f.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := f.syncFn(file); err != nil {
		_ = file.Close()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("rename temp file into place: %w", err)
	}

	dir, err := os.Open(filepath.Dir(f.path))
	if err != nil {
		f.logger.Warn("failed to open directory for post-rename fsync; write is committed, "+
			"but durability of the rename's directory entry is reduced", "path", f.path, "error", err)
		return nil
	}
	defer func() { _ = dir.Close() }()
	if err := f.syncFn(dir); err != nil {
		f.logger.Warn("failed to fsync directory after rename; write is committed, "+
			"but durability of the rename's directory entry is reduced", "path", f.path, "error", err)
	}
	return nil
}
