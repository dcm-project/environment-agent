package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Internal (package store) test file so it can install a syncFn spy without
// widening FileStore's public API purely for testability — mirrors the
// randFn pattern used in messaging.Client for deterministic backoff testing.
var _ = Describe("FileStore durability", Label("unit"), func() {
	var (
		fs     *FileStore
		tmpDir string
		ctx    context.Context
		sp     StoredProvider
	)

	BeforeEach(func() {
		ctx = context.Background()
		tmpDir = GinkgoT().TempDir()
		var err error
		fs, err = NewFileStore(filepath.Join(tmpDir, "providers.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
		Expect(err).NotTo(HaveOccurred())

		sp = StoredProvider{
			ID: "sp-001", Name: "my-provider", Endpoint: "http://localhost:8080",
			ServiceType: "database", SchemaVersion: "v1alpha1", Type: "external",
			CreateTime: time.Now(), UpdateTime: time.Now(),
		}
	})

	It("fsyncs the temp file before rename, then fsyncs the parent directory (UT-SPR-110)", func() {
		var syncedNames []string
		fs.syncFn = func(f *os.File) error {
			syncedNames = append(syncedNames, f.Name())
			return f.Sync()
		}

		Expect(fs.Save(ctx, sp)).To(Succeed())

		Expect(syncedNames).To(HaveLen(2), "must fsync exactly twice: temp data file, then parent directory")
		Expect(syncedNames[0]).To(HaveSuffix(".tmp"), "the data file MUST be fsynced BEFORE the rename (crash-durability ordering)")
		Expect(syncedNames[1]).To(Equal(tmpDir), "the parent directory MUST be fsynced AFTER rename to persist the rename's directory-entry update")
	})

	It("propagates a temp-file fsync failure as a Save error instead of silently continuing to rename (UT-SPR-111)", func() {
		fs.syncFn = func(*os.File) error { return errors.New("simulated fsync failure") }

		err := fs.Save(ctx, sp)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("fsync"))

		_, statErr := os.Stat(fs.path)
		Expect(os.IsNotExist(statErr)).To(BeTrue(), "the real file must not exist: a failed fsync must abort before rename")
	})

	It("reports Save as successful when only the post-rename directory fsync fails (UT-SPR-112)", func() {
		// First syncFn call is the temp file (must succeed, real fsync);
		// second is the parent directory (fails here) — exercises exactly
		// the post-rename step, after the data is already committed.
		calls := 0
		fs.syncFn = func(f *os.File) error {
			calls++
			if calls == 1 {
				return f.Sync()
			}
			return errors.New("simulated directory fsync failure")
		}

		Expect(fs.Save(ctx, sp)).To(Succeed(),
			"Save must report success once the rename has completed — a directory-fsync failure "+
				"afterward is a reduced-durability event, not a 'the write didn't happen' event, "+
				"and must not make the caller believe the write was rolled back (service.go rolls "+
				"back in-memory registry/health state on any Save/Delete error, which would desync "+
				"it from the store the disk now reflects)")

		saved, err := fs.GetByID(ctx, sp.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(saved).NotTo(BeNil(), "the provider must actually be persisted on disk — the rename "+
			"really did commit despite the directory-fsync failure reported above")
	})
})
