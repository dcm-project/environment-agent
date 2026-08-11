package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

func TestService(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Service Suite")
}

func ptr(s string) *string { return &s }

var _ = Describe("ensureIDConsistency", Label("unit"), func() {
	var svc *ProviderService

	BeforeEach(func() {
		svc = &ProviderService{}
	})

	It("accepts nil requestedID (UT-SPR-090)", func() {
		err := svc.ensureIDConsistency("existing-id-abc", nil)
		Expect(err).NotTo(HaveOccurred())
	})

	It("accepts matching ID (UT-SPR-091)", func() {
		err := svc.ensureIDConsistency("existing-id-abc", ptr("existing-id-abc"))
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects mismatched ID with ErrCodeConflict (UT-SPR-092)", func() {
		err := svc.ensureIDConsistency("existing-id-abc", ptr("different-id-xyz"))
		Expect(err).To(HaveOccurred())

		domErr, ok := err.(*DomainError)
		Expect(ok).To(BeTrue(), "expected *DomainError")
		Expect(domErr.Code).To(Equal(ErrCodeConflict))
		Expect(domErr.Message).To(ContainSubstring("existing-id-abc"))
		Expect(domErr.Message).To(ContainSubstring("different-id-xyz"))
	})
})

func newTestService() *ProviderService {
	path := filepath.Join(GinkgoT().TempDir(), "registrations.json")
	fs, err := store.NewFileStore(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	Expect(err).NotTo(HaveOccurred())
	return &ProviderService{
		store:    fs,
		registry: provider.NewRegistry(),
		health:   provider.NewInMemoryHealthTracker(),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

var _ = Describe("SetOnChange callback", Label("unit"), func() {
	It("fires after successful new registration", func() {
		svc := newTestService()

		var called int
		svc.SetOnChange(func() { called++ })

		_, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "test-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(called).To(Equal(1))
	})

	It("fires after successful re-registration", func() {
		svc := newTestService()

		_, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "test-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())

		var called int
		svc.SetOnChange(func() { called++ })

		_, _, err = svc.Register(context.Background(), RegistrationInput{
			Name:          "test-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(called).To(Equal(1))
	})

	It("does not deadlock when callback calls Register", func() {
		svc := newTestService()

		var fired atomic.Bool
		svc.SetOnChange(func() {
			if fired.CompareAndSwap(false, true) {
				_, _, _ = svc.Register(context.Background(), RegistrationInput{
					Name:          "reentrant-sp",
					Endpoint:      "https://reentrant.example.com",
					ServiceType:   "cache",
					SchemaVersion: "v1alpha1",
				})
			}
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _, _ = svc.Register(context.Background(), RegistrationInput{
				Name:          "trigger-sp",
				Endpoint:      "https://trigger.example.com",
				ServiceType:   "compute",
				SchemaVersion: "v1alpha1",
			})
		}()

		Eventually(done).WithTimeout(2 * time.Second).Should(BeClosed())
	})

	It("does not fire on registration failure", func() {
		svc := newTestService()

		_, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "first-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())

		var called int
		svc.SetOnChange(func() { called++ })

		_, _, err = svc.Register(context.Background(), RegistrationInput{
			Name:          "second-sp",
			Endpoint:      "https://other.example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).To(HaveOccurred())
		Expect(called).To(Equal(0))
	})
})

var _ = Describe("registerEmbeddedType cleanup", Label("unit"), func() {
	It("removes stale embedded record when registry slot is occupied by external provider", func() {
		svc := newTestService()
		ctx := context.Background()

		// Simulate the startup scenario: an external provider claimed the
		// "postgres" slot via LoadPersisted, and a stale embedded record
		// for the same service type remains in the store.
		embeddedID := "emb-postgres-id"
		staleEmbedded := store.StoredProvider{
			ID:            embeddedID,
			Name:          "postgres",
			Endpoint:      "embedded://postgres",
			ServiceType:   "postgres",
			SchemaVersion: "v1alpha1",
			Type:          string(v1alpha1.Embedded),
			CreateTime:    time.Now().UTC(),
			UpdateTime:    time.Now().UTC(),
		}
		Expect(svc.store.Save(ctx, staleEmbedded)).To(Succeed())
		svc.health.SetState(embeddedID, v1alpha1.Ready, time.Now().UTC())

		// External provider claims the registry slot (as LoadPersisted would).
		Expect(svc.registry.Claim("ext-pg", "postgres")).To(Succeed())

		// RegisterEmbedded should detect the conflict and clean up the stale record.
		svc.RegisterEmbedded([]string{"postgres"})

		// The stale embedded record must be gone from the store.
		got, err := svc.store.GetByName(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(BeNil(), "stale embedded record should have been deleted")

		// Health state for the stale embedded provider must be removed.
		_, ok := svc.health.GetState(embeddedID)
		Expect(ok).To(BeFalse(), "health state for stale embedded should have been deleted")
	})

	It("does not delete external records when claim fails", func() {
		svc := newTestService()
		ctx := context.Background()

		// Register an external provider with the same name as a service type.
		_, _, err := svc.Register(ctx, RegistrationInput{
			Name:          "postgres",
			Endpoint:      "https://ext-pg.example.com",
			ServiceType:   "postgres",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())

		// RegisterEmbedded must not delete the external record.
		svc.RegisterEmbedded([]string{"postgres"})

		got, err := svc.store.GetByName(ctx, "postgres")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeNil())
		Expect(got.Type).To(Equal(string(v1alpha1.External)))
	})
})

var _ = Describe("toAPI health fallback", Label("unit"), func() {
	It("defaults to Unavailable when no health state exists", func() {
		tracker := provider.NewInMemoryHealthTracker()
		svc := &ProviderService{health: tracker}
		now := time.Now().UTC()

		ext := &store.StoredProvider{
			ID: "ext-1", Name: "ext", ServiceType: "database",
			Type: string(v1alpha1.External), CreateTime: now, UpdateTime: now,
		}
		p := svc.toAPI(ext)
		Expect(p.Status).To(HaveValue(Equal(v1alpha1.Unavailable)))
		Expect(p.LastCheckTime).To(BeNil())

		emb := &store.StoredProvider{
			ID: "emb-1", Name: "emb", ServiceType: "container",
			Type: string(v1alpha1.Embedded), CreateTime: now, UpdateTime: now,
		}
		p = svc.toAPI(emb)
		Expect(p.Status).To(HaveValue(Equal(v1alpha1.Unavailable)))
		Expect(p.LastCheckTime).To(BeNil())
	})
})

var _ = Describe("resolveEmbeddedIdentity", Label("unit"), func() {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	It("generates new ID and uses now when existing is nil (UT-SPR-100)", func() {
		id, ct := resolveEmbeddedIdentity(nil, now)
		Expect(id).NotTo(BeEmpty())
		Expect(ct).To(Equal(now))
	})

	It("generates new ID and uses now when existing is external (UT-SPR-101)", func() {
		existing := &store.StoredProvider{
			ID:         "ext-id-123",
			Type:       string(v1alpha1.External),
			CreateTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		id, ct := resolveEmbeddedIdentity(existing, now)
		Expect(id).NotTo(Equal("ext-id-123"))
		Expect(id).NotTo(BeEmpty())
		Expect(ct).To(Equal(now))
	})

	It("preserves ID and create_time from existing embedded record (UT-SPR-102)", func() {
		orig := time.Date(2025, 6, 15, 9, 30, 0, 0, time.UTC)
		existing := &store.StoredProvider{
			ID:         "emb-stable-id",
			Type:       string(v1alpha1.Embedded),
			CreateTime: orig,
		}
		id, ct := resolveEmbeddedIdentity(existing, now)
		Expect(id).To(Equal("emb-stable-id"))
		Expect(ct).To(Equal(orig))
	})

	It("generates new ID and uses now when embedded record has empty fields (UT-SPR-103)", func() {
		existing := &store.StoredProvider{
			ID:   "",
			Type: string(v1alpha1.Embedded),
		}
		id, ct := resolveEmbeddedIdentity(existing, now)
		Expect(id).NotTo(BeEmpty())
		Expect(ct).To(Equal(now))
	})
})

var _ = Describe("RegisterEmbedded stale removal", Label("unit"), func() {
	It("releases the registry slot when removing a stale embedded provider", func() {
		tmpDir := GinkgoT().TempDir()
		fileStore, err := store.NewFileStore(filepath.Join(tmpDir, "providers.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
		Expect(err).NotTo(HaveOccurred())

		registry := provider.NewRegistry()
		tracker := provider.NewInMemoryHealthTracker()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		svc := New(fileStore, registry, tracker, nil, logger)

		now := time.Now().UTC()
		Expect(fileStore.Save(context.Background(), store.StoredProvider{
			ID: "stale-id", Name: "old-service", ServiceType: "old-service",
			SchemaVersion: "v1alpha1", Type: string(v1alpha1.Embedded),
			CreateTime: now, UpdateTime: now,
		})).To(Succeed())
		Expect(registry.Claim("old-service", "old-service")).To(Succeed())
		tracker.SetState("stale-id", v1alpha1.Ready, now)

		svc.RegisterEmbedded([]string{"new-service"})

		_, occupied := registry.Lookup("old-service")
		Expect(occupied).To(BeFalse(), "stale embedded slot should be released")
	})
})

var _ = Describe("removeStaleEmbedded slot-ownership check", Label("unit"), func() {
	// Startup order (LoadPersisted before RegisterEmbedded) can leave both a
	// stale embedded record and a legitimate external record for the same
	// service type in the store. removeStaleEmbedded must only release the
	// slot if it's still held by the stale embedded provider itself,
	// otherwise it would steal the slot out from under the external
	// provider (REQ-SPR-200).
	It("does not release a service-type slot now legitimately held by a different (external) provider", func() {
		svc := newTestService()
		ctx := context.Background()

		Expect(svc.store.Save(ctx, store.StoredProvider{
			ID: "embedded-container-id", Name: "container", Endpoint: "embedded://container",
			ServiceType: "container", SchemaVersion: "v1alpha1", Type: string(v1alpha1.Embedded),
			CreateTime: time.Now(), UpdateTime: time.Now(),
		})).To(Succeed())
		Expect(svc.store.Save(ctx, store.StoredProvider{
			ID: "ext-id", Name: "external-container-sp", Endpoint: "https://example.com",
			ServiceType: "container", SchemaVersion: "v1alpha1", Type: string(v1alpha1.External),
			CreateTime: time.Now(), UpdateTime: time.Now(),
		})).To(Succeed())
		// Simulates LoadPersisted having already claimed the slot for the
		// external provider before RegisterEmbedded runs.
		Expect(svc.registry.Claim("external-container-sp", "container")).To(Succeed())

		// "container" is not in the enabled list, so removeStaleEmbedded
		// treats the embedded record as stale.
		svc.RegisterEmbedded(nil)

		holder, occupied := svc.registry.Lookup("container")
		Expect(occupied).To(BeTrue(), "the external provider's slot must remain claimed")
		Expect(holder).To(Equal("external-container-sp"))

		stored, err := svc.store.GetByName(ctx, "container")
		Expect(err).NotTo(HaveOccurred())
		Expect(stored).To(BeNil(), "the stale embedded record itself must still be deleted from the store")
	})

	It("releases the slot when it is still held by the stale embedded provider itself", func() {
		svc := newTestService()
		ctx := context.Background()

		Expect(svc.store.Save(ctx, store.StoredProvider{
			ID: "embedded-container-id", Name: "container", Endpoint: "embedded://container",
			ServiceType: "container", SchemaVersion: "v1alpha1", Type: string(v1alpha1.Embedded),
			CreateTime: time.Now(), UpdateTime: time.Now(),
		})).To(Succeed())
		Expect(svc.registry.Claim("container", "container")).To(Succeed())

		svc.RegisterEmbedded(nil)

		_, occupied := svc.registry.Lookup("container")
		Expect(occupied).To(BeFalse(), "the slot must be freed for reuse once the sole (embedded) holder is removed")
	})
})

var _ = Describe("RegisterEmbedded initialCheck transition ordering", Label("unit"), func() {
	// initialCheck=true runs the health check synchronously and can invoke
	// onTransition in-line before RegisterProvider returns, so
	// SetOnTransition must be wired before RegisterEmbedded is called or the
	// transition is silently dropped.
	It("delivers the synchronous transition when SetOnTransition is wired before RegisterEmbedded (UT-SPR-100)", func() {
		GinkgoT().Setenv("AGENT_EMBEDDED_SP_WIDGET_HEALTH", "unhealthy")

		tmpDir := GinkgoT().TempDir()
		fileStore, err := store.NewFileStore(filepath.Join(tmpDir, "providers.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
		Expect(err).NotTo(HaveOccurred())
		registry := provider.NewRegistry()
		tracker := provider.NewInMemoryHealthTracker()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		// FailureThreshold=1 so a single failing check crosses Ready->Unhealthy
		// immediately, matching the synchronous nature of initialCheck.
		mon := monitor.New(tracker, config.HealthConfig{FailureThreshold: 1, CheckTimeout: time.Second}, logger)
		svc := New(fileStore, registry, tracker, mon, logger)

		var captured []string
		mon.SetOnTransition(func(_ string, from, to v1alpha1.ProviderStatus) {
			captured = append(captured, string(from)+"->"+string(to))
		})

		svc.RegisterEmbedded([]string{"widget"})

		Expect(captured).To(Equal([]string{"Ready->Unhealthy"}),
			"the Ready->Unhealthy transition fired synchronously during RegisterEmbedded's "+
				"initialCheck must reach the callback — this only holds when SetOnTransition is "+
				"wired before RegisterEmbedded is called")
	})

	It("silently drops the transition when SetOnTransition is wired after RegisterEmbedded (UT-SPR-101, documents the bug main.go must avoid)", func() {
		GinkgoT().Setenv("AGENT_EMBEDDED_SP_WIDGET_HEALTH", "unhealthy")

		tmpDir := GinkgoT().TempDir()
		fileStore, err := store.NewFileStore(filepath.Join(tmpDir, "providers.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
		Expect(err).NotTo(HaveOccurred())
		registry := provider.NewRegistry()
		tracker := provider.NewInMemoryHealthTracker()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		mon := monitor.New(tracker, config.HealthConfig{FailureThreshold: 1, CheckTimeout: time.Second}, logger)
		svc := New(fileStore, registry, tracker, mon, logger)

		svc.RegisterEmbedded([]string{"widget"})

		var captured []string
		mon.SetOnTransition(func(_ string, from, to v1alpha1.ProviderStatus) {
			captured = append(captured, string(from)+"->"+string(to))
		})

		Expect(captured).To(BeEmpty(),
			"documents the ordering hazard this fix avoids: wiring SetOnTransition after "+
				"RegisterEmbedded means the callback did not exist yet when the transition fired, "+
				"so it is lost forever — this is exactly what main.go's construction order must avoid")
	})
})

// captureHandler records slog records for assertion.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

func findRecord(records []slog.Record, msg string) (slog.Record, bool) {
	for _, r := range records {
		if r.Message == msg {
			return r, true
		}
	}
	return slog.Record{}, false
}

func recordAttr(rec slog.Record, key string) (slog.Value, bool) {
	var v slog.Value
	var found bool
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v, found = a.Value, true
			return false
		}
		return true
	})
	return v, found
}

// fakeStore wraps a store.Store and can be made to fail Save on demand,
// used to simulate persistence failures for audit-logging assertions.
type fakeStore struct {
	store.Store
	saveErr error
}

func (f *fakeStore) Save(ctx context.Context, p store.StoredProvider) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	return f.Store.Save(ctx, p)
}

// newAuditTestService builds a ProviderService with a capture handler wired
// in as its logger, so audit log lines can be asserted on directly.
func newAuditTestService() (*ProviderService, *captureHandler) {
	fileStore, err := store.NewFileStore(filepath.Join(GinkgoT().TempDir(), "providers.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	Expect(err).NotTo(HaveOccurred())
	registry := provider.NewRegistry()
	tracker := provider.NewInMemoryHealthTracker()
	ch := &captureHandler{}
	svc := New(fileStore, registry, tracker, nil, slog.New(ch))
	return svc, ch
}

var _ = Describe("audit logging", Label("unit"), func() {
	It("logs embedded SP registered on successful embedded registration (IT-SPR-190)", func() {
		svc, ch := newAuditTestService()

		svc.RegisterEmbedded([]string{"widget"})

		rec, ok := findRecord(ch.all(), "embedded SP registered")
		Expect(ok).To(BeTrue())
		Expect(rec.Level).To(Equal(slog.LevelInfo))
		v, ok := recordAttr(rec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("widget"))
		v, ok = recordAttr(rec, "provider_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).NotTo(BeEmpty())
	})

	It("logs external SP registered on successful new external registration (IT-SPR-191)", func() {
		svc, ch := newAuditTestService()

		p, created, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeTrue())

		rec, ok := findRecord(ch.all(), "external SP registered")
		Expect(ok).To(BeTrue())
		Expect(rec.Level).To(Equal(slog.LevelInfo))
		v, ok := recordAttr(rec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("database"))
		v, ok = recordAttr(rec, "provider_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal(*p.Id))
		v, ok = recordAttr(rec, "name")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("ext-sp"))
	})

	It("logs external SP registration rejected on service type conflict (IT-SPR-192)", func() {
		svc, ch := newAuditTestService()

		svc.RegisterEmbedded([]string{"container"})

		_, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-container-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "container",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).To(HaveOccurred())

		rec, ok := findRecord(ch.all(), "external SP registration rejected: service type conflict")
		Expect(ok).To(BeTrue())
		Expect(rec.Level).To(Equal(slog.LevelWarn))
		v, ok := recordAttr(rec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("container"))
		v, ok = recordAttr(rec, "name")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("ext-container-sp"))
		v, ok = recordAttr(rec, "error")
		Expect(ok).To(BeTrue())
		Expect(v.String()).NotTo(BeEmpty())
	})

	It("logs persisted provider restored and a load summary from LoadPersisted (IT-SPR-193)", func() {
		svc, ch := newAuditTestService()
		ctx := context.Background()
		now := time.Now().UTC()

		Expect(svc.store.Save(ctx, store.StoredProvider{
			ID: "ext-1", Name: "ext-sp", Endpoint: "https://example.com",
			ServiceType: "database", SchemaVersion: "v1alpha1",
			Type: string(v1alpha1.External), CreateTime: now, UpdateTime: now,
		})).To(Succeed())

		// A second persisted external provider whose service type slot is
		// already claimed elsewhere, exercising the conflict-counting path.
		Expect(svc.registry.Claim("holder", "cache")).To(Succeed())
		Expect(svc.store.Save(ctx, store.StoredProvider{
			ID: "ext-2", Name: "ext-sp-2", Endpoint: "https://other.example.com",
			ServiceType: "cache", SchemaVersion: "v1alpha1",
			Type: string(v1alpha1.External), CreateTime: now, UpdateTime: now,
		})).To(Succeed())

		Expect(svc.LoadPersisted()).To(Succeed())

		restoredRec, ok := findRecord(ch.all(), "persisted provider restored")
		Expect(ok).To(BeTrue())
		Expect(restoredRec.Level).To(Equal(slog.LevelInfo))
		v, ok := recordAttr(restoredRec, "name")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("ext-sp"))
		v, ok = recordAttr(restoredRec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("database"))
		v, ok = recordAttr(restoredRec, "type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal(string(v1alpha1.External)))

		summaryRec, ok := findRecord(ch.all(), "finished loading persisted providers")
		Expect(ok).To(BeTrue())
		Expect(summaryRec.Level).To(Equal(slog.LevelInfo))
		v, ok = recordAttr(summaryRec, "restored")
		Expect(ok).To(BeTrue())
		Expect(v.Int64()).To(Equal(int64(1)))
		v, ok = recordAttr(summaryRec, "conflicts")
		Expect(ok).To(BeTrue())
		Expect(v.Int64()).To(Equal(int64(1)))
	})

	It("logs WARN with stale persisted state when re-registration Save fails despite an existing record (IT-SPR-194)", func() {
		svc, ch := newAuditTestService()

		// First registration succeeds normally, so an existing persisted
		// record is present on disk.
		svc.RegisterEmbedded([]string{"widget"})
		firstRec, ok := findRecord(ch.all(), "embedded SP registered")
		Expect(ok).To(BeTrue())
		Expect(firstRec.Level).To(Equal(slog.LevelInfo))

		// Swap in a store that fails Save while keeping the existing
		// persisted record intact (as the real failure mode would).
		svc.store = &fakeStore{Store: svc.store, saveErr: errors.New("disk full")}

		// Re-register the same (still-enabled) service type; the fresh
		// persistence write fails, but an existing record means execution
		// must continue rather than bail out.
		svc.RegisterEmbedded([]string{"widget"})

		staleRec, ok := findRecord(ch.all(), "embedded SP registered with stale persisted state (save failed)")
		Expect(ok).To(BeTrue())
		Expect(staleRec.Level).To(Equal(slog.LevelWarn))
		v, ok := recordAttr(staleRec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("widget"))
		v, ok = recordAttr(staleRec, "provider_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).NotTo(BeEmpty())

		// The success-path INFO log must not fire again for this attempt.
		var successCount int
		for _, r := range ch.all() {
			if r.Message == "embedded SP registered" {
				successCount++
			}
		}
		Expect(successCount).To(Equal(1), "only the first, successful registration should log at INFO")
	})

	It("logs embedded SP removed on successful embedded cleanup when disabled (IT-SPR-195)", func() {
		svc, ch := newAuditTestService()

		svc.RegisterEmbedded([]string{"widget"})
		rec, ok := findRecord(ch.all(), "embedded SP registered")
		Expect(ok).To(BeTrue())
		v, ok := recordAttr(rec, "provider_id")
		Expect(ok).To(BeTrue())
		providerID := v.String()

		// Shrinking the enabled list to exclude "widget" triggers
		// removeStaleEmbedded -> cleanupEmbeddedRecord for it.
		svc.RegisterEmbedded(nil)

		removedRec, ok := findRecord(ch.all(), "embedded SP removed")
		Expect(ok).To(BeTrue())
		Expect(removedRec.Level).To(Equal(slog.LevelInfo))
		v, ok = recordAttr(removedRec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("widget"))
		v, ok = recordAttr(removedRec, "provider_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal(providerID))
		v, ok = recordAttr(removedRec, "name")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("widget"))
	})

	It("does not log embedded SP removed when the store delete fails (IT-SPR-196)", func() {
		svc, ch := newAuditTestService()

		svc.RegisterEmbedded([]string{"widget"})
		_, ok := findRecord(ch.all(), "embedded SP registered")
		Expect(ok).To(BeTrue())

		svc.store = &fakeDeleteStore{Store: svc.store, deleteErr: errors.New("disk full")}

		svc.RegisterEmbedded(nil)

		_, ok = findRecord(ch.all(), "embedded SP removed")
		Expect(ok).To(BeFalse(), "no success log should be emitted when Delete fails")
		errRec, ok := findRecord(ch.all(), "failed to delete embedded record")
		Expect(ok).To(BeTrue())
		Expect(errRec.Level).To(Equal(slog.LevelError))
	})

	It("logs external SP updated on a successful re-registration with a changed field (IT-SPR-197)", func() {
		svc, ch := newAuditTestService()

		p, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())

		_, created, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://updated.example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeFalse())

		rec, ok := findRecord(ch.all(), "external SP updated")
		Expect(ok).To(BeTrue())
		Expect(rec.Level).To(Equal(slog.LevelInfo))
		v, ok := recordAttr(rec, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("database"))
		v, ok = recordAttr(rec, "provider_id")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal(*p.Id))
		v, ok = recordAttr(rec, "name")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("ext-sp"))
	})

	It("does not log external SP updated when Save fails on re-registration (IT-SPR-198)", func() {
		svc, ch := newAuditTestService()

		_, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())

		svc.store = &fakeStore{Store: svc.store, saveErr: errors.New("disk full")}

		_, _, err = svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://updated.example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).To(HaveOccurred())

		_, ok := findRecord(ch.all(), "external SP updated")
		Expect(ok).To(BeFalse(), "no success log should be emitted when Save fails")
	})

	It("re-registers the health monitor with the new service type when only the service type changes (IT-SPR-199)", func() {
		tmpDir := GinkgoT().TempDir()
		fileStore, err := store.NewFileStore(filepath.Join(tmpDir, "providers.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
		Expect(err).NotTo(HaveOccurred())
		registry := provider.NewRegistry()
		tracker := provider.NewInMemoryHealthTracker()
		ch := &captureHandler{}
		monLogger := slog.New(ch)
		mon := monitor.New(tracker, config.HealthConfig{FailureThreshold: 1, CheckTimeout: time.Second}, monLogger)
		svc := New(fileStore, registry, tracker, mon, monLogger)

		p, _, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "database",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())

		_, created, err := svc.Register(context.Background(), RegistrationInput{
			Name:          "ext-sp",
			Endpoint:      "https://example.com",
			ServiceType:   "cache",
			SchemaVersion: "v1alpha1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created).To(BeFalse())

		// Find the LAST "provider registered for health monitoring" record
		// for this provider ID; it must reflect the new service type.
		var last slog.Record
		var found bool
		for _, r := range ch.all() {
			if r.Message != "provider registered for health monitoring" {
				continue
			}
			v, ok := recordAttr(r, "provider_id")
			if !ok || v.String() != *p.Id {
				continue
			}
			last, found = r, true
		}
		Expect(found).To(BeTrue(), "expected the monitor to re-register this provider")
		v, ok := recordAttr(last, "service_type")
		Expect(ok).To(BeTrue())
		Expect(v.String()).To(Equal("cache"), "monitor's cached service_type must be refreshed after a service-type-only change")
	})
})

// fakeDeleteStore wraps a store.Store and can be made to fail Delete on
// demand, used to simulate deletion failures for audit-logging assertions.
type fakeDeleteStore struct {
	store.Store
	deleteErr error
}

func (f *fakeDeleteStore) Delete(ctx context.Context, name string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.Store.Delete(ctx, name)
}
