package service

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
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
	// removeStaleEmbedded previously called registry.Release(p.ServiceType)
	// unconditionally for any disabled embedded record found in the store,
	// without checking whether the registry slot for that service type is
	// still held by that same embedded provider. Startup order (LoadPersisted
	// runs before RegisterEmbedded) makes it reachable for the store to
	// contain both a stale embedded record AND a legitimate external
	// provider record for the same service type — e.g. an operator disabled
	// an embedded SP and registered a real external SP for that type without
	// the stale embedded record ever being cleaned up. In that case,
	// unconditionally releasing the slot corrupts the registry: the external
	// provider's slot is freed even though the store still shows it as
	// occupied, violating the single-slot-per-service-type invariant
	// (REQ-SPR-200) and opening the door for a second registration to steal
	// the type out from under the legitimate external provider.
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
	// RegisterEmbedded -> registerEmbeddedType calls mon.RegisterProvider with
	// initialCheck=true, which runs the health check synchronously and — if
	// it yields a different status than the initial one — invokes the
	// monitor's onTransition callback in-line, before RegisterProvider even
	// returns. If the caller wires SetOnTransition AFTER calling
	// RegisterEmbedded (main.go's original ordering), that synchronous
	// transition is silently dropped: no callback fires, so nothing
	// downstream (retry-topic processing, health CloudEvent publication,
	// DCM re-registration) reacts to it.
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
