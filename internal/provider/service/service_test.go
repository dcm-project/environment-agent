package service

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
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

var _ = Describe("toAPI health fallback", Label("unit"), func() {
	It("returns type-aware defaults when no health state exists", func() {
		tracker := provider.NewInMemoryHealthTracker()
		svc := &ProviderService{health: tracker}
		now := time.Now().UTC()

		ext := &store.StoredProvider{
			ID: "ext-1", Name: "ext", ServiceType: "database",
			Type: string(v1alpha1.External), CreateTime: now, UpdateTime: now,
		}
		p := svc.toAPI(ext)
		Expect(p.Status).To(HaveValue(Equal(v1alpha1.Unhealthy)))
		Expect(p.LastCheckTime).To(BeNil())

		emb := &store.StoredProvider{
			ID: "emb-1", Name: "emb", ServiceType: "container",
			Type: string(v1alpha1.Embedded), CreateTime: now, UpdateTime: now,
		}
		p = svc.toAPI(emb)
		Expect(p.Status).To(HaveValue(Equal(v1alpha1.Ready)))
		Expect(p.LastCheckTime).To(BeNil())
	})
})

var _ = Describe("RegisterEmbedded stale removal", Label("unit"), func() {
	It("releases the registry slot when removing a stale embedded provider", func() {
		tmpDir := GinkgoT().TempDir()
		fileStore, err := store.NewFileStore(filepath.Join(tmpDir, "providers.json"))
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
