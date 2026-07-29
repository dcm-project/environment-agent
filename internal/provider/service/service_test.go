package service

import (
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
