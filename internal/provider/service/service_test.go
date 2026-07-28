package service

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
