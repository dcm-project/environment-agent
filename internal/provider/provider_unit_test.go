package provider_test

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/provider"
)

var _ = Describe("Provider ID Validation", Label("unit"), func() {
	It("accepts a valid provider ID (UT-SPR-010)", func() {
		Expect(provider.ValidateProviderID("custom-001")).To(Succeed())
	})

	DescribeTable("boundary cases",
		func(id string, valid bool, errorToken string) {
			err := provider.ValidateProviderID(id)
			if valid {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(errorToken))
			}
		},
		Entry("single character 'a' is valid (UT-SPR-011)", "a", true, ""),
		Entry("63 chars is valid (UT-SPR-012)",
			strings.Repeat("a", 31)+"-"+strings.Repeat("b", 31), true, ""),
		Entry("64 chars is invalid (UT-SPR-013)",
			strings.Repeat("a", 64), false, "63"),
		Entry("uppercase is invalid (UT-SPR-014)", "INVALID", false, "lowercase"),
		Entry("special character is invalid (UT-SPR-015)", "invalid!", false, "character"),
		Entry("leading dash is invalid (UT-SPR-016)", "-starts-with-dash", false, "hyphen"),
		Entry("trailing dash is invalid (UT-SPR-017)", "ends-with-dash-", false, "hyphen"),
		Entry("empty string is invalid (UT-SPR-018)", "", false, "empty"),
		Entry("consecutive dashes are valid (UT-SPR-019)", "my--provider", true, ""),
	)
})

var _ = Describe("Schema Version Validation", Label("unit"), func() {
	It("accepts a valid schema version (UT-SPR-020)", func() {
		Expect(provider.ValidateSchemaVersion("v1alpha1")).To(Succeed())
	})

	DescribeTable("boundary cases",
		func(version string, valid bool, errorToken string) {
			err := provider.ValidateSchemaVersion(version)
			if valid {
				Expect(err).NotTo(HaveOccurred())
			} else {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(errorToken))
			}
		},
		Entry("major-only 'v1' is valid (UT-SPR-021)", "v1", true, ""),
		Entry("beta suffix is valid (UT-SPR-022)", "v2beta1", true, ""),
		Entry("multi-digit is valid (UT-SPR-023)", "v10alpha99", true, ""),
		Entry("unsupported suffix is invalid (UT-SPR-024)", "v1gamma1", false, "format"),
		Entry("missing 'v' prefix is invalid (UT-SPR-025)", "1alpha1", false, "format"),
		Entry("'v' alone is invalid (UT-SPR-026)", "v", false, "format"),
		Entry("no digit before suffix is invalid (UT-SPR-027)", "valpha1", false, "format"),
		Entry("empty string is invalid (UT-SPR-028)", "", false, "empty"),
	)
})

var _ = Describe("Slot Registry", Label("unit"), func() {
	Describe("Claim", func() {
		It("claims an unoccupied slot (UT-SPR-030)", func() {
			reg := provider.NewRegistry()

			Expect(reg.Claim("db-provider", "database")).To(Succeed())

			holder, occupied := reg.Lookup("database")
			Expect(occupied).To(BeTrue())
			Expect(holder).To(Equal("db-provider"))
		})

		It("rejects claim when slot occupied by different provider (UT-SPR-040)", func() {
			reg := provider.NewRegistry()
			Expect(reg.Claim("embedded-container", "container")).To(Succeed())

			err := reg.Claim("external-container", "container")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("embedded-container"))
		})

		It("allows same provider to re-claim same slot (UT-SPR-050)", func() {
			reg := provider.NewRegistry()
			Expect(reg.Claim("vm-provider", "vm")).To(Succeed())

			Expect(reg.Claim("vm-provider", "vm")).To(Succeed())
		})
	})

	Describe("Move", func() {
		It("moves provider to an unoccupied slot, freeing old slot (UT-SPR-060)", func() {
			reg := provider.NewRegistry()
			Expect(reg.Claim("db-provider", "database")).To(Succeed())

			Expect(reg.Move("db-provider", "database", "analytics")).To(Succeed())

			_, dbOccupied := reg.Lookup("database")
			Expect(dbOccupied).To(BeFalse())

			holder, analyticsOccupied := reg.Lookup("analytics")
			Expect(analyticsOccupied).To(BeTrue())
			Expect(holder).To(Equal("db-provider"))
		})

		It("rejects move to occupied slot, preserving original (UT-SPR-070)", func() {
			reg := provider.NewRegistry()
			Expect(reg.Claim("db-provider", "database")).To(Succeed())
			Expect(reg.Claim("other-provider", "analytics")).To(Succeed())

			err := reg.Move("db-provider", "database", "analytics")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("other-provider"))

			holder, occupied := reg.Lookup("database")
			Expect(occupied).To(BeTrue())
			Expect(holder).To(Equal("db-provider"))
		})

		// Regression coverage: Move previously did an unconditional
		// delete(r.slots, oldType) without verifying the caller actually
		// holds oldType. If registry/store ever desync, this let a provider
		// "moving" out of a service type it doesn't actually hold silently
		// delete a *different* provider's active slot for that type —
		// violating the single-slot-per-service-type invariant (REQ-SPR-200).
		It("rejects move when oldType is held by a different provider, leaving that provider's slot intact (UT-SPR-072)", func() {
			reg := provider.NewRegistry()
			Expect(reg.Claim("real-holder", "database")).To(Succeed())

			err := reg.Move("impostor", "database", "analytics")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("real-holder"))

			holder, occupied := reg.Lookup("database")
			Expect(occupied).To(BeTrue())
			Expect(holder).To(Equal("real-holder"), "the real holder's slot must survive an impostor's Move call")

			_, analyticsOccupied := reg.Lookup("analytics")
			Expect(analyticsOccupied).To(BeFalse(), "newType must not be claimed when the oldType ownership check fails")
		})

		It("allows move when oldType is unoccupied (no ownership conflict possible) (UT-SPR-073)", func() {
			reg := provider.NewRegistry()

			Expect(reg.Move("new-provider", "unclaimed-type", "target")).To(Succeed())

			holder, occupied := reg.Lookup("target")
			Expect(occupied).To(BeTrue())
			Expect(holder).To(Equal("new-provider"))
		})
	})
})

var _ = Describe("Provider ID Generation", Label("unit"), func() {
	It("generates a valid UUID v4 that passes AEP-122 validation (UT-SPR-080)", func() {
		id := provider.GenerateProviderID()
		Expect(id).To(MatchRegexp(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`))
		Expect(provider.ValidateProviderID(id)).To(Succeed())
	})
})
