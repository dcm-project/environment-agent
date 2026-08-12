package dcm_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/dcm"
)

var _ = Describe("ParseRetryAfter", Label("unit"), func() {
	It("parses seconds format '120' → 120s (UT-DCM-030)", func() {
		d, ok := dcm.ParseRetryAfter("120", time.Now())
		Expect(ok).To(BeTrue())
		Expect(d).To(Equal(120 * time.Second))
	})

	It("parses HTTP-date format (UT-DCM-031)", func() {
		now := time.Date(2025, 12, 1, 15, 55, 0, 0, time.UTC)
		d, ok := dcm.ParseRetryAfter("Thu, 01 Dec 2025 16:00:00 GMT", now)
		Expect(ok).To(BeTrue())
		Expect(d).To(Equal(5 * time.Minute))
	})

	It("parses '0' → 0s (UT-DCM-032)", func() {
		d, ok := dcm.ParseRetryAfter("0", time.Now())
		Expect(ok).To(BeTrue())
		Expect(d).To(BeZero())
	})

	It("returns (0, false) for invalid value (UT-DCM-033)", func() {
		d, ok := dcm.ParseRetryAfter("abc", time.Now())
		Expect(ok).To(BeFalse())
		Expect(d).To(Equal(time.Duration(0)))
	})

	It("returns (0, false) for empty string (UT-DCM-034)", func() {
		d, ok := dcm.ParseRetryAfter("", time.Now())
		Expect(ok).To(BeFalse())
		Expect(d).To(Equal(time.Duration(0)))
	})

	It("returns (0, false) for whitespace-only (UT-DCM-035)", func() {
		d, ok := dcm.ParseRetryAfter("   ", time.Now())
		Expect(ok).To(BeFalse())
		Expect(d).To(Equal(time.Duration(0)))
	})

	It("returns (0, false) for negative seconds (UT-DCM-036)", func() {
		d, ok := dcm.ParseRetryAfter("-5", time.Now())
		Expect(ok).To(BeFalse())
		Expect(d).To(Equal(time.Duration(0)))
	})

	It("handles very large seconds without overflow (UT-DCM-037)", func() {
		d, ok := dcm.ParseRetryAfter("999999999", time.Now())
		Expect(ok).To(BeTrue())
		Expect(d).To(Equal(999999999 * time.Second))
	})

	It("returns (0, false) for an HTTP-date in the past (UT-DCM-038)", func() {
		now := time.Date(2025, 12, 1, 15, 55, 0, 0, time.UTC)
		d, ok := dcm.ParseRetryAfter("Thu, 01 Dec 2025 15:50:00 GMT", now)
		Expect(ok).To(BeFalse())
		Expect(d).To(Equal(time.Duration(0)))
	})

	It("returns (0, false) for an HTTP-date exactly equal to now (UT-DCM-039)", func() {
		// http.ParseTime truncates to whole seconds (RFC1123 has no
		// sub-second precision), so `now` must also be whole-seconds to
		// land exactly on the d > 0 boundary rather than the past-date one.
		now := time.Date(2025, 12, 1, 15, 55, 0, 0, time.UTC)
		d, ok := dcm.ParseRetryAfter("Mon, 01 Dec 2025 15:55:00 GMT", now)
		Expect(ok).To(BeFalse())
		Expect(d).To(Equal(time.Duration(0)))
	})
})
