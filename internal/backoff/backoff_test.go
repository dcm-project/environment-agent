package backoff_test

import (
	"math"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/backoff"
)

var _ = Describe("CalculateBackoff", Label("unit"), func() {
	DescribeTable("computes min(initial × 2^attempt, max)",
		func(initial, max time.Duration, attempt int, expected time.Duration) {
			Expect(backoff.CalculateBackoff(initial, max, attempt)).To(Equal(expected))
		},
		Entry("attempt 0 → initial (UT-DCM-011)", time.Second, 5*time.Minute, 0, time.Second),
		Entry("attempt 1 → 2s (UT-DCM-012)", time.Second, 5*time.Minute, 1, 2*time.Second),
		Entry("attempt 3 → 8s (UT-DCM-010)", time.Second, 5*time.Minute, 3, 8*time.Second),
		Entry("attempt 9 → capped at 300s (UT-DCM-013)", time.Second, 5*time.Minute, 9, 5*time.Minute),
		Entry("attempt 20 → still capped, overflow-safe (UT-DCM-014)", time.Second, 5*time.Minute, 20, 5*time.Minute),
		// This attempt magnitude, against a small max, terminates within a
		// handful of loop iterations regardless (d exceeds max long before
		// attempt is exhausted) — it proves there's no O(attempt) hang, but
		// on its own doesn't exercise the overflow guard near its actual
		// int64 boundary. See UT-DCM-016/017 below for that.
		Entry("attempt 1_000_000 → still capped, no float/overflow (UT-DCM-015)", time.Second, 5*time.Minute, 1_000_000, 5*time.Minute),
		Entry("non-positive initial → returns max directly (UT-DCM-016)", time.Duration(0), 5*time.Minute, 3, 5*time.Minute),
		Entry("negative initial → returns max directly (UT-DCM-016b)", -time.Second, 5*time.Minute, 3, 5*time.Minute),
		// Exercises the d > max/2 overflow guard at actual int64-Duration
		// scale (max ≈ 146 years) rather than a small toy value: 2^61 is
		// the first power of two exceeding max/2, so the guard must fire
		// at exactly that doubling to avoid d ever reaching 2^62 (which
		// would still fit in int64, but the point is to prove the guard's
		// boundary math holds at real magnitudes, not just small ones).
		Entry("many doublings against a near-int64-max cap → guard fires before overflow (UT-DCM-017)",
			time.Nanosecond, time.Duration(math.MaxInt64/2), 1000, time.Duration(math.MaxInt64/2)),
	)
})

var _ = Describe("ApplyJitter", Label("unit"), func() {
	It("with rand=0.0 returns 0 (UT-DCM-021)", func() {
		result := backoff.ApplyJitter(8*time.Second, func() float64 { return 0.0 })
		Expect(result).To(Equal(time.Duration(0)))
	})

	It("with rand=1.0 returns full interval (UT-DCM-022)", func() {
		result := backoff.ApplyJitter(8*time.Second, func() float64 { return 1.0 })
		Expect(result).To(Equal(8 * time.Second))
	})

	It("with rand=0.5 and calculated=8s returns 4s (UT-DCM-023)", func() {
		result := backoff.ApplyJitter(8*time.Second, func() float64 { return 0.5 })
		Expect(result).To(Equal(4 * time.Second))
	})

	It("produces value in valid range [0, calculated] (UT-DCM-020)", func() {
		calculated := 8 * time.Second
		result := backoff.ApplyJitter(calculated, func() float64 { return 0.73 })
		Expect(result).To(BeNumerically(">=", 0))
		Expect(result).To(BeNumerically("<=", calculated))
	})
})
