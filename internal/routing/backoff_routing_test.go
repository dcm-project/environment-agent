package routing_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/backoff"
)

var _ = Describe("Routing Retry Backoff", Label("unit"), func() {
	const (
		retryBackoff    = 2 * time.Second
		retryMaxBackoff = 30 * time.Second
	)

	DescribeTable("uses same formula with routing parameters",
		func(attempt int, expected time.Duration) {
			Expect(backoff.CalculateBackoff(retryBackoff, retryMaxBackoff, attempt)).To(Equal(expected))
		},
		Entry("attempt 4 → capped at 30s (UT-RTE-050)", 4, 30*time.Second),
		Entry("attempt 0 → 2s (UT-RTE-051)", 0, 2*time.Second),
		Entry("attempt 3 → 16s (UT-RTE-052)", 3, 16*time.Second),
		Entry("attempt 10 → capped at 30s (UT-RTE-053)", 10, 30*time.Second),
	)
})
