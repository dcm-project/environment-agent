package routing_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/routing"
)

var _ = Describe("ResourceSet", Label("unit"), func() {
	var dl *routing.ResourceSet

	Describe("Add and Contains", func() {
		BeforeEach(func() {
			dl = routing.NewResourceSet(3)
		})

		It("reports added entry as present and absent entry as not present (UT-RTE-010)", func() {
			dl.Add("res-001")
			Expect(dl.Contains("res-001")).To(BeTrue())
			Expect(dl.Contains("res-999")).To(BeFalse())
		})
	})

	Describe("Consume-on-use", func() {
		BeforeEach(func() {
			dl = routing.NewResourceSet(3)
			dl.Add("res-001")
		})

		It("removes entry on consume and returns true (UT-RTE-020)", func() {
			Expect(dl.Consume("res-001")).To(BeTrue())
			Expect(dl.Contains("res-001")).To(BeFalse())
		})
	})

	Describe("LRU eviction at capacity", func() {
		BeforeEach(func() {
			dl = routing.NewResourceSet(3)
			dl.Add("res-001")
			dl.Add("res-002")
			dl.Add("res-003")
		})

		It("evicts oldest entry when capacity exceeded (UT-RTE-030)", func() {
			dl.Add("res-004")
			Expect(dl.Contains("res-001")).To(BeFalse())
			Expect(dl.Contains("res-002")).To(BeTrue())
			Expect(dl.Contains("res-003")).To(BeTrue())
			Expect(dl.Contains("res-004")).To(BeTrue())
		})
	})

	Describe("Access refreshes LRU position", func() {
		BeforeEach(func() {
			dl = routing.NewResourceSet(3)
			dl.Add("res-001")
			dl.Add("res-002")
			dl.Add("res-003")
		})

		It("accessed entry survives eviction, oldest-after-access evicted (UT-RTE-040)", func() {
			Expect(dl.Contains("res-001")).To(BeTrue()) // refresh res-001
			dl.Add("res-004")                           // should evict res-002 (now oldest)
			Expect(dl.Contains("res-001")).To(BeTrue())
			Expect(dl.Contains("res-002")).To(BeFalse())
			Expect(dl.Contains("res-003")).To(BeTrue())
			Expect(dl.Contains("res-004")).To(BeTrue())
		})
	})

	Describe("Unbounded set never evicts", func() {
		BeforeEach(func() {
			dl = routing.NewUnboundedResourceSet()
		})

		It("retains all entries regardless of insertion count (UT-RTE-050)", func() {
			for i := 0; i < 5000; i++ {
				dl.Add(fmt.Sprintf("res-%d", i))
			}
			Expect(dl.Contains("res-0")).To(BeTrue())
			Expect(dl.Contains("res-4999")).To(BeTrue())
			Expect(dl.Len()).To(Equal(5000))
		})
	})
})
