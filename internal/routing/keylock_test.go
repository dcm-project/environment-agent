package routing_test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/routing"
)

var _ = Describe("KeyLock", Label("unit"), func() {
	var kl *routing.KeyLock

	BeforeEach(func() {
		kl = routing.NewKeyLock()
	})

	It("never evicts regardless of insertion count (UT-RTE-045)", func() {
		for i := 0; i < 5000; i++ {
			Expect(kl.AddIfAbsent(fmt.Sprintf("res-%d", i))).To(BeTrue())
		}
		Expect(kl.Len()).To(Equal(5000))
	})

	It("returns false for an already-held key and keeps it held exactly once (UT-RTE-045)", func() {
		Expect(kl.AddIfAbsent("res-001")).To(BeTrue())
		Expect(kl.AddIfAbsent("res-001")).To(BeFalse())
		Expect(kl.Len()).To(Equal(1))
	})

	It("allows re-claiming a key after Remove (UT-RTE-045)", func() {
		Expect(kl.AddIfAbsent("res-001")).To(BeTrue())
		kl.Remove("res-001")
		Expect(kl.AddIfAbsent("res-001")).To(BeTrue())
	})
})
