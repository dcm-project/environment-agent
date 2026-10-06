package embedded_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded"
	"github.com/dcm-project/environment-agent/internal/embedded/cluster"
	"github.com/dcm-project/environment-agent/internal/embedded/container"
	"github.com/dcm-project/environment-agent/internal/embedded/network"
	"github.com/dcm-project/environment-agent/internal/embedded/storage"
	"github.com/dcm-project/environment-agent/internal/embedded/vm"
)

func TestEmbedded(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded Wiring Suite")
}

var _ = Describe("Operations", Label("unit"), func() {
	DescribeTable("every embedded SP declares the operations its capability contract implements",
		func(serviceType string, declared []string) {
			Expect(declared).To(Equal([]string{"CREATE", "READ", "DELETE"}), "service type %q", serviceType)
		},
		Entry("cluster", cluster.ServiceType, cluster.Operations),
		Entry("container", container.ServiceType, container.Operations),
		Entry("network", network.ServiceType, network.Operations),
		Entry("storage", storage.ServiceType, storage.Operations),
		Entry("vm", vm.ServiceType, vm.Operations),
	)

	It("returns nil for a nil bundle set", func() {
		Expect(embedded.Operations(nil)).To(BeNil())
	})

	It("returns nil when no embedded SP is enabled", func() {
		Expect(embedded.Operations(&embedded.Bundles{})).To(BeNil())
	})

	It("advertises the VM SP's declared operations under its service type", func() {
		ops := embedded.Operations(&embedded.Bundles{VM: &vm.Bundle{Operations: vm.Operations}})

		Expect(ops).To(HaveLen(1))
		Expect(ops[vm.ServiceType]).To(Equal([]string{"CREATE", "READ", "DELETE"}))
	})

	It("keys operations by service type for every enabled SP", func() {
		ops := embedded.Operations(&embedded.Bundles{
			Cluster:   &cluster.Bundle{Operations: cluster.Operations},
			Container: &container.Bundle{Operations: container.Operations},
			Network:   &network.Bundle{Operations: network.Operations},
			Storage:   &storage.Bundle{Operations: storage.Operations},
			VM:        &vm.Bundle{Operations: vm.Operations},
		})

		Expect(ops).To(HaveLen(5))
		for _, st := range []string{
			cluster.ServiceType,
			container.ServiceType,
			network.ServiceType,
			storage.ServiceType,
			vm.ServiceType,
		} {
			Expect(ops).To(HaveKey(st))
		}
	})

	It("skips a bundle that declares no operations", func() {
		ops := embedded.Operations(&embedded.Bundles{
			VM:      &vm.Bundle{},
			Storage: &storage.Bundle{Operations: storage.Operations},
		})

		Expect(ops).To(HaveLen(1))
		Expect(ops).To(HaveKey(storage.ServiceType))
	})
})
