package embedded_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/embedded"
	"github.com/dcm-project/environment-agent/internal/embedded/cluster"
	"github.com/dcm-project/environment-agent/internal/embedded/container"
	"github.com/dcm-project/environment-agent/internal/embedded/network"
	"github.com/dcm-project/environment-agent/internal/embedded/storage"
	"github.com/dcm-project/environment-agent/internal/embedded/vm"
	"github.com/dcm-project/environment-agent/internal/routing"
)

func TestEmbedded(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embedded Wiring Suite")
}

type stubHandler struct{}

func (stubHandler) CreateResource(context.Context, routing.CreateResourceRequest) error { return nil }
func (stubHandler) DeleteResource(context.Context, routing.DeleteResourceRequest) error { return nil }

var _ = Describe("Operations", Label("unit"), func() {
	It("returns nil for a nil bundle set", func() {
		Expect(embedded.Operations(nil)).To(BeNil())
	})

	It("returns nil when no embedded SP is enabled", func() {
		Expect(embedded.Operations(&embedded.Bundles{})).To(BeNil())
	})

	It("advertises CREATE, READ and DELETE for the VM SP", func() {
		ops := embedded.Operations(&embedded.Bundles{VM: &vm.Bundle{Handler: stubHandler{}}})

		Expect(ops).To(HaveLen(1))
		Expect(ops[vm.ServiceType]).To(Equal([]string{"CREATE", "READ", "DELETE"}))
	})

	It("keys operations by service type for every enabled SP", func() {
		ops := embedded.Operations(&embedded.Bundles{
			Cluster:   &cluster.Bundle{Handler: stubHandler{}},
			Container: &container.Bundle{Handler: stubHandler{}},
			Network:   &network.Bundle{Handler: stubHandler{}},
			Storage:   &storage.Bundle{Handler: stubHandler{}},
			VM:        &vm.Bundle{Handler: stubHandler{}},
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
		for st, declared := range ops {
			Expect(declared).To(ConsistOf("CREATE", "READ", "DELETE"), "service type %q", st)
		}
	})

	It("skips a bundle that was constructed without a handler", func() {
		ops := embedded.Operations(&embedded.Bundles{
			VM:      &vm.Bundle{},
			Storage: &storage.Bundle{Handler: stubHandler{}},
		})

		Expect(ops).To(HaveLen(1))
		Expect(ops).To(HaveKey(storage.ServiceType))
	})
})
