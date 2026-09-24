package validate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	containerapi "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/container/store"
	"github.com/dcm-project/environment-agent/internal/openshift/container/validate"
)

var _ = Describe("ValidateCreate", func() {
	validSpec := func() containerapi.ContainerSpec {
		return containerapi.ContainerSpec{
			Metadata: containerapi.ContainerMetadata{Name: "my-container"},
			Resources: containerapi.ContainerResources{
				Cpu:    containerapi.ContainerCpu{Min: "1000m", Max: "2000m"},
				Memory: containerapi.ContainerMemory{Min: "1GB", Max: "2GB"},
			},
		}
	}

	It("accepts a valid spec", func() {
		Expect(validate.ValidateCreate("container-1", validSpec())).To(Succeed())
	})

	It("rejects reserved container ID health", func() {
		err := validate.ValidateCreate("health", validSpec())
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("reserved"))
	})

	It("rejects cpu.min greater than cpu.max", func() {
		spec := validSpec()
		spec.Resources.Cpu = containerapi.ContainerCpu{Min: "4000m", Max: "1000m"}
		err := validate.ValidateCreate("container-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("cpu.min"))
	})

	It("accepts fractional CPU below one core", func() {
		spec := validSpec()
		spec.Resources.Cpu = containerapi.ContainerCpu{Min: "500m", Max: "1500m"}
		Expect(validate.ValidateCreate("container-1", spec)).To(Succeed())
	})

	DescribeTable("rejects invalid CPU millicore strings",
		func(cpuMin, cpuMax, expectedSubstring string) {
			spec := validSpec()
			spec.Resources.Cpu = containerapi.ContainerCpu{Min: cpuMin, Max: cpuMax}
			err := validate.ValidateCreate("container-1", spec)
			Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
			Expect(err.Error()).To(ContainSubstring(expectedSubstring))
		},
		Entry("invalid Min value", "invalid", "4000m", "cpu.min"),
		Entry("invalid Max value", "1000m", "invalid", "cpu.max"),
		Entry("empty values", "", "", "cpu.min"),
	)

	It("rejects invalid memory format", func() {
		spec := validSpec()
		spec.Resources.Memory.Min = "not-memory"
		err := validate.ValidateCreate("container-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("memory.min"))
	})

	It("rejects reserved DCM labels", func() {
		spec := validSpec()
		labels := map[string]string{dcm.LabelManagedBy: "user"}
		spec.Metadata.Labels = &labels
		err := validate.ValidateCreate("container-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("reserved by DCM"))
	})
})
