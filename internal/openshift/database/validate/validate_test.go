package validate_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	databaseapi "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"github.com/dcm-project/environment-agent/internal/openshift/database/validate"
)

var _ = Describe("ValidateCreate", func() {
	validSpec := func() databaseapi.DatabaseSpec {
		return databaseapi.DatabaseSpec{
			Metadata: databaseapi.DatabaseMetadata{Name: "my-database"},
			Resources: databaseapi.DatabaseResources{
				Cpu:    databaseapi.DatabaseCpu{Min: "1", Max: "2"},
				Memory: databaseapi.DatabaseMemory{Min: "1GB", Max: "2GB"},
			},
		}
	}

	It("accepts a valid spec", func() {
		Expect(validate.ValidateCreate("database-1", validSpec())).To(Succeed())
	})

	It("rejects reserved database ID health", func() {
		err := validate.ValidateCreate("health", validSpec())
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("reserved"))
	})

	It("rejects cpu.min greater than cpu.max", func() {
		spec := validSpec()
		spec.Resources.Cpu = databaseapi.DatabaseCpu{Min: "10", Max: "2000m"}
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("cpu.min"))
	})

	It("rejects invalid memory format", func() {
		spec := validSpec()
		spec.Resources.Memory.Min = "not-memory"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("memory.min"))
	})

	It("rejects reserved DCM labels", func() {
		spec := validSpec()
		labels := map[string]string{dcm.LabelManagedBy: "user"}
		spec.Metadata.Labels = &labels
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("reserved by DCM"))
	})
})
