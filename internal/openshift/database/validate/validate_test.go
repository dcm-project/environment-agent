package validate_test

import (
	"strings"

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
				Cpu:     databaseapi.DatabaseCpu{Min: "1", Max: "2"},
				Memory:  databaseapi.DatabaseMemory{Min: "1GB", Max: "2GB"},
				Storage: "30GB",
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

	It("rejects an uppercase metadata.name", func() {
		spec := validSpec()
		spec.Metadata.Name = "MyDatabase"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("metadata.name"))
	})

	It("rejects a dotted metadata.name", func() {
		spec := validSpec()
		spec.Metadata.Name = "my.database"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("metadata.name"))
	})

	It("rejects a metadata.name longer than 63 characters", func() {
		spec := validSpec()
		spec.Metadata.Name = "a" + strings.Repeat("b", 63) // 64 chars
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("metadata.name"))
	})

	It("rejects an invalid resource ID", func() {
		err := validate.ValidateCreate("INVALID_ID", validSpec())
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("database ID"))
	})

	It("rejects a resource ID longer than 63 characters", func() {
		longID := "a" + strings.Repeat("b", 63) // 64 chars
		err := validate.ValidateCreate(longID, validSpec())
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("database ID"))
	})

	It("rejects an unrecognised storage unit", func() {
		spec := validSpec()
		spec.Resources.Storage = "not-storage"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("storage"))
	})

	It("rejects zero storage", func() {
		spec := validSpec()
		spec.Resources.Storage = "0GB"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("storage"))
	})

	It("rejects missing storage", func() {
		spec := validSpec()
		spec.Resources.Storage = ""
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("storage"))
	})

	It("rejects negative cpu.min", func() {
		spec := validSpec()
		spec.Resources.Cpu.Min = "-1"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("cpu.min"))
	})

	It("rejects negative cpu.max", func() {
		spec := validSpec()
		spec.Resources.Cpu.Max = "-1"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("cpu.max"))
	})

	It("rejects negative memory.min", func() {
		spec := validSpec()
		spec.Resources.Memory.Min = "-1GB"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("memory.min"))
	})

	It("rejects negative memory.max", func() {
		spec := validSpec()
		spec.Resources.Memory.Max = "-1GB"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("memory.max"))
	})

	It("rejects negative storage", func() {
		spec := validSpec()
		spec.Resources.Storage = "-10GB"
		err := validate.ValidateCreate("database-1", spec)
		Expect(err).To(BeAssignableToTypeOf(&store.InvalidArgumentError{}))
		Expect(err.Error()).To(ContainSubstring("storage"))
	})
})
