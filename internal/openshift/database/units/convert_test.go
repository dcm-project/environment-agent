package units_test

import (
	"testing"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/units"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestUnits(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Units Suite")
}

var _ = Describe("ConvertCPU", func() {
	It("converts min/max to request/limit quantities", func() {
		cpu := v1alpha1.DatabaseCpu{Min: "1", Max: "4"}
		req, lim := units.ConvertCPU(cpu)
		Expect(req.Equal(*resource.NewQuantity(1, resource.DecimalSI))).To(BeTrue())
		Expect(lim.Equal(*resource.NewQuantity(4, resource.DecimalSI))).To(BeTrue())
	})

	It("handles zero values", func() {
		cpu := v1alpha1.DatabaseCpu{Min: "0", Max: "0"}
		req, lim := units.ConvertCPU(cpu)
		Expect(req.IsZero()).To(BeTrue())
		Expect(lim.IsZero()).To(BeTrue())
	})

	It("handles micro cpu units", func() {
		cpu := v1alpha1.DatabaseCpu{Min: "500m", Max: "4000m"}
		req, lim := units.ConvertCPU(cpu)
		Expect(req.Equal(resource.MustParse("500m"))).To(BeTrue())
		Expect(lim.Equal(*resource.NewQuantity(4, resource.DecimalSI))).To(BeTrue())
	})
})

var _ = Describe("ConvertMemory", func() {
	DescribeTable("converts valid memory strings",
		func(input string, expectedK8s string) {
			q, err := units.ConvertMemory(input)
			Expect(err).NotTo(HaveOccurred())

			expected, parseErr := resource.ParseQuantity(expectedK8s)
			Expect(parseErr).NotTo(HaveOccurred())
			Expect(q.Equal(expected)).To(BeTrue(), "expected %s, got %s", expected.String(), q.String())
		},
		Entry("megabytes", "512MB", "512Mi"),
		Entry("gigabytes", "2GB", "2Gi"),
		Entry("terabytes", "1TB", "1Ti"),
		Entry("fractional gigabytes", "1GB", "1Gi"),
		Entry("mebibytes", "512MiB", "512Mi"),
		Entry("gibibytes", "2GiB", "2Gi"),
		Entry("tebibytes", "1TiB", "1Ti"),
	)

	DescribeTable("rejects invalid memory strings",
		func(input string) {
			_, err := units.ConvertMemory(input)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported memory unit"))
		},
		Entry("no unit", "1024"),
		Entry("unsupported unit KB", "1024KB"),
		Entry("unsupported unit XB", "10XB"),
		Entry("empty string", ""),
	)
})

var _ = Describe("MemoryQuantityToAPI", func() {
	DescribeTable("converts K8s quantities to API strings",
		func(k8sStr string, expectedAPI string) {
			q, err := resource.ParseQuantity(k8sStr)
			Expect(err).NotTo(HaveOccurred())
			Expect(units.MemoryQuantityToAPI(q)).To(Equal(expectedAPI))
		},
		Entry("mebibytes to megabytes", "512Mi", "512MB"),
		Entry("gibibytes to gigabytes", "2Gi", "2GB"),
		Entry("tebibytes to terabytes", "1Ti", "1TB"),
	)

	It("falls back to raw string for unrecognized suffix", func() {
		q, err := resource.ParseQuantity("1Ki")
		Expect(err).NotTo(HaveOccurred())
		result := units.MemoryQuantityToAPI(q)
		Expect(result).To(Equal(q.String()))
	})
})
