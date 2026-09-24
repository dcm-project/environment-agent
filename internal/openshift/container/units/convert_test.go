package units_test

import (
	"testing"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/container/units"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestUnits(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Units Suite")
}

var _ = Describe("ConvertCPU", func() {
	It("converts millicore strings to request/limit quantities", func() {
		cpu := v1alpha1.ContainerCpu{Min: "1000m", Max: "4000m"}
		req, lim, err := units.ConvertCPU(cpu)
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Equal(*resource.NewMilliQuantity(1000, resource.DecimalSI))).To(BeTrue())
		Expect(lim.Equal(*resource.NewMilliQuantity(4000, resource.DecimalSI))).To(BeTrue())
	})

	It("handles fractional CPU (sub-core)", func() {
		cpu := v1alpha1.ContainerCpu{Min: "500m", Max: "1500m"}
		req, lim, err := units.ConvertCPU(cpu)
		Expect(err).NotTo(HaveOccurred())
		Expect(req.MilliValue()).To(Equal(int64(500)))
		Expect(lim.MilliValue()).To(Equal(int64(1500)))
	})

	It("returns error for invalid Min", func() {
		cpu := v1alpha1.ContainerCpu{Min: "invalid", Max: "1000m"}
		_, _, err := units.ConvertCPU(cpu)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cpu.min"))
	})

	It("returns error for invalid Max", func() {
		cpu := v1alpha1.ContainerCpu{Min: "1000m", Max: "invalid"}
		_, _, err := units.ConvertCPU(cpu)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cpu.max"))
	})

	It("returns error for empty Min and Max", func() {
		cpu := v1alpha1.ContainerCpu{Min: "", Max: ""}
		_, _, err := units.ConvertCPU(cpu)
		Expect(err).To(HaveOccurred())
	})

	It("parses explicit zero CPU values", func() {
		cpu := v1alpha1.ContainerCpu{Min: "0m", Max: "0m"}
		req, lim, err := units.ConvertCPU(cpu)
		Expect(err).NotTo(HaveOccurred())
		Expect(req.MilliValue()).To(Equal(int64(0)))
		Expect(lim.MilliValue()).To(Equal(int64(0)))
	})
})

var _ = Describe("CPUQuantityToAPI", func() {
	It("converts whole core quantity to millicore string", func() {
		q := resource.MustParse("2")
		Expect(units.CPUQuantityToAPI(q)).To(Equal("2000m"))
	})

	It("converts millicore quantity to millicore string", func() {
		q := resource.MustParse("500m")
		Expect(units.CPUQuantityToAPI(q)).To(Equal("500m"))
	})

	It("converts zero quantity to millicore string", func() {
		q := resource.MustParse("0")
		Expect(units.CPUQuantityToAPI(q)).To(Equal("0m"))
	})

	It("converts fractional core quantity to millicore string", func() {
		q := resource.MustParse("0.5")
		Expect(units.CPUQuantityToAPI(q)).To(Equal("500m"))
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
