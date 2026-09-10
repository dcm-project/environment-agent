package v1alpha1_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
)

func TestDatabase(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Database Suite")
}

var _ = Describe("PostPath", func() {
	It("returns the expected database path without error", func() {
		got, err := v1alpha1.PostPath()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).NotTo(BeEmpty())
		Expect(got).To(Equal("/api/v1alpha1/databases"))
	})
})
