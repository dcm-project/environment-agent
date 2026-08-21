package kubevirtprovider_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKubeVirt(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "KubeVirt Service Suite")
}
