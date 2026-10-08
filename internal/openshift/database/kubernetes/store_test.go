package kubernetes_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestK8sStore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cnpg Database Store Suite")
}
