package baremetal_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBareMetal(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "BareMetal Service Suite")
}
