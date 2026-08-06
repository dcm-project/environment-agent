package dcm_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDCM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DCM Suite")
}
