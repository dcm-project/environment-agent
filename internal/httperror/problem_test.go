package httperror_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHttperror(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Httperror Suite")
}
