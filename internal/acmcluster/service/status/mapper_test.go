package status_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStatusMapper(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Status Mapper Suite")
}
