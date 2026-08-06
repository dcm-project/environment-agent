package cloudevent_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCloudevent(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "CloudEvent Suite")
}
