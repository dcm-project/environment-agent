//go:build e2e

package httpserver_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestShutdown(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E HTTP Server Shutdown Suite")
}
