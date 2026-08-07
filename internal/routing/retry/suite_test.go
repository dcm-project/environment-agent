package retry_test

import (
	"os"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var testNATSServer *natsserver.Server

var testStoreDir string

func TestRetry(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Retry Suite")
}

var _ = BeforeSuite(func() {
	var err error
	testStoreDir, err = os.MkdirTemp("", "nats-retry-test-*")
	Expect(err).NotTo(HaveOccurred())

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = testStoreDir
	testNATSServer = natstest.RunServer(&opts)
})

var _ = AfterSuite(func() {
	if testNATSServer != nil {
		testNATSServer.Shutdown()
	}
	if testStoreDir != "" {
		_ = os.RemoveAll(testStoreDir)
	}
})
