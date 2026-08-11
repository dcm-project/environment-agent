package retry_test

import (
	"context"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/messaging"
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

	// Simulates the control-plane-owned dcm-agent-requests stream: the agent
	// only creates durable consumers on it, never the stream itself. Created
	// once for the suite; tests derive unique subjects under dcm.agent.>.
	conn, err := nats.Connect(testNATSServer.ClientURL())
	Expect(err).NotTo(HaveOccurred())
	defer conn.Close()
	js, err := jetstream.New(conn)
	Expect(err).NotTo(HaveOccurred())
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer setupCancel()
	_, err = js.CreateOrUpdateStream(setupCtx, jetstream.StreamConfig{
		Name: messaging.RequestStreamName, Subjects: []string{"dcm.agent.>"},
	})
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if testNATSServer != nil {
		testNATSServer.Shutdown()
	}
	if testStoreDir != "" {
		_ = os.RemoveAll(testStoreDir)
	}
})
