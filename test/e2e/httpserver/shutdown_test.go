//go:build e2e

package httpserver_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var binaryPath string

var _ = BeforeSuite(func() {
	tmpDir := GinkgoT().TempDir()
	binaryPath = filepath.Join(tmpDir, "environment-agent")

	cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/environment-agent")
	cmd.Dir = findRepoRoot()
	out, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "failed to build binary: %s", string(out))
})

func findRepoRoot() string {
	dir, err := os.Getwd()
	Expect(err).NotTo(HaveOccurred())
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			Fail("could not find repository root (go.mod)")
		}
		dir = parent
	}
}

func waitForReady(port string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		client := &http.Client{Timeout: 200 * time.Millisecond}
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%s/api/v1alpha1/health", port))
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("server at :%s did not become ready within %s", port, timeout)
}

// startAgent launches the agent binary with AGENT_SERVER_ADDRESS=:0 and
// discovers the OS-assigned port by parsing the JSON "server listening"
// log line from stdout. It returns the running command and the port string.
func startAgent(extraEnv ...string) (*exec.Cmd, string) {
	cmd := exec.Command(binaryPath)
	cmd.Env = append(os.Environ(), "AGENT_SERVER_ADDRESS=:0")
	cmd.Env = append(cmd.Env, extraEnv...)

	stdout, err := cmd.StdoutPipe()
	Expect(err).NotTo(HaveOccurred())
	cmd.Stderr = GinkgoWriter

	Expect(cmd.Start()).To(Succeed())

	addrCh := make(chan string, 1)
	go func() {
		defer GinkgoRecover()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(GinkgoWriter, line)

			var entry struct {
				Msg     string `json:"msg"`
				Address string `json:"address"`
			}
			if json.Unmarshal([]byte(line), &entry) == nil && entry.Msg == "server listening" {
				_, port, err := net.SplitHostPort(entry.Address)
				Expect(err).NotTo(HaveOccurred(), "parsing address from log line")
				addrCh <- port
			}
		}
	}()

	var port string
	select {
	case port = <-addrCh:
	case <-time.After(10 * time.Second):
		Fail("timed out waiting for server to emit listening address")
	}

	return cmd, port
}

// holdPartialRequest opens a raw TCP connection and sends an HTTP POST
// with complete headers but no body, keeping the server-side handler
// blocked on body read. Returns the connection and the body to send later.
func holdPartialRequest(port string) (net.Conn, string) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
	Expect(err).NotTo(HaveOccurred())

	body := `{"name":"drain-test","endpoint":"http://test.local","service_type":"drain","schema_version":"v1alpha1"}`
	headers := fmt.Sprintf(
		"POST /api/v1alpha1/providers HTTP/1.1\r\n"+
			"Host: 127.0.0.1:%s\r\n"+
			"Content-Type: application/json\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n",
		port, len(body))

	_, err = conn.Write([]byte(headers))
	Expect(err).NotTo(HaveOccurred())

	return conn, body
}

func waitForExit(cmd *exec.Cmd, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		Fail(fmt.Sprintf("process did not exit within %s", timeout))
		return nil
	}
}

var _ = Describe("HTTP Server Graceful Shutdown", func() {

	It("drains in-flight requests on SIGTERM and exits 0 (IT-HTTP-030)", func() {
		cmd, port := startAgent()
		DeferCleanup(func() { cmd.Process.Kill() })

		Expect(waitForReady(port, 5*time.Second)).To(Succeed(),
			"server must start and be ready before signal test")

		conn, body := holdPartialRequest(port)
		DeferCleanup(func() { conn.Close() })

		Expect(cmd.Process.Signal(syscall.SIGTERM)).To(Succeed())

		time.Sleep(50 * time.Millisecond)
		_, err := conn.Write([]byte(body))
		Expect(err).NotTo(HaveOccurred(), "completing the in-flight body must succeed")

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp := make([]byte, 4096)
		n, readErr := conn.Read(resp)
		Expect(readErr).NotTo(HaveOccurred(), "in-flight request must receive a response (drain)")
		Expect(n).To(BeNumerically(">", 0))
		Expect(string(resp[:n])).To(MatchRegexp(`HTTP/1\.1 \d{3}`),
			"drained response must be valid HTTP")

		Expect(waitForExit(cmd, 10*time.Second)).NotTo(HaveOccurred(),
			"process should exit with code 0")
	})

	It("drains in-flight requests on SIGINT and exits 0 (IT-HTTP-040)", func() {
		cmd, port := startAgent()
		DeferCleanup(func() { cmd.Process.Kill() })

		Expect(waitForReady(port, 5*time.Second)).To(Succeed(),
			"server must start and be ready before signal test")

		conn, body := holdPartialRequest(port)
		DeferCleanup(func() { conn.Close() })

		Expect(cmd.Process.Signal(syscall.SIGINT)).To(Succeed())

		time.Sleep(50 * time.Millisecond)
		_, err := conn.Write([]byte(body))
		Expect(err).NotTo(HaveOccurred(), "completing the in-flight body must succeed")

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp := make([]byte, 4096)
		n, readErr := conn.Read(resp)
		Expect(readErr).NotTo(HaveOccurred(), "in-flight request must receive a response (drain)")
		Expect(n).To(BeNumerically(">", 0))
		Expect(string(resp[:n])).To(MatchRegexp(`HTTP/1\.1 \d{3}`),
			"drained response must be valid HTTP")

		Expect(waitForExit(cmd, 10*time.Second)).NotTo(HaveOccurred(),
			"process should exit with code 0")
	})

	It("closes in-flight connections after shutdown timeout (IT-HTTP-050)", func() {
		cmd, port := startAgent("AGENT_SERVER_SHUTDOWN_TIMEOUT=1s")
		DeferCleanup(func() { cmd.Process.Kill() })

		Expect(waitForReady(port, 5*time.Second)).To(Succeed(),
			"server must start and be ready before timeout test")

		conn, _ := holdPartialRequest(port)
		DeferCleanup(func() { conn.Close() })

		Expect(cmd.Process.Signal(syscall.SIGTERM)).To(Succeed())

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp := make([]byte, 4096)
		_, readErr := conn.Read(resp)
		Expect(readErr).To(HaveOccurred(),
			"in-flight connection must be terminated after shutdown timeout")

		Expect(waitForExit(cmd, 10*time.Second)).NotTo(HaveOccurred(),
			"process should exit with code 0 after forced close")
	})
})
