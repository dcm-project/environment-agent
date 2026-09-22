//go:build subsystem

package subsystem_test

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	apiURL                      string
	keycloakURL                 string
	environmentAgentSecret      string
	environmentAgentNoAudSecret string
	httpClient                  = &http.Client{Timeout: 10 * time.Second}
)

func TestSubsystem(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Auth Subsystem Suite")
}

var _ = BeforeSuite(func() {
	apiURL = envOrDefault("API_URL", "http://localhost:29080/api/v1alpha1")
	keycloakURL = envOrDefault("KEYCLOAK_URL", "http://localhost:29180")
	environmentAgentSecret = envOrDefault("ENVIRONMENT_AGENT_CLIENT_SECRET", "test-environment-agent-secret")
	environmentAgentNoAudSecret = envOrDefault("ENVIRONMENT_AGENT_NO_AUDIENCE_CLIENT_SECRET", "test-environment-agent-no-audience-secret")

	Eventually(func() error {
		resp, err := httpClient.Get(apiURL + "/health")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("health check returned %d", resp.StatusCode)
		}
		return nil
	}).WithTimeout(120 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
})

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
