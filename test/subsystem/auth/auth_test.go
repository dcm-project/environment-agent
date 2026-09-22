//go:build subsystem

package subsystem_test

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Real Keycloak JWT authentication (AC-AUTH-130)", func() {
	It("accepts a valid client_credentials token from the environment-agent client (ST-AUTH-010)", func() {
		token := getClientCredentialsToken("environment-agent", environmentAgentSecret)

		resp := doRequest(http.MethodGet, "/providers", withBearerToken(token))
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	It("rejects a request with no Authorization header (ST-AUTH-020)", func() {
		resp := doRequest(http.MethodGet, "/providers")
		Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
		Expect(resp.Header.Get("WWW-Authenticate")).To(Equal("Bearer"))

		problem := readProblemResponse(resp)
		Expect(problem.Type).To(Equal("UNAUTHORIZED"))
		Expect(problem.Detail).To(Equal("invalid Bearer token"))
	})

	It("rejects a tampered (signature-invalidated) token (ST-AUTH-030)", func() {
		token := getClientCredentialsToken("environment-agent", environmentAgentSecret)
		tampered := token[:len(token)-4] + "XXXX"

		resp := doRequest(http.MethodGet, "/providers", withBearerToken(tampered))
		Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

		problem := readProblemResponse(resp)
		Expect(problem.Detail).To(Equal("invalid Bearer token"))
	})

	It("rejects a validly-signed token whose audience does not match AGENT_AUTH_JWT_AUDIENCE (ST-AUTH-040)", func() {
		token := getClientCredentialsToken("environment-agent-no-audience", environmentAgentNoAudSecret)

		resp := doRequest(http.MethodGet, "/providers", withBearerToken(token))
		Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))

		problem := readProblemResponse(resp)
		Expect(problem.Detail).To(Equal("invalid Bearer token"))
	})
})
