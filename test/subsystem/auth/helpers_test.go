//go:build subsystem

package subsystem_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

// --- Keycloak helpers ---

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}

// getClientCredentialsToken obtains an access token for the given confidential
// client via the client_credentials grant against the dcm realm.
func getClientCredentialsToken(clientID, secret string) string {
	GinkgoHelper()
	tokenURL := keycloakURL + "/realms/dcm/protocol/openid-connect/token"
	resp, err := httpClient.PostForm(tokenURL, url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {secret},
	})
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	Expect(resp.StatusCode).To(Equal(http.StatusOK), "failed to get token for client %q", clientID)

	var tokenResp tokenResponse
	Expect(json.NewDecoder(resp.Body).Decode(&tokenResp)).To(Succeed())
	Expect(tokenResp.AccessToken).NotTo(BeEmpty())
	return tokenResp.AccessToken
}

// --- HTTP request helpers ---

type requestOption func(*http.Request)

func withBearerToken(token string) requestOption {
	return func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func doRequest(method, path string, opts ...requestOption) *http.Response {
	GinkgoHelper()
	reqURL := apiURL + path
	req, err := http.NewRequest(method, reqURL, nil)
	Expect(err).NotTo(HaveOccurred())
	for _, opt := range opts {
		opt(req)
	}
	resp, err := httpClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	return resp
}

// --- RFC 9457 response helpers ---

type problemResponse struct {
	Type   v1alpha1.ErrorType `json:"type"`
	Status int                `json:"status"`
	Title  string             `json:"title"`
	Detail string             `json:"detail"`
}

func readProblemResponse(resp *http.Response) problemResponse {
	GinkgoHelper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var problem problemResponse
	Expect(json.Unmarshal(body, &problem)).To(Succeed(), "body: %s", string(body))
	return problem
}
