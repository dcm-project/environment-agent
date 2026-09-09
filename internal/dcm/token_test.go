package dcm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/dcm"
)

var _ = Describe("TokenSource", Label("unit"), func() {
	Describe("StaticTokenSource", func() {
		It("returns the configured token (TC-DCM-UT-AUTH-010)", func() {
			ts := dcm.NewStaticTokenSource("my-jwt")
			token, err := ts.Token(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("my-jwt"))
		})
	})

	Describe("ClientCredentialsTokenSource", func() {
		var (
			tokenEndpoint *httptest.Server
			callCount     atomic.Int32
		)

		newTokenEndpoint := func(accessToken string, expiresIn int) *httptest.Server {
			return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				callCount.Add(1)
				Expect(r.Method).To(Equal(http.MethodPost))
				Expect(r.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded"))

				Expect(r.ParseForm()).To(Succeed())
				Expect(r.FormValue("grant_type")).To(Equal("client_credentials"))

				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"access_token": accessToken,
					"token_type":   "Bearer",
					"expires_in":   expiresIn,
				}
				Expect(json.NewEncoder(w).Encode(resp)).To(Succeed())
			}))
		}

		BeforeEach(func() {
			callCount.Store(0)
		})

		AfterEach(func() {
			if tokenEndpoint != nil {
				tokenEndpoint.Close()
			}
		})

		It("fetches from token endpoint (TC-DCM-UT-AUTH-020)", func() {
			tokenEndpoint = newTokenEndpoint("fetched-jwt", 3600)

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			token, err := ts.Token(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("fetched-jwt"))
			Expect(callCount.Load()).To(Equal(int32(1)))
		})

		It("caches token, reuses on second call (TC-DCM-UT-AUTH-030)", func() {
			tokenEndpoint = newTokenEndpoint("cached-jwt", 3600)

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			token1, err := ts.Token(context.Background())
			Expect(err).NotTo(HaveOccurred())

			token2, err := ts.Token(context.Background())
			Expect(err).NotTo(HaveOccurred())

			Expect(token1).To(Equal("cached-jwt"))
			Expect(token2).To(Equal("cached-jwt"))
			Expect(callCount.Load()).To(Equal(int32(1)))
		})

		It("refreshes when cached token is near expiry (TC-DCM-UT-AUTH-040)", func() {
			var tokenSeq atomic.Int32
			tokenEndpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callCount.Add(1)
				seq := tokenSeq.Add(1)
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"access_token": fmt.Sprintf("token-%d", seq),
					"token_type":   "Bearer",
					"expires_in":   1,
				}
				Expect(json.NewEncoder(w).Encode(resp)).To(Succeed())
			}))

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			_, err := ts.Token(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(callCount.Load()).To(Equal(int32(1)))

			time.Sleep(2 * time.Second)

			_, err = ts.Token(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(callCount.Load()).To(Equal(int32(2)))
		})

		It("returns error on endpoint failure (TC-DCM-UT-AUTH-050)", func() {
			tokenEndpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callCount.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			token, err := ts.Token(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(token).To(BeEmpty())
		})

		DescribeTable("rejects non-positive expires_in without caching it (TC-DCM-UT-AUTH-070)",
			func(expiresIn int) {
				tokenEndpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					callCount.Add(1)
					w.Header().Set("Content-Type", "application/json")
					resp := map[string]interface{}{
						"access_token": "bad-expiry-jwt",
						"token_type":   "Bearer",
						"expires_in":   expiresIn,
					}
					Expect(json.NewEncoder(w).Encode(resp)).To(Succeed())
				}))

				ts := dcm.NewClientCredentialsTokenSource(
					tokenEndpoint.URL, "my-client", "my-secret", nil,
				)

				token, err := ts.Token(context.Background())
				Expect(err).To(HaveOccurred())
				Expect(token).To(BeEmpty())
				Expect(callCount.Load()).To(Equal(int32(1)))

				// A rejected response must not be cached — a second call must
				// hit the endpoint again rather than reusing a bad entry.
				_, err = ts.Token(context.Background())
				Expect(err).To(HaveOccurred())
				Expect(callCount.Load()).To(Equal(int32(2)))
			},
			Entry("zero", 0),
			Entry("negative", -1),
		)

		It("rejects missing expires_in without caching it (TC-DCM-UT-AUTH-070)", func() {
			tokenEndpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callCount.Add(1)
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"access_token": "no-expiry-jwt",
					"token_type":   "Bearer",
				}
				Expect(json.NewEncoder(w).Encode(resp)).To(Succeed())
			}))

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			token, err := ts.Token(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(token).To(BeEmpty())
		})

		It("rejects an expires_in that overflows time.Duration (TC-DCM-UT-AUTH-080)", func() {
			tokenEndpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callCount.Add(1)
				w.Header().Set("Content-Type", "application/json")
				resp := map[string]interface{}{
					"access_token": "overflow-jwt",
					"token_type":   "Bearer",
					"expires_in":   int64(math.MaxInt64),
				}
				Expect(json.NewEncoder(w).Encode(resp)).To(Succeed())
			}))

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			token, err := ts.Token(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(token).To(BeEmpty())
		})

		It("is thread-safe (TC-DCM-UT-AUTH-060)", func() {
			tokenEndpoint = newTokenEndpoint("concurrent-jwt", 3600)

			ts := dcm.NewClientCredentialsTokenSource(
				tokenEndpoint.URL, "my-client", "my-secret", nil,
			)

			const goroutines = 10
			var wg sync.WaitGroup
			wg.Add(goroutines)
			errs := make(chan error, goroutines)

			for range goroutines {
				go func() {
					defer wg.Done()
					_, err := ts.Token(context.Background())
					if err != nil {
						errs <- err
					}
				}()
			}

			wg.Wait()
			close(errs)

			for err := range errs {
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
})
