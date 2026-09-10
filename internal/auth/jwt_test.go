package auth_test

import (
	"context"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dcm-project/environment-agent/internal/auth"
)

var _ = Describe("NewOIDCValidator", Label("unit"), func() {
	It("panics when context is nil (UT-AUTH-015)", func() {
		Expect(func() {
			//nolint:staticcheck // intentionally passing a nil context to exercise the guard
			_, _ = auth.NewOIDCValidator(nil, "https://issuer.example.com", "aud")
		}).To(Panic())
	})

	It("does not panic on nil context check when context is provided", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(func() {
			_, _ = auth.NewOIDCValidator(ctx, "https://issuer.invalid.example", "aud")
		}).NotTo(Panic())
	})
})

var _ = Describe("JWT Token Extraction", Label("unit"), func() {
	Describe("ExtractBearerToken", func() {
		It("parses valid Authorization header (UT-AUTH-010)", func() {
			req, err := http.NewRequest(http.MethodGet, "/test", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9")

			token, err := auth.ExtractBearerToken(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"))
		})

		It("returns error when Authorization header is missing (UT-AUTH-011)", func() {
			req, err := http.NewRequest(http.MethodGet, "/test", nil)
			Expect(err).NotTo(HaveOccurred())

			token, err := auth.ExtractBearerToken(req)
			Expect(err).To(HaveOccurred())
			Expect(token).To(BeEmpty())
		})

		It("returns error for non-Bearer scheme (UT-AUTH-012)", func() {
			req, err := http.NewRequest(http.MethodGet, "/test", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Basic abc")

			token, err := auth.ExtractBearerToken(req)
			Expect(err).To(HaveOccurred())
			Expect(token).To(BeEmpty())
		})

		It("returns error for empty token after Bearer prefix (UT-AUTH-013)", func() {
			req, err := http.NewRequest(http.MethodGet, "/test", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Bearer ")

			token, err := auth.ExtractBearerToken(req)
			Expect(err).To(HaveOccurred())
			Expect(token).To(BeEmpty())
		})

		It("accepts case-insensitive scheme match (UT-AUTH-014)", func() {
			req, err := http.NewRequest(http.MethodGet, "/test", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9")

			token, err := auth.ExtractBearerToken(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(Equal("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"))
		})
	})
})
