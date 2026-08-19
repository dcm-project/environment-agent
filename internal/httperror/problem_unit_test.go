package httperror_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/httperror"
)

var _ = Describe("RFC 7807 Error Construction", Label("unit"), func() {
	Describe("WriteResponse", func() {
		It("constructs error body with all required fields (UT-XC-ERR-010)", func() {
			recorder := httptest.NewRecorder()
			logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
			instance := "/api/v1alpha1/providers"

			httperror.WriteResponse(
				recorder, logger, 409, "CONFLICT",
				"Conflict",
				"Service type 'database' already served by 'db-provider'",
				&instance,
			)

			Expect(recorder.Code).To(Equal(409))
			Expect(recorder.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(recorder.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("CONFLICT"))
			Expect(errBody.Title).To(Equal("Conflict"))
			Expect(errBody.Status).To(HaveValue(Equal(409)))
			Expect(errBody.Detail).To(HaveValue(Equal("Service type 'database' already served by 'db-provider'")))
			Expect(errBody.Instance).To(HaveValue(Equal("/api/v1alpha1/providers")))
		})

		It("sanitizes detail for INTERNAL errors (UT-XC-ERR-020)", func() {
			recorder := httptest.NewRecorder()
			logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

			httperror.WriteResponse(
				recorder, logger, 500, "INTERNAL",
				httperror.InternalTitle,
				"nil pointer at server.go:42",
				nil,
			)

			Expect(recorder.Code).To(Equal(500))
			Expect(recorder.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(recorder.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal("INTERNAL"))
			Expect(errBody.Status).To(HaveValue(Equal(500)))
			Expect(errBody.Detail).To(HaveValue(Equal(httperror.InternalDetail)))
			Expect(*errBody.Detail).NotTo(ContainSubstring("nil pointer"))
			Expect(*errBody.Detail).NotTo(ContainSubstring("server.go"))
		})
	})
})
