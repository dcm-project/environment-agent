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

var _ = Describe("RFC 7807 Error Construction", func() {
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

	Describe("StatusForType", func() {
		DescribeTable(
			"maps error types to HTTP status codes (UT-XC-ERR-030)",
			func(errType string, expectedStatus int) {
				Expect(httperror.StatusForType(errType)).To(Equal(expectedStatus))
			},
			Entry("INVALID_ARGUMENT → 400", "INVALID_ARGUMENT", 400),
			Entry("UNAUTHORIZED → 401", "UNAUTHORIZED", 401),
			Entry("NOT_FOUND → 404", "NOT_FOUND", 404),
			Entry("CONFLICT → 409", "CONFLICT", 409),
			Entry("UNPROCESSABLE_ENTITY → 422", "UNPROCESSABLE_ENTITY", 422),
			Entry("INTERNAL → 500", "INTERNAL", 500),
			Entry("UNAVAILABLE → 503", "UNAVAILABLE", 503),
		)
	})

	Describe("PanicToErrorBody", func() {
		It("converts panic value to INTERNAL error body (UT-XC-ERR-040)", func() {
			errBody := httperror.PanicToErrorBody("unexpected nil deref")

			Expect(errBody.Type).To(Equal("INTERNAL"))
			Expect(errBody.Status).To(HaveValue(Equal(500)))
			Expect(errBody.Detail).NotTo(BeNil())
			Expect(*errBody.Detail).NotTo(ContainSubstring("unexpected nil deref"))
			Expect(*errBody.Detail).To(Equal(httperror.InternalDetail))
		})
	})
})
