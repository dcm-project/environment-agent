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

var _ = Describe("RFC 9457 Error Construction", Label("unit"), func() {
	Describe("WriteType", func() {
		It("constructs error body with all required fields (UT-XC-ERR-010)", func() {
			recorder := httptest.NewRecorder()
			logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
			instance := "/api/v1alpha1/providers"

			httperror.WriteType(
				recorder, logger, v1alpha1.ErrorTypeALREADYEXISTS,
				"Service type 'database' already served by 'db-provider'",
				&instance,
			)

			Expect(recorder.Code).To(Equal(409))
			Expect(recorder.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(recorder.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal(v1alpha1.ErrorTypeALREADYEXISTS))
			Expect(string(errBody.Type)).To(Equal("https://dcm-project.github.io/problems/already-exists"))
			Expect(errBody.Title).To(Equal("Already exists"))
			Expect(errBody.Status).To(HaveValue(Equal(409)))
			Expect(errBody.Detail).To(HaveValue(Equal("Service type 'database' already served by 'db-provider'")))
			Expect(errBody.Instance).To(HaveValue(Equal("/api/v1alpha1/providers")))
		})

		It("sanitizes detail for INTERNAL errors (UT-XC-ERR-020)", func() {
			recorder := httptest.NewRecorder()
			logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

			httperror.WriteType(
				recorder, logger, v1alpha1.ErrorTypeINTERNAL,
				"nil pointer at server.go:42",
				nil,
			)

			Expect(recorder.Code).To(Equal(500))
			Expect(recorder.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var errBody v1alpha1.Error
			Expect(json.NewDecoder(recorder.Body).Decode(&errBody)).To(Succeed())
			Expect(errBody.Type).To(Equal(v1alpha1.ErrorTypeINTERNAL))
			Expect(errBody.Title).To(Equal(httperror.InternalTitle))
			Expect(errBody.Status).To(HaveValue(Equal(500)))
			Expect(errBody.Detail).To(HaveValue(Equal(httperror.InternalDetail)))
			Expect(*errBody.Detail).NotTo(ContainSubstring("nil pointer"))
			Expect(*errBody.Detail).NotTo(ContainSubstring("server.go"))
		})
	})

	Describe("Problem", func() {
		DescribeTable("maps each problem type to its canonical status and title",
			func(errType v1alpha1.ErrorType, wantStatus int, wantTitle string) {
				p := httperror.Problem(errType, "some detail")
				Expect(p.Type).To(Equal(errType))
				Expect(p.Status).To(Equal(wantStatus))
				Expect(p.Title).To(Equal(wantTitle))
			},
			Entry("invalid argument", v1alpha1.ErrorTypeINVALIDARGUMENT, 400, "Invalid argument"),
			Entry("unauthenticated", v1alpha1.ErrorTypeUNAUTHENTICATED, 401, "Unauthenticated"),
			Entry("permission denied", v1alpha1.ErrorTypePERMISSIONDENIED, 403, "Permission denied"),
			Entry("not found", v1alpha1.ErrorTypeNOTFOUND, 404, "Not found"),
			Entry("already exists", v1alpha1.ErrorTypeALREADYEXISTS, 409, "Already exists"),
			Entry("unprocessable entity", v1alpha1.ErrorTypeUNPROCESSABLEENTITY, 422, "Unprocessable entity"),
			Entry("internal", v1alpha1.ErrorTypeINTERNAL, 500, httperror.InternalTitle),
			Entry("unavailable", v1alpha1.ErrorTypeUNAVAILABLE, 503, "Service unavailable"),
		)

		DescribeTable("every problem type is a project-controlled URI, never about:blank",
			func(errType v1alpha1.ErrorType) {
				Expect(string(errType)).To(HavePrefix("https://dcm-project.github.io/problems/"))
				Expect(errType.Valid()).To(BeTrue())
			},
			Entry("invalid argument", v1alpha1.ErrorTypeINVALIDARGUMENT),
			Entry("unauthenticated", v1alpha1.ErrorTypeUNAUTHENTICATED),
			Entry("permission denied", v1alpha1.ErrorTypePERMISSIONDENIED),
			Entry("not found", v1alpha1.ErrorTypeNOTFOUND),
			Entry("already exists", v1alpha1.ErrorTypeALREADYEXISTS),
			Entry("unprocessable entity", v1alpha1.ErrorTypeUNPROCESSABLEENTITY),
			Entry("internal", v1alpha1.ErrorTypeINTERNAL),
			Entry("unavailable", v1alpha1.ErrorTypeUNAVAILABLE),
		)

		It("falls back to INTERNAL semantics for an unknown type", func() {
			p := httperror.Problem(v1alpha1.ErrorType("https://example.test/problems/mystery"), "detail")
			Expect(p.Status).To(Equal(500))
			Expect(p.Title).To(Equal(httperror.InternalTitle))
		})
	})
})
