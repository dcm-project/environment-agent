package kubevirt_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/kubevirt"
	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/oapi/server"
)

func k8sStatusError(code int32, reason metav1.StatusReason, message string) *apierrors.StatusError {
	return &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    code,
			Reason:  reason,
			Message: message,
		},
	}
}

var _ = Describe("Errors", func() {
	Describe("InternalServerError", func() {
		It("should return 500 status and correct detail", func() {
			body, statusCode := kubevirt.InternalServerError("something went wrong")

			Expect(statusCode).To(Equal(http.StatusInternalServerError))
			Expect(body.Title).To(Equal("Internal Server Error"))
			Expect(*body.Detail).To(Equal("something went wrong"))
			Expect(*body.Status).To(Equal(http.StatusInternalServerError))
			Expect(body.Type).To(Equal("about:blank"))
		})
	})

	Describe("ValidationError", func() {
		It("should return 400 status and correct detail", func() {
			body, statusCode := kubevirt.ValidationError("invalid field")

			Expect(statusCode).To(Equal(http.StatusBadRequest))
			Expect(body.Title).To(Equal("Validation Error"))
			Expect(*body.Detail).To(Equal("invalid field"))
			Expect(*body.Status).To(Equal(http.StatusBadRequest))
		})
	})

	Describe("WriteProblem", func() {
		It("should write application/problem+json with type and title", func() {
			rec := httptest.NewRecorder()
			kubevirt.WriteProblem(rec, http.StatusBadRequest, "Validation Error", "value is required but missing")

			Expect(rec.Code).To(Equal(http.StatusBadRequest))
			Expect(rec.Header().Get("Content-Type")).To(Equal("application/problem+json"))

			var body server.Error
			Expect(json.Unmarshal(rec.Body.Bytes(), &body)).To(Succeed())
			Expect(body.Type).To(Equal("about:blank"))
			Expect(body.Title).To(Equal("Validation Error"))
			Expect(*body.Detail).To(Equal("value is required but missing"))
			Expect(*body.Status).To(Equal(http.StatusBadRequest))
		})
	})

	Describe("IsNotFoundError", func() {
		It("should return true for a not-found error", func() {
			err := apierrors.NewNotFound(schema.GroupResource{Resource: "vms"}, "test")
			Expect(kubevirt.IsNotFoundError(err)).To(BeTrue())
		})

		It("should return false for a non-not-found error", func() {
			err := fmt.Errorf("some other error")
			Expect(kubevirt.IsNotFoundError(err)).To(BeFalse())
		})
	})

	Describe("IsAlreadyExistsError", func() {
		It("should return true for an already-exists error", func() {
			err := apierrors.NewAlreadyExists(schema.GroupResource{Resource: "vms"}, "test")
			Expect(kubevirt.IsAlreadyExistsError(err)).To(BeTrue())
		})

		It("should return false for other errors", func() {
			err := fmt.Errorf("some other error")
			Expect(kubevirt.IsAlreadyExistsError(err)).To(BeFalse())
		})
	})

	Describe("IsInvalidError", func() {
		It("should return true for an invalid error", func() {
			err := apierrors.NewInvalid(schema.GroupKind{Kind: "VirtualMachine"}, "test", nil)
			Expect(kubevirt.IsInvalidError(err)).To(BeTrue())
		})

		It("should return false for other errors", func() {
			err := fmt.Errorf("some other error")
			Expect(kubevirt.IsInvalidError(err)).To(BeFalse())
		})
	})

	Describe("MapKubernetesError", func() {
		It("should return nil for nil error", func() {
			resp := kubevirt.MapKubernetesError(nil)
			Expect(resp).To(BeNil())
		})

		It("should map a non-k8s error to 500", func() {
			resp := kubevirt.MapKubernetesError(fmt.Errorf("connection refused"))

			errResp, ok := resp.(*server.CreateVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusInternalServerError))
		})

		It("should map a conflict error to 409", func() {
			err := k8sStatusError(http.StatusConflict, metav1.StatusReasonConflict, "conflict")
			resp := kubevirt.MapKubernetesError(err)

			errResp, ok := resp.(*server.CreateVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusConflict))
		})

		It("should map an unprocessable entity error to 422", func() {
			err := k8sStatusError(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid, "invalid")
			resp := kubevirt.MapKubernetesError(err)

			errResp, ok := resp.(*server.CreateVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusUnprocessableEntity))
		})

		It("should map a bad request error to 400", func() {
			err := k8sStatusError(http.StatusBadRequest, metav1.StatusReasonBadRequest, "bad request")
			resp := kubevirt.MapKubernetesError(err)

			errResp, ok := resp.(*server.CreateVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusBadRequest))
		})

		It("should map a not found error to 404", func() {
			err := k8sStatusError(http.StatusNotFound, metav1.StatusReasonNotFound, "not found")
			resp := kubevirt.MapKubernetesError(err)

			errResp, ok := resp.(*server.CreateVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusNotFound))
		})

		It("should map a forbidden error to 500 with fallback detail", func() {
			err := k8sStatusError(http.StatusForbidden, metav1.StatusReasonForbidden, "forbidden")
			resp := kubevirt.MapKubernetesError(err)

			errResp, ok := resp.(*server.CreateVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusInternalServerError))
			Expect(*errResp.Body.Detail).To(Equal("Failed to create virtual machine"))
		})
	})

	Describe("MapKubernetesErrorForDelete", func() {
		It("should return nil for nil error", func() {
			resp := kubevirt.MapKubernetesErrorForDelete(nil)
			Expect(resp).To(BeNil())
		})

		It("should map a 404 error to typed 404 response", func() {
			err := k8sStatusError(http.StatusNotFound, metav1.StatusReasonNotFound, "not found")
			resp := kubevirt.MapKubernetesErrorForDelete(err)

			_, ok := resp.(server.DeleteVM404ApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
		})

		It("should map a non-404 error to default response", func() {
			err := fmt.Errorf("connection refused")
			resp := kubevirt.MapKubernetesErrorForDelete(err)

			errResp, ok := resp.(server.DeleteVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusInternalServerError))
		})
	})

	Describe("MapKubernetesErrorForGet", func() {
		It("should return nil for nil error", func() {
			resp := kubevirt.MapKubernetesErrorForGet(nil)
			Expect(resp).To(BeNil())
		})

		It("should map a 404 error to typed 404 response", func() {
			err := k8sStatusError(http.StatusNotFound, metav1.StatusReasonNotFound, "not found")
			resp := kubevirt.MapKubernetesErrorForGet(err)

			_, ok := resp.(server.GetVM404ApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
		})

		It("should map a non-404 error to default response", func() {
			err := fmt.Errorf("connection refused")
			resp := kubevirt.MapKubernetesErrorForGet(err)

			errResp, ok := resp.(server.GetVMdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusInternalServerError))
		})
	})

	Describe("MapKubernetesErrorForList", func() {
		It("should return nil for nil error", func() {
			resp := kubevirt.MapKubernetesErrorForList(nil)
			Expect(resp).To(BeNil())
		})

		It("should map an error to typed response", func() {
			err := fmt.Errorf("connection refused")
			resp := kubevirt.MapKubernetesErrorForList(err)

			errResp, ok := resp.(*server.ListVMsdefaultApplicationProblemPlusJSONResponse)
			Expect(ok).To(BeTrue())
			Expect(errResp.StatusCode).To(Equal(http.StatusInternalServerError))
		})
	})
})
