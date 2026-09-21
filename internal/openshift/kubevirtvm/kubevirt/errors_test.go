package kubevirt_test

import (
	"fmt"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/kubevirt"
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

	Describe("HTTPError", func() {
		It("should return fallback for a nil error", func() {
			code, msg := kubevirt.HTTPError(nil, "fallback")
			Expect(code).To(Equal(http.StatusInternalServerError))
			Expect(msg).To(Equal("fallback"))
		})

		It("should map a conflict error to 409", func() {
			err := k8sStatusError(http.StatusConflict, metav1.StatusReasonConflict, "conflict")
			code, msg := kubevirt.HTTPError(err, "fallback")
			Expect(code).To(Equal(http.StatusConflict))
			Expect(msg).To(Equal("conflict"))
		})

		It("should map a forbidden error to 500 with fallback detail", func() {
			err := k8sStatusError(http.StatusForbidden, metav1.StatusReasonForbidden, "forbidden")
			code, msg := kubevirt.HTTPError(err, "Failed to create virtual machine")
			Expect(code).To(Equal(http.StatusInternalServerError))
			Expect(msg).To(Equal("Failed to create virtual machine"))
		})

		It("should map a non-k8s error to 500 with the error message", func() {
			code, msg := kubevirt.HTTPError(fmt.Errorf("connection refused"), "fallback")
			Expect(code).To(Equal(http.StatusInternalServerError))
			Expect(msg).To(Equal("connection refused"))
		})
	})
})
