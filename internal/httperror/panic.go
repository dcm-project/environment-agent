package httperror

import (
	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/ptr"
)

// PanicToErrorBody converts a recovered panic value to an RFC 7807 Error body.
func PanicToErrorBody(_ interface{}) v1alpha1.Error {
	return v1alpha1.Error{
		Type:   "INTERNAL",
		Title:  InternalTitle,
		Status: ptr.To(500),
		Detail: ptr.To(InternalDetail),
	}
}
