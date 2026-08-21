package runtime

import (
	"errors"
	"net/http"

	acmv1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
)

// OperationError describes a failed cluster operation using HTTP status semantics.
type OperationError struct {
	StatusCode int
	Message    string
}

// MapOperationError maps a cluster service error to HTTP status semantics.
// Returns nil when err is nil.
func MapOperationError(err error) *OperationError {
	if err == nil {
		return nil
	}

	var domainErr *service.DomainError
	if !errors.As(err, &domainErr) {
		return &OperationError{
			StatusCode: http.StatusInternalServerError,
			Message:    err.Error(),
		}
	}

	msg := domainErr.Message
	if domainErr.Detail != "" {
		msg = domainErr.Detail
	}
	if domainErr.Type == acmv1.ErrorTypeINTERNAL {
		msg = "an internal error occurred"
	}

	return &OperationError{
		StatusCode: httpStatusForErrorType(domainErr.Type),
		Message:    msg,
	}
}

func httpStatusForErrorType(t acmv1.ErrorType) int {
	switch t {
	case acmv1.ErrorTypeINVALIDARGUMENT:
		return http.StatusBadRequest
	case acmv1.ErrorTypeNOTFOUND:
		return http.StatusNotFound
	case acmv1.ErrorTypeALREADYEXISTS:
		return http.StatusConflict
	case acmv1.ErrorTypeUNPROCESSABLEENTITY:
		return http.StatusUnprocessableEntity
	case acmv1.ErrorTypeUNAVAILABLE:
		return http.StatusServiceUnavailable
	case acmv1.ErrorTypePERMISSIONDENIED:
		return http.StatusForbidden
	case acmv1.ErrorTypeUNAUTHENTICATED:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}
