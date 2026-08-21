package service

import (
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
)

// DomainError represents a typed domain error from the service layer.
type DomainError struct {
	Type    v1alpha1.ErrorType
	Message string
	Detail  string
	Cause   error
}

func (e *DomainError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Type, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func (e *DomainError) Unwrap() error {
	return e.Cause
}

// WithDetail adds an optional detail field and returns the error for chaining.
func (e *DomainError) WithDetail(detail string) *DomainError {
	e.Detail = detail
	return e
}

// NewNotFoundError creates a NOT_FOUND domain error.
func NewNotFoundError(msg string) *DomainError {
	return &DomainError{Type: v1alpha1.ErrorTypeNOTFOUND, Message: msg}
}

// NewAlreadyExistsError creates an ALREADY_EXISTS domain error.
func NewAlreadyExistsError(msg string) *DomainError {
	return &DomainError{Type: v1alpha1.ErrorTypeALREADYEXISTS, Message: msg}
}

// NewInvalidArgumentError creates an INVALID_ARGUMENT domain error.
func NewInvalidArgumentError(msg string) *DomainError {
	return &DomainError{Type: v1alpha1.ErrorTypeINVALIDARGUMENT, Message: msg}
}

// NewUnprocessableEntityError creates an UNPROCESSABLE_ENTITY domain error.
func NewUnprocessableEntityError(msg string) *DomainError {
	return &DomainError{Type: v1alpha1.ErrorTypeUNPROCESSABLEENTITY, Message: msg}
}

// NewInternalError creates an INTERNAL domain error with a wrapped cause.
func NewInternalError(msg string, cause error) *DomainError {
	return &DomainError{Type: v1alpha1.ErrorTypeINTERNAL, Message: msg, Cause: cause}
}

// NewUnavailableError creates an UNAVAILABLE domain error.
func NewUnavailableError(msg string) *DomainError {
	return &DomainError{Type: v1alpha1.ErrorTypeUNAVAILABLE, Message: msg}
}
