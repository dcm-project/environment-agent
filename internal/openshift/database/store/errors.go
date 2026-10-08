package store

import (
	"errors"
	"fmt"
)

// NotFoundError indicates the requested resource was not found.
type NotFoundError struct {
	ID string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("database %q not found", e.ID)
}

func IsNotFound(e error) bool {
	var notFound *NotFoundError
	return errors.As(e, &notFound)
}

// ConflictError indicates a resource conflict (e.g., duplicate instance ID).
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string {
	return e.Message
}

func IsConflict(e error) bool {
	var conflict *ConflictError
	return errors.As(e, &conflict)
}

// InvalidArgumentError indicates a validation failure in the request.
type InvalidArgumentError struct {
	Message string
}

func (e *InvalidArgumentError) Error() string {
	return e.Message
}

func IsInvalidArgument(e error) bool {
	var invalidArgument *InvalidArgumentError
	return errors.As(e, &invalidArgument)
}

// FailedPreconditionError indicates a request that cannot be fulfilled due to
// policy or cluster state (HTTP 422).
type FailedPreconditionError struct {
	Message string
}

func (e *FailedPreconditionError) Error() string {
	return e.Message
}
