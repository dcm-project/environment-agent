package store

import "fmt"

// NotFoundError indicates the requested volume was not found.
type NotFoundError struct {
	ID string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("volume %q not found", e.ID)
}

// ConflictError indicates a resource conflict (e.g., duplicate name or ID).
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string {
	return e.Message
}

// InvalidArgumentError indicates a validation failure in the request.
type InvalidArgumentError struct {
	Message string
}

func (e *InvalidArgumentError) Error() string {
	return e.Message
}

// FailedPreconditionError indicates a request that cannot be fulfilled due to
// policy or cluster state (HTTP 422).
type FailedPreconditionError struct {
	Message string
}

func (e *FailedPreconditionError) Error() string {
	return e.Message
}
