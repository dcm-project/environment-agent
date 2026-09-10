package store

import "fmt"

// NotFoundError indicates the requested resource was not found.
type NotFoundError struct {
	ID string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("database %q not found", e.ID)
}

// ConflictError indicates a resource conflict (e.g., duplicate instance ID).
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
