package store

import "fmt"

// NotFoundError indicates a requested resource was not found.
type NotFoundError struct {
	Resource string
	ID       string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %q not found", e.Resource, e.ID)
}

// ConflictError indicates a resource already exists or conflicts with existing state.
type ConflictError struct {
	Resource string
	Field    string
	Value    string
	Message  string
}

func (e *ConflictError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("%s with %s=%q already exists", e.Resource, e.Field, e.Value)
}

// InvalidArgumentError indicates invalid input parameters.
type InvalidArgumentError struct {
	Field   string
	Message string
}

func (e *InvalidArgumentError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
}
