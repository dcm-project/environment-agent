package service

// ErrorCode is a typed string for domain error classification.
type ErrorCode string

const (
	ErrCodeConflict ErrorCode = "CONFLICT"
	ErrCodeNotFound ErrorCode = "NOT_FOUND"
)

// DomainError is the single error type returned by the service layer.
type DomainError struct {
	Code    ErrorCode
	Message string
}

func (e *DomainError) Error() string { return e.Message }
