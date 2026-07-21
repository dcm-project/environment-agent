// Package httperror provides RFC 7807 error response construction and mapping.
package httperror

import (
	"encoding/json"
	"log/slog"
	"net/http"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/ptr"
)

// WriteResponse writes an RFC 7807 error response to the given writer.
func WriteResponse(w http.ResponseWriter, logger *slog.Logger, statusCode int, errType string, title, detail string, instance *string) {
	if errType == "INTERNAL" {
		detail = InternalDetail
	}

	errBody := v1alpha1.Error{
		Type:     errType,
		Title:    title,
		Status:   ptr.To(statusCode),
		Detail:   ptr.To(detail),
		Instance: instance,
	}

	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(errBody); err != nil {
		logger.Error("failed to write error response", "error", err)
	}
}

// StatusForType returns the HTTP status code for the given error type string.
func StatusForType(errType string) int {
	switch errType {
	case "INVALID_ARGUMENT":
		return http.StatusBadRequest
	case "UNAUTHORIZED":
		return http.StatusUnauthorized
	case "NOT_FOUND":
		return http.StatusNotFound
	case "CONFLICT":
		return http.StatusConflict
	case "UNPROCESSABLE_ENTITY":
		return http.StatusUnprocessableEntity
	case "INTERNAL":
		return http.StatusInternalServerError
	case "UNAVAILABLE":
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
