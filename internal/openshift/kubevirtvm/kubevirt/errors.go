package kubevirt

import (
	"errors"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// classifyKubernetesError extracts status code and detail from a Kubernetes error.
// The fallbackDetail is used when the original error should not be exposed to clients.
func classifyKubernetesError(err error, fallbackDetail string) (int, string) {
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return http.StatusInternalServerError, err.Error()
	}

	switch statusErr.ErrStatus.Code {
	case http.StatusConflict:
		return http.StatusConflict, statusErr.ErrStatus.Message
	case http.StatusUnprocessableEntity:
		return http.StatusUnprocessableEntity, statusErr.ErrStatus.Message
	case http.StatusBadRequest:
		return http.StatusBadRequest, statusErr.ErrStatus.Message
	case http.StatusNotFound:
		return http.StatusNotFound, statusErr.ErrStatus.Message
	default:
		return http.StatusInternalServerError, fallbackDetail
	}
}

// IsAlreadyExistsError checks if the error indicates a resource already exists.
func IsAlreadyExistsError(err error) bool {
	return apierrors.IsAlreadyExists(err)
}

// IsNotFoundError checks if the error indicates a resource was not found.
func IsNotFoundError(err error) bool {
	return apierrors.IsNotFound(err)
}

// IsInvalidError checks if the error indicates invalid input.
func IsInvalidError(err error) bool {
	return apierrors.IsInvalid(err)
}

// HTTPError maps a Kubernetes API error to an HTTP status code and message.
func HTTPError(err error, fallback string) (int, string) {
	if err == nil {
		return http.StatusInternalServerError, fallback
	}
	code, detail := classifyKubernetesError(err, fallback)
	if detail == "" {
		return code, fallback
	}
	return code, detail
}
