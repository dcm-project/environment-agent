package httperror

import (
	"log/slog"
	"net/http"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
)

// WriteInvalidArgument writes a 400 RFC 9457 problem for request validation failures.
func WriteInvalidArgument(w http.ResponseWriter, r *http.Request, logger *slog.Logger, detail string) {
	uri := r.RequestURI
	WriteType(w, logger, v1alpha1.ErrorTypeINVALIDARGUMENT, detail, &uri)
}
