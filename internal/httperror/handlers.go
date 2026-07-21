package httperror

import (
	"log/slog"
	"net/http"
)

// WriteInvalidArgument writes a 400 RFC 7807 error for request validation failures.
func WriteInvalidArgument(w http.ResponseWriter, r *http.Request, logger *slog.Logger, detail string) {
	uri := r.RequestURI
	WriteResponse(w, logger, http.StatusBadRequest, "INVALID_ARGUMENT", "Bad Request", detail, &uri)
}
