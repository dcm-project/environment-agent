// Package httperror provides RFC 9457 problem details construction and mapping.
package httperror

import (
	"encoding/json"
	"log/slog"
	"net/http"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/ptr"
)

// ProblemFields holds the RFC 9457 problem detail fields for an error
// response, before the per-occurrence instance URI is attached.
type ProblemFields struct {
	Type   v1alpha1.ErrorType
	Status int
	Title  string
	Detail string
}

// problemMapping is the canonical HTTP status and title for a problem type.
// A given type always carries the same title, as RFC 9457 section 3.1.1
// requires.
type problemMapping struct {
	Status int
	Title  string
}

// mapErrorType returns the canonical status and title for a problem type.
// Titles follow the humanized-slug convention shared with the ACM Cluster SP
// and the K8s Container SP. Unknown types map to INTERNAL semantics.
func mapErrorType(t v1alpha1.ErrorType) problemMapping {
	switch t {
	case v1alpha1.ErrorTypeINVALIDARGUMENT:
		return problemMapping{http.StatusBadRequest, "Invalid argument"}
	case v1alpha1.ErrorTypeUNAUTHENTICATED:
		return problemMapping{http.StatusUnauthorized, "Unauthenticated"}
	case v1alpha1.ErrorTypePERMISSIONDENIED:
		return problemMapping{http.StatusForbidden, "Permission denied"}
	case v1alpha1.ErrorTypeNOTFOUND:
		return problemMapping{http.StatusNotFound, "Not found"}
	case v1alpha1.ErrorTypeALREADYEXISTS:
		return problemMapping{http.StatusConflict, "Already exists"}
	case v1alpha1.ErrorTypeUNPROCESSABLEENTITY:
		return problemMapping{http.StatusUnprocessableEntity, "Unprocessable entity"}
	case v1alpha1.ErrorTypeINTERNAL:
		return problemMapping{http.StatusInternalServerError, InternalTitle}
	case v1alpha1.ErrorTypeUNAVAILABLE:
		return problemMapping{http.StatusServiceUnavailable, "Service unavailable"}
	default:
		return problemMapping{http.StatusInternalServerError, InternalTitle}
	}
}

// Problem builds the canonical problem fields for an error type. The detail of
// an INTERNAL problem is replaced with a generic message so internal failure
// text never reaches the client.
func Problem(errType v1alpha1.ErrorType, detail string) ProblemFields {
	m := mapErrorType(errType)
	if errType == v1alpha1.ErrorTypeINTERNAL {
		detail = InternalDetail
	}
	return ProblemFields{
		Type:   errType,
		Status: m.Status,
		Title:  m.Title,
		Detail: detail,
	}
}

// Body converts problem fields into the wire-level Error schema, attaching the
// per-occurrence instance URI.
func (p ProblemFields) Body(instance *string) v1alpha1.Error {
	return v1alpha1.Error{
		Type:     p.Type,
		Title:    p.Title,
		Status:   ptr.To(p.Status),
		Detail:   ptr.To(p.Detail),
		Instance: instance,
	}
}

// marshalFailBody is the static problem document written when marshalling the
// real one fails, so a client always receives a valid problem+json body.
const marshalFailBody = `{"type":"` + string(v1alpha1.ErrorTypeINTERNAL) +
	`","title":"` + InternalTitle +
	`","status":500,"detail":"` + InternalDetail + `"}`

// Write writes problem fields as an RFC 9457 application/problem+json
// response. The body is marshalled before the status is written, so a
// marshalling failure cannot leave a half-written body under a success status.
func Write(w http.ResponseWriter, logger *slog.Logger, p ProblemFields, instance *string) {
	status := p.Status
	body, err := json.Marshal(p.Body(instance))
	if err != nil {
		logger.Error("failed to marshal error response", "error", err)
		status = http.StatusInternalServerError
		body = []byte(marshalFailBody)
	}

	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)

	if _, err := w.Write(append(body, '\n')); err != nil {
		logger.Error("failed to write error response", "error", err)
	}
}

// WriteType is shorthand for Write with the canonical fields of errType.
func WriteType(w http.ResponseWriter, logger *slog.Logger, errType v1alpha1.ErrorType, detail string, instance *string) {
	Write(w, logger, Problem(errType, detail), instance)
}
