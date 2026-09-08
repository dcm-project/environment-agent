// Package requestctx provides request-scoped context helpers.
package requestctx

import (
	"context"
	"net/http"
	"time"
)

type (
	ctxKey       struct{}
	startTimeKey struct{}
	outcomeKey   struct{}
)

// Outcome is a per-request mutable record of the final audit-log fields
// (status code and authenticated identity, if any). Middleware creates
// exactly one *Outcome per request and stores the pointer in the context.
//
// It is shared via pointer, not value, so that downstream middleware which
// derive child contexts (via r.WithContext) still mutate the same struct a
// caller further up the chain can observe after next.ServeHTTP returns —
// context values themselves don't propagate back up. This lets exactly one
// caller (apiserver.RequestTimeout) own the single point of audit-log
// emission while reflecting what inner middleware recorded (status from
// apiserver.RequestLogger, rejection status from auth's writeAuthError).
//
// No synchronization is needed: a request's handler chain runs on one
// goroutine, and writes always happen-before the single read. Do not share
// an *Outcome across requests or hand it to a goroutine outliving the
// request.
type Outcome struct {
	Status int
	Claims any
}

// Middleware stores the request URI, start time, and a fresh *Outcome in
// the context for downstream handlers. Recorded here, ahead of auth and
// request-timeout, so audit logs measure duration from the same origin
// regardless of where a request is finalized (see Outcome).
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), ctxKey{}, r.RequestURI)
		ctx = context.WithValue(ctx, startTimeKey{}, time.Now())
		ctx = context.WithValue(ctx, outcomeKey{}, &Outcome{})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// URIFromContext retrieves the request URI stored by Middleware.
// Returns nil if the middleware was not applied.
func URIFromContext(ctx context.Context) *string {
	if uri, ok := ctx.Value(ctxKey{}).(string); ok {
		return &uri
	}
	return nil
}

// StartTimeFromContext retrieves the request start time stored by Middleware.
// Returns the zero time and false if the middleware was not applied.
func StartTimeFromContext(ctx context.Context) (time.Time, bool) {
	start, ok := ctx.Value(startTimeKey{}).(time.Time)
	return start, ok
}

// OutcomeFromContext retrieves the shared *Outcome stored by Middleware.
// Returns nil if the middleware was not applied to this request.
func OutcomeFromContext(ctx context.Context) *Outcome {
	outcome, _ := ctx.Value(outcomeKey{}).(*Outcome)
	return outcome
}
