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
)

// Middleware stores the request URI and start time in the context for
// downstream handlers. Recorded here, ahead of auth and request-timeout, so
// audit logs measure duration from the same origin regardless of where a
// request is finalized.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), ctxKey{}, r.RequestURI)
		ctx = context.WithValue(ctx, startTimeKey{}, time.Now())
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
