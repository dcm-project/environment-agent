// Package requestctx provides request-scoped context helpers.
package requestctx

import (
	"context"
	"net/http"
)

type ctxKey struct{}

// Middleware stores the request URI in the context for downstream handlers.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), ctxKey{}, r.RequestURI)
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
