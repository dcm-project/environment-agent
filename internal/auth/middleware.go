package auth

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/dcm-project/environment-agent/internal/httperror"
	"github.com/dcm-project/environment-agent/internal/requestctx"
)

const healthPath = "/api/v1alpha1/health"

// invalidTokenDetail is the fixed, generic detail returned to clients for
// any Bearer token failure; internal validator errors are never echoed back.
const invalidTokenDetail = "invalid Bearer token"

// MiddlewareConfig holds dependencies for the auth middleware.
type MiddlewareConfig struct {
	JWTValidator JWTValidator
	Logger       *slog.Logger
}

// Middleware returns HTTP middleware that validates JWT Bearer tokens.
// GET requests to the health endpoint are bypassed (REQ-AUTH-020); the
// bypass is scoped to GET since that's the only operation the OpenAPI spec
// declares as public — any other method at that path must still authenticate.
func Middleware(cfg MiddlewareConfig) func(http.Handler) http.Handler {
	if cfg.JWTValidator == nil {
		panic("auth: JWTValidator must not be nil")
	}
	if cfg.Logger == nil {
		panic("auth: Logger must not be nil")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == healthPath {
				next.ServeHTTP(w, r)
				return
			}

			start := requestStartTime(r)

			token, err := ExtractBearerToken(r)
			if err != nil {
				logAuthFailure(cfg.Logger, r, "bearer token extraction failed", err)
				writeAuthError(w, r, cfg.Logger, invalidTokenDetail, start)
				return
			}

			claims, err := cfg.JWTValidator.Validate(r.Context(), token)
			if err != nil {
				logAuthFailure(cfg.Logger, r, "jwt validation failed", err)
				writeAuthError(w, r, cfg.Logger, invalidTokenDetail, start)
				return
			}

			cfg.Logger.Debug("authenticated request",
				"sub", claims.Subject,
				"preferred_username", claims.PreferredUsername,
				"method", r.Method,
				"path", r.URL.Path,
			)

			ctx := WithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// DisabledMiddleware returns a passthrough middleware that performs no
// authentication. A WARN log is emitted at construction time (REQ-AUTH-090).
func DisabledMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		panic("auth: logger must not be nil")
	}
	logger.Warn("authentication is disabled")
	return func(next http.Handler) http.Handler {
		return next
	}
}

// logAuthFailure logs the underlying Bearer token failure server-side; the
// error itself must never reach the client.
func logAuthFailure(logger *slog.Logger, r *http.Request, msg string, err error) {
	logger.Warn(msg,
		"error", err,
		"method", r.Method,
		"path", r.URL.Path,
	)
}

// writeAuthError writes a 401 RFC 7807 response with a WWW-Authenticate: Bearer
// header (DD-AUTH-02). detail must be a fixed, generic message, never a raw
// validator error. The body is delegated to httperror.WriteResponse.
//
// It also emits the request audit log directly, since rejections short-circuit
// before RequestLogger runs (REQ-AUTH-120).
func writeAuthError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, detail string, start time.Time) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	instance := requestctx.URIFromContext(r.Context())
	httperror.WriteResponse(w, logger, http.StatusUnauthorized,
		"UNAUTHORIZED", "Unauthorized", detail, instance)
	LogRequest(r.Context(), logger, r, http.StatusUnauthorized, start)
}

// requestStartTime returns the value recorded by requestctx.Middleware, or
// now if absent (e.g. this middleware is exercised directly in unit tests).
func requestStartTime(r *http.Request) time.Time {
	if start, ok := requestctx.StartTimeFromContext(r.Context()); ok {
		return start
	}
	return time.Now()
}

// LogRequest emits the standard per-request audit record: method, path,
// status, duration, and identity (`sub`, `preferred_username`) if present in
// ctx. Shared by writeAuthError and apiserver.RequestLogger (REQ-HTTP-060,
// REQ-AUTH-070, REQ-AUTH-120).
func LogRequest(ctx context.Context, logger *slog.Logger, r *http.Request, status int, start time.Time) {
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.String("duration", time.Since(start).String()),
	}
	if claims, ok := ClaimsFromContext(ctx); ok {
		attrs = append(attrs, slog.String("sub", claims.Subject))
		if claims.PreferredUsername != "" {
			attrs = append(attrs, slog.String("preferred_username", claims.PreferredUsername))
		}
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "request", attrs...)
}
