package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/auth"
	"github.com/dcm-project/environment-agent/internal/httperror"
	"github.com/dcm-project/environment-agent/internal/requestctx"
)

// detailRequestTimeout is the detail written when a request exceeds the
// server's per-request deadline.
const detailRequestTimeout = "request timeout exceeded"

// PanicRecovery returns middleware that catches panics and returns RFC 9457 INTERNAL problems.
func PanicRecovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if p := recover(); p != nil {
					if p == http.ErrAbortHandler {
						panic(p)
					}
					logger.Error("panic recovered",
						"panic", fmt.Sprint(p),
						"stack", string(debug.Stack()),
					)
					uri := r.RequestURI
					httperror.WriteType(w, logger, v1alpha1.ErrorTypeINTERNAL, fmt.Sprint(p), &uri)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestLogger returns middleware that records the final response status —
// and, on success, the authenticated identity — into the shared
// requestctx.Outcome, rather than emitting the audit log itself. It runs
// inside RequestTimeout, which may still override the status with a 503, so
// RequestTimeout is the single place that calls auth.LogRequest (DD-510).
//
// Uses defer so the outcome is still recorded on panic (status forced to
// 500); RequestTimeout's own recover then emits the audit log.
func RequestLogger() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			srw := &statusRecordingResponseWriter{ResponseWriter: w, code: http.StatusOK}
			panicked := true
			defer func() {
				code := srw.code
				if panicked {
					code = http.StatusInternalServerError
				}
				if outcome := requestctx.OutcomeFromContext(r.Context()); outcome != nil {
					outcome.Status = code
					// auth.Middleware places claims in r.Context() via WithClaims
					// before next.ServeHTTP reaches this handler.
					if claims, ok := auth.ClaimsFromContext(r.Context()); ok {
						outcome.Claims = claims
					}
				}
			}()
			next.ServeHTTP(srw, r)
			panicked = false
		})
	}
}

// RequestTimeout returns middleware that applies a per-request context
// deadline and buffers the response so a late handler cannot write directly
// to the real ResponseWriter. It is also the single point that emits the
// request's audit log entry (DD-510), once the downstream chain has
// returned: it logs its own 503 on timeout, otherwise the status/claims
// recorded into the shared requestctx.Outcome by RequestLogger or auth's
// writeAuthError.
//
// The deadline is cooperative: handlers that ignore ctx.Done() will not be
// preempted (see DD-170). After the handler returns, if the deadline has been
// exceeded the client receives a 503 instead of the handler's response.
//
// This middleware always wraps the request (buffering + logging), even when
// timeout <= 0 disables deadline enforcement itself.
func RequestTimeout(timeout time.Duration, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			var cancel context.CancelFunc
			if timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			start, ok := requestctx.StartTimeFromContext(r.Context())
			if !ok {
				start = time.Now()
			}
			outcome := requestctx.OutcomeFromContext(r.Context())

			// logOutcome emits the single audit log entry for this request,
			// re-deriving a context carrying any claims recorded into outcome
			// (the claims-bearing context from downstream never propagates back
			// up to this scope; only the shared *Outcome pointer does).
			logOutcome := func(status int) {
				logCtx := r.Context()
				if outcome != nil {
					if claims, ok := outcome.Claims.(*auth.JWTClaims); ok && claims != nil {
						logCtx = auth.WithClaims(logCtx, claims)
					}
				}
				auth.LogRequest(logCtx, logger, r, status, start)
			}

			// finalStatus resolves the status recorded by RequestLogger/
			// writeAuthError, falling back to the buffered writer's own code
			// if outcome is unset (defensive only; not expected in production).
			finalStatus := func(buf *bufferedResponseWriter) int {
				if outcome != nil && outcome.Status != 0 {
					return outcome.Status
				}
				return buf.code
			}

			buf := &bufferedResponseWriter{header: make(http.Header), code: 0}

			defer func() {
				if p := recover(); p != nil {
					status := http.StatusInternalServerError
					if outcome != nil && outcome.Status != 0 {
						status = outcome.Status
					}
					logOutcome(status)
					panic(p)
				}
			}()

			next.ServeHTTP(buf, r.WithContext(ctx))

			if ctx.Err() == context.DeadlineExceeded {
				uri := r.RequestURI
				httperror.WriteType(w, logger, v1alpha1.ErrorTypeUNAVAILABLE,
					detailRequestTimeout, &uri)
				logOutcome(http.StatusServiceUnavailable)
				return
			}
			buf.flushTo(w)
			logOutcome(finalStatus(buf))
		})
	}
}

type statusRecordingResponseWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (w *statusRecordingResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code = code
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *statusRecordingResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.code = http.StatusOK
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusRecordingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

type bufferedResponseWriter struct {
	header      http.Header
	body        bytes.Buffer
	code        int
	wroteHeader bool
}

func (w *bufferedResponseWriter) Header() http.Header {
	return w.header
}

func (w *bufferedResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.code = code
		w.wroteHeader = true
	}
}

func (w *bufferedResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.code = http.StatusOK
		w.wroteHeader = true
	}
	return w.body.Write(b)
}

func (w *bufferedResponseWriter) flushTo(dst http.ResponseWriter) {
	for k, vv := range w.header {
		for _, v := range vv {
			dst.Header().Add(k, v)
		}
	}
	if w.code > 0 {
		dst.WriteHeader(w.code)
	}
	_, _ = dst.Write(w.body.Bytes())
}
