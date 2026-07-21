package apiserver

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/dcm-project/environment-agent/internal/httperror"
)

// PanicRecovery returns middleware that catches panics and returns RFC 7807 INTERNAL errors.
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
					httperror.WriteResponse(w, logger, http.StatusInternalServerError,
						"INTERNAL", httperror.InternalTitle, fmt.Sprint(p), &uri)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestLogger returns middleware that logs each HTTP request at INFO level.
// Uses defer so the log line is emitted even when a panic propagates through.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			srw := &statusRecordingResponseWriter{ResponseWriter: w, code: http.StatusOK}
			panicked := true
			defer func() {
				if panicked {
					srw.code = http.StatusInternalServerError
				}
				logger.Info("request",
					"method", r.Method,
					"path", r.URL.Path,
					"status", srw.code,
					"duration", time.Since(start).String(),
				)
			}()
			next.ServeHTTP(srw, r)
			panicked = false
		})
	}
}

// RequestTimeout returns middleware that enforces a per-request timeout.
func RequestTimeout(timeout time.Duration, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if timeout <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			buf := &bufferedResponseWriter{header: make(http.Header), code: 0}
			next.ServeHTTP(buf, r.WithContext(ctx))

			if ctx.Err() == context.DeadlineExceeded {
				uri := r.RequestURI
				httperror.WriteResponse(w, logger, http.StatusServiceUnavailable,
					"UNAVAILABLE", "Service Unavailable", "request timeout exceeded", &uri)
				return
			}
			buf.flushTo(w)
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
