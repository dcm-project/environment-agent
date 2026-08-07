// Package apiserver provides HTTP server lifecycle management for the environment agent.
package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-chi/chi/v5"
	nethttpmiddleware "github.com/oapi-codegen/nethttp-middleware"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/httperror"
	"github.com/dcm-project/environment-agent/internal/requestctx"
)

// Server manages the HTTP server lifecycle.
type Server struct {
	cfg      *config.Config
	logger   *slog.Logger
	listener net.Listener
	handler  server.ServerInterface
	srv      *http.Server
}

// New creates a new Server with the given dependencies.
// The listener is passed to Run(), not to the constructor.
func New(cfg *config.Config, logger *slog.Logger, handler server.ServerInterface) *Server {
	return &Server{
		cfg:     cfg,
		logger:  logger,
		handler: handler,
	}
}

// Run starts the HTTP server and blocks until the context is cancelled or an error occurs.
// The listener is accepted at runtime to keep the constructor pure.
func (s *Server) Run(ctx context.Context, ln net.Listener) error {
	s.listener = ln
	r := chi.NewRouter()

	r.Use(PanicRecovery(s.logger))
	r.Use(requestctx.Middleware)
	r.Use(RequestLogger(s.logger))
	r.Use(RequestTimeout(s.cfg.Server.RequestTimeout, s.logger))

	spec, err := v1alpha1.GetSpec()
	if err != nil {
		return err
	}
	stripHandlerValidatedConstraints(spec)

	r.Use(nethttpmiddleware.OapiRequestValidatorWithOptions(spec, &nethttpmiddleware.Options{
		Options: openapi3filter.Options{
			RegexCompiler: noopRegexCompiler,
		},
		SilenceServersWarning: true,
		ErrorHandlerWithOpts: func(_ context.Context, valErr error, w http.ResponseWriter, req *http.Request, _ nethttpmiddleware.ErrorHandlerOpts) {
			httperror.WriteInvalidArgument(w, req, s.logger, valErr.Error())
		},
	}))

	server.HandlerWithOptions(s.handler, server.ChiServerOptions{
		BaseRouter: r,
		BaseURL:    "/api/v1alpha1",
		ErrorHandlerFunc: func(w http.ResponseWriter, req *http.Request, err error) {
			httperror.WriteInvalidArgument(w, req, s.logger, err.Error())
		},
	})

	s.srv = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Log before starting to Serve, not after: the listener can already be
	// accepting connections the instant the goroutine below runs, so logging
	// afterward races an external observer (e.g. a readiness probe, or a
	// deployment tool tailing logs for this exact message) against the
	// scheduler actually getting around to running this log statement
	// (IT-HTTP-090).
	s.logger.Info("server listening", "address", s.listener.Addr().String())

	serveCh := make(chan error, 1)
	go func() {
		if err := s.srv.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveCh <- err
		} else {
			serveCh <- nil
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-serveCh:
		return err
	}

	s.logger.Info("server shutdown initiated")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), s.cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Warn("shutdown timeout exceeded, forcing close")
			_ = s.srv.Close()
		} else {
			s.logger.Error("shutdown error", "error", err)
		}
	}

	if err := <-serveCh; err != nil {
		return err
	}

	s.logger.Info("server shutdown complete")
	return nil
}

// Addr returns the address the server is listening on.
// Returns an empty string if the server has not started yet.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// noopRegexCompiler disables pattern validation at the middleware level for
// request body schemas. Patterns remain in the spec for documentation; the
// handler enforces them and returns 422 for semantic violations.
func noopRegexCompiler(_ string) (openapi3.RegexMatcher, error) {
	return alwaysMatch{}, nil
}

type alwaysMatch struct{}

func (alwaysMatch) MatchString(string) bool { return true }

// stripHandlerValidatedConstraints removes pattern/minLength/maxLength from
// parameter schemas annotated with x-validated-by: handler. This works around
// kin-openapi not applying RegexCompiler to parameter validation (only to
// request body validation), which would cause the middleware to reject with 400
// before the handler can return 422.
func stripHandlerValidatedConstraints(spec *openapi3.T) {
	for _, pathItem := range spec.Paths.Map() {
		for _, op := range pathItem.Operations() {
			for _, paramRef := range op.Parameters {
				param := paramRef.Value
				if param == nil || param.Schema == nil || param.Schema.Value == nil {
					continue
				}
				schema := param.Schema.Value
				if v, ok := schema.Extensions["x-validated-by"]; ok && v == "handler" {
					schema.Pattern = ""
					schema.MinLength = 0
					schema.MaxLength = nil
				}
			}
		}
	}
}
