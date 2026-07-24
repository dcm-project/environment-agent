// Package apiserver provides HTTP server lifecycle management for the environment agent.
package apiserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	nethttpmiddleware "github.com/oapi-codegen/nethttp-middleware"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/httperror"
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
	r.Use(RequestLogger(s.logger))
	r.Use(RequestTimeout(s.cfg.Server.RequestTimeout, s.logger))

	spec, err := v1alpha1.GetSpec()
	if err != nil {
		return err
	}

	r.Use(nethttpmiddleware.OapiRequestValidatorWithOptions(spec, &nethttpmiddleware.Options{
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

	serveCh := make(chan error, 1)
	go func() {
		if err := s.srv.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveCh <- err
		} else {
			serveCh <- nil
		}
	}()

	s.logger.Info("server listening", "address", s.listener.Addr().String())

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
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}
