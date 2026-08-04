package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	oapigen "github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/handler"
	"github.com/dcm-project/environment-agent/internal/health"
	"github.com/dcm-project/environment-agent/internal/health/monitor"
	"github.com/dcm-project/environment-agent/internal/httperror"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/service"
	"github.com/dcm-project/environment-agent/internal/provider/store"
)

// TODO: replace with real MessagingStatus from the NATS/messaging subsystem.
type messagingStatus struct{}

func (messagingStatus) IsConnected() bool { return true }

func main() {
	code := mainRun()
	os.Exit(code)
}

func mainRun() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return run(ctx)
}

func run(ctx context.Context) int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Environment Agent starting")

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		return 1
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		return 1
	}

	ln, err := net.Listen("tcp", cfg.Server.Address)
	if err != nil {
		logger.Error("failed to listen", "error", err, "address", cfg.Server.Address)
		return 1
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			logger.Error("failed to close listener", "error", closeErr)
		}
	}()

	fileStore, err := store.NewFileStore(cfg.Provider.PersistencePath)
	if err != nil {
		logger.Error("failed to initialize provider store", "error", err, "path", cfg.Provider.PersistencePath)
		return 1
	}
	registry := provider.NewRegistry()
	healthTracker := provider.NewInMemoryHealthTracker()
	healthMonitor := monitor.New(healthTracker, cfg.Health, logger)
	providerSvc := service.New(fileStore, registry, healthTracker, healthMonitor, logger)

	if err := providerSvc.LoadPersisted(); err != nil {
		logger.Error("failed to load persisted providers", "error", err)
		return 1
	}
	providerSvc.RegisterEmbedded(cfg.Provider.EmbeddedSPs)

	healthMonitor.Start(ctx)
	defer healthMonitor.Stop()

	healthSvc := health.NewService(messagingStatus{})
	strictHandler := handler.New(healthSvc, providerSvc)
	h := oapigen.NewStrictHandlerWithOptions(strictHandler, nil, oapigen.StrictHTTPServerOptions{
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			httperror.WriteResponse(w, logger, http.StatusInternalServerError,
				"INTERNAL", "Internal Server Error",
				err.Error(), &r.RequestURI)
		},
	})
	srv := apiserver.New(cfg, logger, h)

	if err := srv.Run(ctx, ln); err != nil {
		logger.Error("server error", "error", err)
		return 1
	}
	logger.Info("Environment Agent stopped")
	return 0
}
