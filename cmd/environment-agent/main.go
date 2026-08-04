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

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	oapigen "github.com/dcm-project/environment-agent/internal/api/server"
	"github.com/dcm-project/environment-agent/internal/apiserver"
	"github.com/dcm-project/environment-agent/internal/config"
	"github.com/dcm-project/environment-agent/internal/dcm"
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

// stubConsumerLagProvider returns 0 until Topic 7 provides real NATS consumer lag.
type stubConsumerLagProvider struct{}

func (stubConsumerLagProvider) ConsumerLag() int64 { return 0 }

// serviceTypeLister adapts ProviderService to dcm.ServiceTypeLister.
type serviceTypeLister struct {
	providerSvc *service.ProviderService
	logger      *slog.Logger
}

func (s *serviceTypeLister) AdvertisableServiceTypes() []string {
	providers, err := s.providerSvc.List(context.Background())
	if err != nil {
		s.logger.Error("failed to list providers for advertisable service types", "error", err)
		return nil
	}
	var types []string
	for _, p := range providers {
		if p.Status != nil && *p.Status != v1alpha1.Unavailable {
			types = append(types, p.ServiceType)
		}
	}
	return types
}

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

	// DCM Registrar — created before monitor starts so callbacks can be wired
	// before any health transitions fire. Deferred after monitor so LIFO shuts
	// registrar down first.
	topicName := cfg.Messaging.TopicName
	if topicName == "" {
		topicName = cfg.Agent.Name
	}
	registrar, err := dcm.NewRegistrar(
		dcm.RegistrarConfig{
			AgentName:                 cfg.Agent.Name,
			Environment:               cfg.Agent.Environment,
			Cost:                      cfg.Agent.Cost,
			TopicName:                 topicName,
			RegistrationURL:           cfg.DCM.RegistrationURL,
			InitialBackoff:            cfg.DCM.InitialBackoff,
			MaxBackoff:                cfg.DCM.MaxBackoff,
			HeartbeatInterval:         cfg.Heartbeat.Interval,
			PrerequisiteRetryInterval: cfg.DCM.PrerequisiteRetryInterval,
		},
		&serviceTypeLister{providerSvc: providerSvc, logger: logger},
		stubConsumerLagProvider{},
		nil,
		logger,
	)
	if err != nil {
		logger.Error("failed to create DCM registrar", "error", err)
		return 1
	}

	// Wire service-type change notifications before starting the periodic
	// monitor loop. Note: embedded initialCheck transitions (from RegisterEmbedded
	// above) may fire before this wiring, but the one-time kick below compensates
	// by forcing a state re-evaluation.
	healthMonitor.SetOnTransition(func(_ string, from, to v1alpha1.ProviderStatus) {
		if from == v1alpha1.Unavailable || to == v1alpha1.Unavailable {
			registrar.NotifyServiceTypeChange()
		}
	})
	providerSvc.SetOnChange(registrar.NotifyServiceTypeChange)

	healthMonitor.Start(ctx)
	defer healthMonitor.Stop()

	registrar.NotifyServiceTypeChange()

	regCtx, regCancel := context.WithCancel(context.Background())
	registrar.Start(regCtx)
	defer func() {
		regCancel()
		<-registrar.Done()
	}()

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
