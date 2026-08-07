package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/dcm-project/environment-agent/internal/messaging"
	"github.com/dcm-project/environment-agent/internal/provider"
	"github.com/dcm-project/environment-agent/internal/provider/service"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
	"github.com/dcm-project/environment-agent/internal/routing/retry"
)

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
	if err := cfg.ValidateHandlerAckWaitInvariant(); err != nil {
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

	// Messaging client — must start before registrar (provides ConsumerLagProvider)
	msgClient, topicMain, err := setupMessaging(cfg, logger)
	if err != nil {
		logger.Error("invalid topic name", "error", err)
		return 1
	}

	// Wire routing before starting messaging so handlers are set
	denyList := routing.NewResourceSet(cfg.Routing.DenyListMaxSize)
	forwarder := routing.NewForwarder(routing.ForwarderConfig{Logger: logger})
	router := routing.NewRouter(routing.RouterDeps{
		Registry:      registry,
		HealthTracker: healthTracker,
		Store:         fileStore,
		Forwarder:     forwarder,
		Publisher:     msgClient,
		DenyList:      denyList,
		Config:        cfg.Routing,
		Logger:        logger,
		AgentName:     cfg.Agent.Name,
		TopicName:     topicMain,
	})
	msgClient.SetMainHandler(router.HandleRequest)
	msgClient.SetCancelHandler(router.HandleCancel)

	if err := msgClient.Start(ctx); err != nil {
		logger.Error("failed to start messaging client", "error", err)
		return 1
	}
	defer msgClient.Stop()

	// Retry processor — wired after messaging client starts (JS context ready)
	retryProcessor := retry.NewProcessor(retry.ProcessorDeps{
		Registry:            registry,
		HealthTracker:       healthTracker,
		Store:               fileStore,
		Forwarder:           forwarder,
		Publisher:           msgClient,
		JSProvider:          msgClient.JS,
		DenyList:            denyList,
		ClaimedResourcesSet: router.ClaimedResourcesSet(),
		Config: retry.ProcessorConfig{
			MaxDeliver:     cfg.Messaging.MaxDeliver,
			HandlerTimeout: cfg.Routing.HandlerTimeout,
			NakDelay:       cfg.Routing.NakDelay,
		},
		Logger:    logger,
		AgentName: cfg.Agent.Name,
		TopicName: topicMain,
	})
	router.SetRetryConsumer(retryProcessor)
	defer retryProcessor.Stop()

	// DCM Registrar — created before monitor starts so callbacks can be wired
	// before any health transitions fire. Deferred after monitor so LIFO shuts
	// registrar down first.
	registrar, err := dcm.NewRegistrar(
		dcm.RegistrarConfig{
			AgentName:                 cfg.Agent.Name,
			Environment:               cfg.Agent.Environment,
			Cost:                      cfg.Agent.Cost,
			TopicName:                 topicMain,
			RegistrationURL:           cfg.DCM.RegistrationURL,
			InitialBackoff:            cfg.DCM.InitialBackoff,
			MaxBackoff:                cfg.DCM.MaxBackoff,
			HeartbeatInterval:         cfg.Heartbeat.Interval,
			PrerequisiteRetryInterval: cfg.DCM.PrerequisiteRetryInterval,
		},
		&serviceTypeLister{providerSvc: providerSvc, logger: logger},
		msgClient,
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
	healthCEPub := health.NewCEPublisher(fileStore, msgClient, logger, cfg.Agent.Name, topicMain)
	healthMonitor.SetOnTransition(func(providerID string, from, to v1alpha1.ProviderStatus) {
		if from == v1alpha1.Unavailable || to == v1alpha1.Unavailable {
			registrar.NotifyServiceTypeChange()
		}
		retryProcessor.RunTransition(ctx, providerID, from, to)
		healthCEPub.OnTransition(ctx, providerID, from, to)
	})
	providerSvc.SetOnChange(registrar.NotifyServiceTypeChange)

	if err := retryProcessor.ProcessOnRestart(ctx); err != nil {
		logger.Error("failed to process retry on restart", "error", err)
	}

	healthMonitor.Start(ctx)
	defer healthMonitor.Stop()

	registrar.NotifyServiceTypeChange()

	regCtx, regCancel := context.WithCancel(context.Background())
	registrar.Start(regCtx)
	defer func() {
		regCancel()
		<-registrar.Done()
	}()

	healthSvc := health.NewService(msgClient)
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

func setupMessaging(cfg *config.Config, logger *slog.Logger) (*messaging.Client, string, error) {
	topics := messaging.DeriveTopicNames(cfg.Agent.Name, cfg.Messaging.TopicName)
	if err := messaging.ValidateTopicName(topics.Main); err != nil {
		return nil, "", fmt.Errorf("invalid topic name: %w", err)
	}
	client := messaging.NewClient(messaging.ClientConfig{
		URL:            cfg.Messaging.URL,
		TopicName:      topics.Main,
		AgentName:      cfg.Agent.Name,
		AckWait:        cfg.Messaging.AckWait,
		CancelAckWait:  cfg.Messaging.CancelAckWait,
		MaxDeliver:     cfg.Messaging.MaxDeliver,
		HandlerTimeout: cfg.Routing.HandlerTimeout,
		NakDelay:       cfg.Routing.NakDelay,
	}, logger)
	return client, topics.Main, nil
}
