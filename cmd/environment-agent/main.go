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

	fileStore, err := store.NewFileStore(cfg.Provider.PersistencePath, logger)
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
	// RegisterEmbedded is deliberately NOT called here: its synchronous
	// initialCheck can fire healthMonitor's onTransition callback in-line,
	// which isn't wired via SetOnTransition until further down. It's called
	// after that wiring, below.

	// Messaging client — must start before registrar (provides ConsumerLagProvider)
	msgClient, topics, err := setupMessaging(cfg, logger)
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
		TopicName:     topics.Main,
		RetryTopic:    topics.Retry,
	})
	msgClient.SetMainHandler(router.HandleRequest)
	msgClient.SetCancelHandler(router.HandleCancel)

	// Retry processor — constructed before Start so its JSProvider/Publisher
	// method values are ready to bind once JetStream actually comes up; the
	// underlying msgClient.JS()/msgClient itself only need to be non-nil,
	// not yet "started" (see JSProvider: msgClient.JS below).
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
			HandlerTimeout: cfg.Routing.HandlerTimeout,
		},
		Logger:    logger,
		AgentName: cfg.Agent.Name,
		Topics:    topics,
	})
	router.SetRetryConsumer(retryProcessor)

	// ProcessOnRestart must drain the main/cancel durable consumers before
	// live pull-consumption begins, and Start is non-blocking (AC-MSG-050),
	// so this is wired via SetOnSetupReady rather than run synchronously
	// after Start — see ClientConfig.DeferConsume.
	msgClient.SetOnSetupReady(func() {
		if err := retryProcessor.ProcessOnRestart(ctx); err != nil {
			logger.Error("failed to process retry on restart", "error", err)
		}
		msgClient.StartConsuming()
	})

	if err := msgClient.Start(ctx); err != nil {
		logger.Error("failed to start messaging client", "error", err)
		return 1
	}

	// Stop messaging before the retry processor (not LIFO-by-construction):
	// msgClient.Stop() must quiesce in-flight handlers and stop accepting new
	// messages before it makes sense to wait for retryProcessor's own
	// in-flight RunTransition goroutines.
	defer func() {
		msgClient.Stop()
		retryProcessor.Stop()
	}()

	// DCM Registrar — created before monitor starts so callbacks can be wired
	// before any health transitions fire. Deferred after monitor so LIFO shuts
	// registrar down first.
	registrar, err := dcm.NewRegistrar(
		dcm.RegistrarConfig{
			AgentName:                 cfg.Agent.Name,
			Environment:               cfg.Agent.Environment,
			Cost:                      cfg.Agent.Cost,
			TopicName:                 topics.Main,
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

	// Wire service-type change notifications BEFORE RegisterEmbedded:
	// RegisterEmbedded's synchronous initialCheck can fire a transition
	// in-line, and that must not be missed by an as-yet-unset callback.
	healthCEPub := health.NewCEPublisher(fileStore, msgClient, logger, cfg.Agent.Name, topics.Main)
	healthMonitor.SetOnTransition(func(providerID string, from, to v1alpha1.ProviderStatus) {
		if from == v1alpha1.Unavailable || to == v1alpha1.Unavailable {
			registrar.NotifyServiceTypeChange()
		}
		retryProcessor.RunTransition(ctx, providerID, from, to)
		healthCEPub.OnTransition(ctx, providerID, from, to)
	})
	providerSvc.SetOnChange(registrar.NotifyServiceTypeChange)

	// Now safe to register embedded SPs: any transition their initialCheck
	// triggers is captured by the callback wired immediately above.
	providerSvc.RegisterEmbedded(cfg.Provider.EmbeddedSPs)

	healthMonitor.Start(ctx)
	defer healthMonitor.Stop()

	// Explicit kick: the steady-state case (SP already at its initial
	// status) never fires the transition callback above (from == to).
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
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			httperror.WriteInvalidArgument(w, r, logger, err.Error())
		},
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

func setupMessaging(cfg *config.Config, logger *slog.Logger) (*messaging.Client, messaging.TopicNames, error) {
	topics := messaging.DeriveTopicNames(cfg.Agent.Name, cfg.Messaging.TopicName)
	if err := messaging.ValidateTopicName(topics.Base); err != nil {
		return nil, messaging.TopicNames{}, fmt.Errorf("invalid topic name: %w", err)
	}
	// TopicName is the raw override (or empty), not the derived/prefixed Main
	// subject — messaging.NewClient calls DeriveTopicNames itself, so passing
	// the already-prefixed value here would double-prefix it.
	client := messaging.NewClient(messaging.ClientConfig{
		URL:                     cfg.Messaging.URL,
		TopicName:               cfg.Messaging.TopicName,
		AgentName:               cfg.Agent.Name,
		AckWait:                 cfg.Messaging.AckWait,
		CancelAckWait:           cfg.Messaging.CancelAckWait,
		MaxDeliver:              cfg.Messaging.MaxDeliver,
		HandlerTimeout:          cfg.Routing.HandlerTimeout,
		NakDelay:                cfg.Routing.NakDelay,
		ReconnectInitialBackoff: cfg.Messaging.ReconnectInitialBackoff,
		ReconnectMaxBackoff:     cfg.Messaging.ReconnectMaxBackoff,
		// DeferConsume: live main/cancel consumption is started explicitly
		// below (msgClient.StartConsuming) only after retryProcessor.
		// ProcessOnRestart has drained those same durable consumers — see
		// ClientConfig.DeferConsume and the message-stealing race it prevents.
		DeferConsume: true,
	}, logger)
	return client, topics, nil
}
