// Package app wires the database service provider for in-process (embedded) use.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/openshift/database/config"
	"github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/database/monitoring"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	"github.com/dcm-project/environment-agent/internal/openshift/worker"
)

// App holds wired domain services and optional background workers.
type App struct {
	// required dependencies
	cfg    *config.Config
	logger *slog.Logger

	// core services
	repo store.DatabaseRepository

	// optional services (nil when monitoring is disabled)
	publisher *monitoring.NATSPublisher
	monitor   *monitoring.StatusMonitor

	// infrastructure
	lifecycle worker.AppLifecycle
}

// Options configures App construction and background lifecycle.
type Options struct {
	// DisableMonitor skips the Cluster/Pod status monitor and NATS publisher.
	DisableMonitor bool
}

// New constructs an App: Kubernetes client, database store, and status monitor.
func New(_ context.Context, cfg *config.Config, logger *slog.Logger, opts Options) (*App, error) {
	if cfg == nil {
		panic("database app: config must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	a := &App{cfg: cfg, logger: logger, lifecycle: worker.NewAppLifecycle(logger)}

	if opts.DisableMonitor {
		repo, err := a.buildStore()
		if err != nil {
			return nil, err
		}
		a.repo = repo
		return a, nil
	}

	publisher, err := monitoring.NewNATSPublisher(cfg.MessagingURL, cfg.Name, logger)
	if err != nil {
		return nil, fmt.Errorf("creating NATS publisher: %w", err)
	}

	k8sClient, dynClient, err := kubernetes.NewClients(cfg.Kubeconfig)
	if err != nil {
		_ = publisher.Close()
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	k8sCfg := kubernetes.CnpgK8sConfig{
		Namespace:           cfg.Namespace,
		ExternalServiceType: cfg.ExternalServiceType,
		ImageCatalogName:    cfg.ImageCatalog,
		DefaultStorageClass: cfg.DefaultStorageClass,
		DefaultVersion:      cfg.DefaultVersion,
	}
	repo := kubernetes.NewCnpgDatabaseStore(k8sClient, dynClient, k8sCfg, logger)

	monitorCfg := monitoring.MonitorConfig{
		Namespace:             cfg.Namespace,
		ProviderName:          cfg.Name,
		DebounceMs:            cfg.DebounceMs,
		ResyncPeriod:          cfg.ResyncPeriod,
		PublishMaxAttempts:    cfg.PublishMaxAttempts,
		PublishTimeoutSeconds: cfg.PublishTimeoutSeconds,
	}
	statusMonitor := monitoring.NewStatusMonitor(k8sClient, dynClient, monitorCfg, publisher, logger)

	a.repo = repo
	a.publisher = publisher
	a.lifecycle.RegisterCloser(publisher)
	a.monitor = statusMonitor
	return a, nil
}

func (a *App) buildStore() (store.DatabaseRepository, error) {
	k8sClient, dynClient, err := kubernetes.NewClients(a.cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}
	k8sCfg := kubernetes.CnpgK8sConfig{
		Namespace:           a.cfg.Namespace,
		ExternalServiceType: a.cfg.ExternalServiceType,
		ImageCatalogName:    a.cfg.ImageCatalog,
		DefaultStorageClass: a.cfg.DefaultStorageClass,
		DefaultVersion:      a.cfg.DefaultVersion,
	}
	return kubernetes.NewCnpgDatabaseStore(k8sClient, dynClient, k8sCfg, a.logger), nil
}

// Store returns the in-process database repository.
func (a *App) Store() store.DatabaseRepository {
	return a.repo
}

// Start launches background workers (status monitor). It is non-blocking.
func (a *App) Start(ctx context.Context) {
	a.lifecycle.Start(ctx, func(taskCtx context.Context) error {
		if a.monitor == nil {
			return nil
		}
		return a.monitor.Start(taskCtx)
	}, "database status monitor failed")
}

// Close stops the status monitor and releases resources such as the NATS connection.
func (a *App) Close() error {
	return a.lifecycle.Close()
}
