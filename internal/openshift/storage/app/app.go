// Package app wires the storage service provider for in-process (embedded) use.
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/openshift/storage/config"
	k8s "github.com/dcm-project/environment-agent/internal/openshift/storage/kubernetes"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/monitoring"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	"github.com/dcm-project/environment-agent/internal/openshift/worker"
)

// App holds wired domain services and optional background workers.
type App struct {
	// Configuration
	cfg *config.Config

	// Dependencies
	repo      store.VolumeRepository
	publisher *monitoring.NATSPublisher
	monitor   *monitoring.StatusMonitor
	logger    *slog.Logger

	// Lifecycle
	lifecycle worker.AppLifecycle
}

// Options configures App construction and background lifecycle.
type Options struct {
	// DisableMonitor skips the PVC status monitor and NATS publisher.
	DisableMonitor bool
}

// New constructs an App: Kubernetes client, volume store, and status monitor.
func New(_ context.Context, cfg *config.Config, logger *slog.Logger, opts Options) (*App, error) {
	if cfg == nil {
		panic("storage app: config must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	a := &App{cfg: cfg, logger: logger, lifecycle: worker.NewAppLifecycle(logger)}

	if opts.DisableMonitor {
		repo, err := a.buildStore()
		if err != nil {
			return nil, fmt.Errorf("building volume store: %w", err)
		}
		a.repo = repo
		return a, nil
	}

	publisher, err := monitoring.NewNATSPublisher(cfg.MessagingURL, cfg.Name, logger)
	if err != nil {
		return nil, fmt.Errorf("creating NATS publisher: %w", err)
	}

	k8sClient, err := k8s.NewClient(cfg.Kubeconfig)
	if err != nil {
		_ = publisher.Close()
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	k8sCfg := k8s.K8sConfig{
		Namespace:           cfg.Namespace,
		DefaultStorageClass: cfg.DefaultStorageClass,
		DefaultAccessMode:   cfg.DefaultAccessMode,
	}
	repo := k8s.NewK8sVolumeStore(k8sClient, k8sCfg, logger)

	monitorCfg := monitoring.MonitorConfig{
		Namespace:          cfg.Namespace,
		DebounceMs:         cfg.DebounceMs,
		ResyncPeriod:       cfg.ResyncPeriod,
		PublishMaxAttempts: cfg.PublishMaxAttempts,
	}
	statusMonitor := monitoring.NewStatusMonitor(k8sClient, monitorCfg, publisher, logger)

	a.repo = repo
	a.publisher = publisher
	a.lifecycle.RegisterCloser(publisher)
	a.monitor = statusMonitor
	return a, nil
}

func (a *App) buildStore() (store.VolumeRepository, error) {
	k8sClient, err := k8s.NewClient(a.cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}
	k8sCfg := k8s.K8sConfig{
		Namespace:           a.cfg.Namespace,
		DefaultStorageClass: a.cfg.DefaultStorageClass,
		DefaultAccessMode:   a.cfg.DefaultAccessMode,
	}
	return k8s.NewK8sVolumeStore(k8sClient, k8sCfg, a.logger), nil
}

// Store returns the in-process volume repository.
func (a *App) Store() store.VolumeRepository {
	return a.repo
}

// Start launches background workers (status monitor). It is non-blocking.
func (a *App) Start(ctx context.Context) {
	a.lifecycle.Start(ctx, func(taskCtx context.Context) error {
		if a.monitor == nil {
			return nil
		}
		return a.monitor.Start(taskCtx)
	}, "storage status monitor failed")
}

// Close stops the status monitor and releases resources such as the NATS connection.
func (a *App) Close() error {
	return a.lifecycle.Close()
}
