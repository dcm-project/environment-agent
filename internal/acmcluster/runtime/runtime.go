// Package runtime wires the ACM cluster service provider's domain services for
// standalone or embedded use. Standalone callers still attach an HTTP server
// and SPM registration via cmd/acm-cluster-service-provider; embedded callers
// (e.g. environment-agent) use ClusterService and HealthChecker in-process.
package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster/dispatcher"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/health"
	"github.com/dcm-project/environment-agent/internal/acmcluster/monitoring"
	"github.com/dcm-project/environment-agent/internal/acmcluster/registration"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	spmclient "github.com/dcm-project/service-provider-manager/pkg/client/provider"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Runtime holds wired domain services and optional background workers.
type Runtime struct {
	cfg            *config.Config
	clusterService service.ClusterService
	healthChecker  service.HealthChecker
	monitor        *monitoring.StatusMonitor
	publisher      *monitoring.NATSPublisher
	registrar      *registration.Registrar
	logger         *slog.Logger

	startOnce     sync.Once
	monitorCancel context.CancelFunc
	monitorDone   chan struct{}
	closeOnce     sync.Once
}

// PrepareConfig loads derived configuration values that are not set directly
// from environment variables: the OCP→K8s compatibility matrix and the shared
// pull secret name.
func PrepareConfig(cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is required")
	}

	matrix, err := registration.LoadCompatibilityMatrix(cfg.Cluster.VersionMatrixPath)
	if err != nil {
		return fmt.Errorf("loading compatibility matrix: %w", err)
	}
	cfg.Cluster.VersionMatrix = map[string]string(matrix)
	cfg.Cluster.PullSecretName = cfg.Registration.ProviderName + "-pull-secret"
	return nil
}

// New constructs a Runtime: Kubernetes clients, cluster service, health checker,
// and optionally SPM registration and the status monitor. cfg must already have
// passed config.Load (or equivalent) and PrepareConfig. Kubernetes clients may
// be injected via Options; when unset, kubeconfig is loaded automatically.
func New(ctx context.Context, cfg *config.Config, logger *slog.Logger, opts Options) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Cluster.PullSecretName == "" {
		return nil, fmt.Errorf("cluster pull secret name is empty")
	}

	restCfg, k8sClient, err := resolveKubernetesClients(opts)
	if err != nil {
		return nil, err
	}

	if err := cluster.EnsurePullSecret(ctx, k8sClient, cfg.Cluster, logger); err != nil {
		return nil, fmt.Errorf("ensuring pull secret: %w", err)
	}

	rt := &Runtime{
		cfg:            cfg,
		clusterService: dispatcher.New(k8sClient, cfg.Cluster, cfg.Health.EnabledPlatforms),
		healthChecker:  health.NewChecker(k8sClient, cfg.Health, opts.version(), time.Now()),
		logger:         logger,
	}

	if err := rt.initRegistration(k8sClient, opts); err != nil {
		return nil, err
	}
	if err := rt.initMonitor(restCfg, opts); err != nil {
		_ = rt.Close()
		return nil, err
	}

	return rt, nil
}

func resolveKubernetesClients(opts Options) (*rest.Config, client.Client, error) {
	if opts.KubernetesClient != nil {
		if opts.DisableMonitor || opts.RestConfig != nil {
			return opts.RestConfig, opts.KubernetesClient, nil
		}
		return nil, nil, fmt.Errorf("RestConfig is required when KubernetesClient is set and status monitor is enabled")
	}

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("loading kubeconfig: %w", err)
	}

	scheme, err := util.BuildScheme()
	if err != nil {
		return nil, nil, fmt.Errorf("building scheme: %w", err)
	}

	k8sClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	return restCfg, k8sClient, nil
}

func (r *Runtime) initRegistration(k8sClient client.Client, opts Options) error {
	if opts.DisableRegistration {
		return nil
	}

	dcmClient, err := spmclient.NewClientWithResponses(r.cfg.Registration.DCMRegistrationURL)
	if err != nil {
		return fmt.Errorf("creating DCM client: %w", err)
	}

	matrix := registration.CompatibilityMatrix(r.cfg.Cluster.VersionMatrix)
	r.registrar = registration.New(r.cfg.Registration, dcmClient, k8sClient, r.logger, matrix)
	return nil
}

func (r *Runtime) initMonitor(restCfg *rest.Config, opts Options) error {
	if opts.DisableMonitor {
		return nil
	}

	dynamicClient := opts.DynamicClient
	if dynamicClient == nil {
		if restCfg == nil {
			return fmt.Errorf("rest config is required for status monitor")
		}
		var err error
		dynamicClient, err = dynamic.NewForConfig(restCfg)
		if err != nil {
			return fmt.Errorf("creating dynamic kubernetes client: %w", err)
		}
	}

	publisher, err := monitoring.NewNATSPublisher(
		r.cfg.Monitoring.NATSUrl,
		r.cfg.Registration.ProviderName,
		r.logger,
	)
	if err != nil {
		return fmt.Errorf("creating NATS publisher: %w", err)
	}
	r.publisher = publisher

	monitorCfg := monitoring.MonitorConfig{
		Namespace:            r.cfg.Cluster.ClusterNamespace,
		ProviderName:         r.cfg.Registration.ProviderName,
		DebounceInterval:     r.cfg.Monitoring.DebounceInterval,
		ResyncInterval:       r.cfg.Monitoring.ResyncInterval,
		PublishRetryMax:      r.cfg.Monitoring.PublishRetryMax,
		PublishRetryInterval: r.cfg.Monitoring.PublishRetryInterval,
	}
	r.monitor = monitoring.New(dynamicClient, monitorCfg, publisher, r.logger)
	return nil
}

// Config returns the runtime configuration. Callers must not mutate it.
func (r *Runtime) Config() *config.Config {
	return r.cfg
}

// ClusterService returns the in-process cluster lifecycle service.
func (r *Runtime) ClusterService() service.ClusterService {
	return r.clusterService
}

// HealthChecker returns the dependency health checker.
func (r *Runtime) HealthChecker() service.HealthChecker {
	return r.healthChecker
}

// Start launches background workers (SPM registration and status monitor).
// It is non-blocking. Background workers start at most once, even if Start is
// called multiple times.
func (r *Runtime) Start(ctx context.Context) {
	if r.registrar != nil {
		r.registrar.Start(ctx)
	}

	r.startOnce.Do(func() {
		if r.monitor == nil {
			return
		}

		monitorCtx, cancel := context.WithCancel(ctx)
		r.monitorCancel = cancel
		done := make(chan struct{})
		r.monitorDone = done

		go func() {
			defer close(done)
			if err := r.monitor.Start(monitorCtx); err != nil && monitorCtx.Err() == nil {
				r.logger.Error("status monitor failed", "error", err)
			}
		}()
	})
}

// Close stops the status monitor and releases runtime resources such as the
// NATS connection. It is safe to call multiple times.
func (r *Runtime) Close() error {
	var err error
	r.closeOnce.Do(func() {
		if r.monitorCancel != nil {
			r.monitorCancel()
		}
		if r.monitorDone != nil {
			<-r.monitorDone
		}
		if r.publisher != nil {
			err = r.publisher.Close()
		}
	})
	return err
}
