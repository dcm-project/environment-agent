package monitoring

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dcm-project/environment-agent/internal/acmcluster/cluster"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type resourceType string

const (
	resourceTypeHostedCluster resourceType = "hostedcluster"
	resourceTypeNodePool      resourceType = "nodepool"
)

// StatusMonitor watches HostedCluster and NodePool resources and publishes status CloudEvents.
type StatusMonitor struct {
	dynamicClient dynamic.Interface
	cfg           MonitorConfig
	publisher     StatusPublisher
	logger        *slog.Logger
}

// New creates a new StatusMonitor.
func New(dynamicClient dynamic.Interface, cfg MonitorConfig, publisher StatusPublisher, logger *slog.Logger) *StatusMonitor {
	return &StatusMonitor{
		dynamicClient: dynamicClient,
		cfg:           cfg,
		publisher:     publisher,
		logger:        logger,
	}
}

// Start begins watching HostedCluster and NodePool resources. It blocks until ctx is cancelled.
func (m *StatusMonitor) Start(ctx context.Context) error {
	state := newReconcileState()

	selector := fmt.Sprintf("%s=%s,%s=%s",
		cluster.LabelManagedBy, cluster.ValueManagedBy,
		cluster.LabelServiceType, cluster.ValueServiceType,
	)

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		m.dynamicClient,
		m.cfg.ResyncInterval,
		m.cfg.Namespace,
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		},
	)

	hcInformer := factory.ForResource(util.HostedClusterGVR).Informer()
	npInformer := factory.ForResource(util.NodePoolGVR).Informer()

	if err := hcInformer.AddIndexers(cache.Indexers{
		InstanceIDIndex: InstanceIDIndexFunc,
	}); err != nil {
		return fmt.Errorf("adding HC indexers: %w", err)
	}

	if err := npInformer.AddIndexers(cache.Indexers{
		InstanceIDIndex: InstanceIDIndexFunc,
	}); err != nil {
		return fmt.Errorf("adding NP indexers: %w", err)
	}

	debouncer := NewDebouncer(m.cfg.DebounceInterval, func(event StatusEvent) {
		m.publishWithRetry(ctx, event)
	})

	reconciler := func(rt resourceType) func(obj any) {
		return func(obj any) {
			uns, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
			instanceID, event := m.reconcileResource(uns, rt, state)
			if event != nil {
				debouncer.Submit(instanceID, *event)
			}
		}
	}

	reconcileHC := reconciler(resourceTypeHostedCluster)
	reconcileNP := reconciler(resourceTypeNodePool)

	if _, err := hcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: reconcileHC,
		UpdateFunc: func(_, newObj any) {
			reconcileHC(newObj)
		},
		DeleteFunc: func(obj any) {
			id, event := m.handleDeleteHostedCluster(obj, state)
			if event != nil {
				debouncer.Submit(id, *event)
			}
		},
	}); err != nil {
		return fmt.Errorf("adding HC event handler: %w", err)
	}

	if _, err := npInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: reconcileNP,
		UpdateFunc: func(_, newObj any) {
			reconcileNP(newObj)
		},
		DeleteFunc: func(obj any) {
			id, event := m.handleDeleteNodePool(obj, state)
			if event != nil {
				debouncer.Submit(id, *event)
			}
		},
	}); err != nil {
		return fmt.Errorf("adding NP event handler: %w", err)
	}

	factory.Start(ctx.Done())
	defer factory.Shutdown()
	defer debouncer.Stop()

	synced := factory.WaitForCacheSync(ctx.Done())
	if ctx.Err() == nil {
		for gvr, ok := range synced {
			if !ok {
				m.logger.Error("cache sync failed, aborting start", "type", gvr)
				return fmt.Errorf("cache sync failed for %v", gvr)
			}
		}
	}

	<-ctx.Done()
	return nil
}

// publishWithRetry publishes an event with exponential backoff retry.
func (m *StatusMonitor) publishWithRetry(ctx context.Context, event StatusEvent) {
	backoff := m.cfg.PublishRetryInterval
	for attempt := 0; attempt <= m.cfg.PublishRetryMax; attempt++ {
		if err := m.publisher.Publish(ctx, event); err != nil {
			m.logger.Warn("publish failed",
				"instanceID", event.InstanceID,
				"attempt", attempt+1,
				"error", err,
			)
			if attempt < m.cfg.PublishRetryMax {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
			}
			continue
		}
		return
	}
	m.logger.Warn("dropping event after retry exhaustion",
		"instanceID", event.InstanceID,
	)
}
