package monitoring

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	k8sstore "github.com/dcm-project/environment-agent/internal/openshift/network/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const shutdownPublishTimeout = 30 * time.Second

// StatusMonitor watches network service resources for DCM-managed networks and
// publishes status change events via a StatusPublisher.
type StatusMonitor struct {
	client    kubernetes.Interface
	cfg       MonitorConfig
	publisher StatusPublisher
	logger    *slog.Logger

	mu            sync.Mutex
	lastPublished map[string]StatusEvent
	lastSubmitted map[string]StatusEvent // queued to debouncer (before NATS ack)
}

// NewStatusMonitor creates a new StatusMonitor.
func NewStatusMonitor(client kubernetes.Interface, cfg MonitorConfig, publisher StatusPublisher, logger *slog.Logger) *StatusMonitor {
	if client == nil {
		panic("status monitor: client must not be nil")
	}
	if publisher == nil {
		panic("status monitor: publisher must not be nil")
	}
	if logger == nil {
		panic("status monitor: logger must not be nil")
	}
	return &StatusMonitor{
		client:        client,
		cfg:           cfg,
		publisher:     publisher,
		logger:        logger,
		lastPublished: make(map[string]StatusEvent),
		lastSubmitted: make(map[string]StatusEvent),
	}
}

// Start begins watching for network service changes. It blocks until ctx is cancelled.
func (m *StatusMonitor) Start(ctx context.Context) error {
	selector := dcm.Selector()

	// Service factory: DCM label selector so we only track managed networks.
	serviceFactory := informers.NewSharedInformerFactoryWithOptions(
		m.client,
		m.cfg.ResyncPeriod,
		informers.WithNamespace(m.cfg.Namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector
		}),
	)

	serviceInformer := serviceFactory.Core().V1().Services().Informer()

	debouncer := NewDebouncer(
		time.Duration(m.cfg.DebounceMs)*time.Millisecond,
		func(event StatusEvent) {
			publishCtx := ctx
			if ctx.Err() != nil {
				var cancel context.CancelFunc
				publishCtx, cancel = context.WithTimeout(context.Background(), m.shutdownPublishTimeout())
				defer cancel()
			}
			m.publishWithRetry(publishCtx, event)
		},
	)

	if _, err := serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.handleServiceEvent(obj, debouncer)
		},
		UpdateFunc: func(_, newObj any) {
			m.handleServiceEvent(newObj, debouncer)
		},
		DeleteFunc: func(obj any) {
			m.handleServiceDelete(obj, debouncer)
		},
	}); err != nil {
		return fmt.Errorf("adding Service event handler: %w", err)
	}

	serviceFactory.Start(ctx.Done())
	defer serviceFactory.Shutdown()
	defer debouncer.Stop()

	if err := waitForCacheSync(ctx, serviceFactory); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

func (m *StatusMonitor) shutdownPublishTimeout() time.Duration {
	if m.cfg.ShutdownPublishTimeout > 0 {
		return m.cfg.ShutdownPublishTimeout
	}
	return shutdownPublishTimeout
}

func waitForCacheSync(ctx context.Context, factories ...informers.SharedInformerFactory) error {
	for _, f := range factories {
		synced := f.WaitForCacheSync(ctx.Done())
		if err := ctx.Err(); err != nil {
			return err
		}
		for typ, ok := range synced {
			if !ok {
				return fmt.Errorf("cache sync failed for %v", typ)
			}
		}
	}
	return nil
}

func (m *StatusMonitor) handleServiceEvent(obj any, debouncer *Debouncer) {
	service, ok := obj.(*corev1.Service)
	if !ok {
		return
	}
	instanceID := ExtractInstanceID(service)
	if instanceID == "" {
		return
	}

	status, msg := k8sstore.MapServiceToStatus(service)
	m.submitIfChanged(debouncer, StatusEvent{
		InstanceID: instanceID,
		Status:     status,
		Message:    msg,
	})
}

func (m *StatusMonitor) handleServiceDelete(obj any, debouncer *Debouncer) {
	instanceID := extractInstanceIDFromDelete(obj)
	if instanceID == "" {
		return
	}

	m.submitIfChanged(debouncer, StatusEvent{
		InstanceID: instanceID,
		Status:     v1alpha1.DELETED,
		Message:    "resource no longer exists",
	})
}

func extractInstanceIDFromDelete(obj any) string {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	meta, ok := obj.(metav1.Object)
	if !ok {
		return ""
	}
	return ExtractInstanceID(meta)
}

// submitIfChanged queues a publish only when status or message differs from the
// last successfully published event for this instance (resync-safe).
func (m *StatusMonitor) submitIfChanged(debouncer *Debouncer, event StatusEvent) {
	m.mu.Lock()
	if prev, ok := m.lastPublished[event.InstanceID]; ok &&
		prev.Status == event.Status && prev.Message == event.Message {
		m.mu.Unlock()
		return
	}
	m.lastSubmitted[event.InstanceID] = event
	m.mu.Unlock()
	debouncer.Submit(event.InstanceID, event)
}

func (m *StatusMonitor) publishWithRetry(ctx context.Context, event StatusEvent) {
	maxAttempts := m.cfg.PublishMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 5
	}
	backoff := 10 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := m.publisher.Publish(ctx, event); err != nil {
			m.logger.Error("failed to publish status event",
				"error", err,
				"resource_id", event.InstanceID,
				"attempt", attempt,
			)
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
			}
			continue
		}
		m.mu.Lock()
		if event.Status == v1alpha1.DELETED {
			delete(m.lastPublished, event.InstanceID)
			delete(m.lastSubmitted, event.InstanceID)
		} else {
			m.lastPublished[event.InstanceID] = event
		}
		m.mu.Unlock()
		return
	}
}
