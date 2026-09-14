// Package kubernetes implements Kubernetes-backed operations for the network SP.
package kubernetes

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// K8sNetworkStore implements store.NetworkRepository backed by Kubernetes Services.
type K8sNetworkStore struct {
	client kubernetes.Interface
	cfg    K8sConfig
	logger *slog.Logger
}

// NewK8sNetworkStore creates a new K8sNetworkStore with the given client, config, and logger.
func NewK8sNetworkStore(client kubernetes.Interface, cfg K8sConfig, logger *slog.Logger) *K8sNetworkStore {
	if client == nil {
		panic("network store: client must not be nil")
	}
	if logger == nil {
		panic("network store: logger must not be nil")
	}
	return &K8sNetworkStore{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

var _ store.NetworkRepository = (*K8sNetworkStore)(nil)

// CheckHealth verifies the backing Kubernetes cluster is reachable.
func (s *K8sNetworkStore) CheckHealth(_ context.Context) error {
	_, err := s.client.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("kubernetes cluster discovery: %w", err)
	}
	return nil
}

func (s *K8sNetworkStore) findService(ctx context.Context, networkID string) (*corev1.Service, error) {
	services, err := s.client.CoreV1().Services(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: instanceSelector(networkID),
	})
	if err != nil {
		return nil, fmt.Errorf("listing services: %w", err)
	}
	if len(services.Items) == 0 {
		return nil, &store.NotFoundError{Resource: "network", ID: networkID}
	}
	if len(services.Items) > 1 {
		return nil, &store.ConflictError{Message: fmt.Sprintf("multiple services found with id %q", networkID)}
	}
	svc := &services.Items[0]
	if !isDCMManagedService(svc, networkID) {
		return nil, &store.NotFoundError{Resource: "network", ID: networkID}
	}
	return svc, nil
}
