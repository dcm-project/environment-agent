package kubernetes

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Delete removes a network by deleting its Kubernetes Service.
func (s *K8sNetworkStore) Delete(ctx context.Context, networkID string) error {
	service, err := s.findService(ctx, networkID)
	if err != nil {
		return fmt.Errorf("finding service: %w", err)
	}

	err = s.client.CoreV1().Services(s.cfg.Namespace).Delete(ctx, service.Name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deleting service: %w", err)
	}
	return nil
}
