package kubernetes

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Delete removes a volume by deleting its PersistentVolumeClaim.
func (s *K8sVolumeStore) Delete(ctx context.Context, volumeID string) error {
	pvc, err := s.findPVC(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("deleting volume %q: %w", volumeID, err)
	}
	err = s.client.CoreV1().PersistentVolumeClaims(s.cfg.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deleting PVC %q in namespace %q: %w", pvc.Name, s.cfg.Namespace, err)
	}
	return nil
}
