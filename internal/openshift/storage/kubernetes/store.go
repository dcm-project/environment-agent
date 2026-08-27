// Package kubernetes implements Kubernetes-backed operations for the storage SP.
package kubernetes

import (
	"context"
	"fmt"
	"log/slog"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// K8sVolumeStore implements store.VolumeRepository backed by PersistentVolumeClaims.
type K8sVolumeStore struct {
	client kubernetes.Interface
	cfg    K8sConfig
	logger *slog.Logger
}

// NewK8sVolumeStore creates a new K8sVolumeStore with the given client, config, and logger.
func NewK8sVolumeStore(client kubernetes.Interface, cfg K8sConfig, logger *slog.Logger) *K8sVolumeStore {
	if client == nil {
		panic("kubernetes volume store: client must not be nil")
	}
	if logger == nil {
		panic("kubernetes volume store: logger must not be nil")
	}
	return &K8sVolumeStore{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
}

var _ store.VolumeRepository = (*K8sVolumeStore)(nil)

// CheckHealth verifies the backing Kubernetes cluster is reachable.
func (s *K8sVolumeStore) CheckHealth(_ context.Context) error {
	_, err := s.client.Discovery().ServerVersion()
	if err != nil {
		return fmt.Errorf("kubernetes discovery health check: %w", err)
	}
	return nil
}

func (s *K8sVolumeStore) findPVC(ctx context.Context, volumeID string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := s.client.CoreV1().PersistentVolumeClaims(s.cfg.Namespace).Get(ctx, volumeID, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &store.NotFoundError{ID: volumeID}
		}
		return nil, fmt.Errorf("getting PVC %q in namespace %q: %w", volumeID, s.cfg.Namespace, err)
	}
	if !isDCMManagedPVC(pvc, volumeID) {
		return nil, &store.NotFoundError{ID: volumeID}
	}
	return pvc, nil
}

func (s *K8sVolumeStore) buildVolume(pvc *corev1.PersistentVolumeClaim, instanceID string) *v1alpha1.Volume {
	v := volumeFromPVC(pvc, instanceID)
	return &v
}
