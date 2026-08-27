package kubernetes

import (
	"context"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Create creates a new volume backed by a PersistentVolumeClaim.
func (s *K8sVolumeStore) Create(ctx context.Context, spec v1alpha1.StorageSpec, id string) (*v1alpha1.Volume, error) {
	spec.Metadata.Name = id
	name := id
	labels := dcmLabels(name)
	if spec.Metadata.Labels != nil {
		labels = mergeLabels(labels, *spec.Metadata.Labels)
	}

	storageClass := resolveStorageClass(spec, s.cfg.DefaultStorageClass)
	if storageClass != "" {
		if err := s.validateStorageClass(ctx, storageClass); err != nil {
			return nil, fmt.Errorf("validating StorageClass %q for volume %q: %w", storageClass, name, err)
		}
	}

	pvc, err := buildPVC(spec, s.cfg, labels)
	if err != nil {
		return nil, fmt.Errorf("building PVC for volume %q: %w", name, err)
	}

	created, err := s.client.CoreV1().PersistentVolumeClaims(s.cfg.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, &store.ConflictError{Message: fmt.Sprintf("volume %q already exists", name)}
		}
		return nil, fmt.Errorf("creating PVC %q in namespace %q: %w", name, s.cfg.Namespace, err)
	}

	return s.buildVolume(created, name), nil
}

func (s *K8sVolumeStore) validateStorageClass(ctx context.Context, name string) error {
	_, err := s.client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &store.FailedPreconditionError{Message: fmt.Sprintf("StorageClass %q does not exist", name)}
		}
		return fmt.Errorf("getting StorageClass %q: %w", name, err)
	}
	return nil
}
