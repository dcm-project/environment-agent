package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"strings"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultPageSize  = 50
	maxPageSizeLimit = 1000
)

func normalizePageSize(maxPageSize int32) int32 {
	if maxPageSize <= 0 {
		return defaultPageSize
	}
	if maxPageSize > maxPageSizeLimit {
		return maxPageSizeLimit
	}
	return maxPageSize
}

// List returns a paginated list of volumes using Kubernetes Limit/Continue
// tokens as the AEP opaque page_token / next_page_token.
func (s *K8sVolumeStore) List(ctx context.Context, maxPageSize int32, pageToken string) (*v1alpha1.VolumeList, error) {
	maxPageSize = normalizePageSize(maxPageSize)

	pvcs, err := s.client.CoreV1().PersistentVolumeClaims(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: dcmSelector(),
		Limit:         int64(maxPageSize),
		Continue:      pageToken,
	})
	if err != nil {
		return nil, fmt.Errorf("listing PVCs in namespace %q: %w", s.cfg.Namespace, mapListError(err))
	}

	volumes := make([]v1alpha1.Volume, 0, len(pvcs.Items))
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if !isDCMManagedPVC(pvc, pvc.Name) {
			continue
		}
		volumes = append(volumes, *s.buildVolume(pvc, pvc.Name))
	}

	result := &v1alpha1.VolumeList{
		Volumes: &volumes,
	}
	if pvcs.Continue != "" {
		token := pvcs.Continue
		result.NextPageToken = &token
	}

	return result, nil
}

func mapListError(err error) error {
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		s := status.Status()
		if s.Code == 400 {
			msg := strings.ToLower(s.Message)
			if strings.Contains(msg, "continue") || strings.Contains(msg, "page_token") || strings.Contains(msg, "page token") {
				return &store.InvalidArgumentError{Message: "invalid page_token"}
			}
			if s.Message != "" {
				return &store.InvalidArgumentError{Message: s.Message}
			}
			return &store.InvalidArgumentError{Message: "invalid argument"}
		}
	}
	return err
}
