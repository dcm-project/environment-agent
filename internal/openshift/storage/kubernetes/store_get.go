package kubernetes

import (
	"context"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
)

// Get retrieves a volume by its DCM instance ID.
func (s *K8sVolumeStore) Get(ctx context.Context, volumeID string) (*v1alpha1.Volume, error) {
	pvc, err := s.findPVC(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("getting volume %q: %w", volumeID, err)
	}
	return s.buildVolume(pvc, volumeID), nil
}
