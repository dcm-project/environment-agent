package kubernetes

import (
	"context"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
)

// Get retrieves a cluster by its instance ID, enriching it with runtime
// data from Pods, Services, and secrets.
func (s *CnpgDatabaseStore) Get(ctx context.Context, containerID string) (*v1alpha1.Database, error) {
	deploy, err := s.findCluster(ctx, containerID)
	if err != nil {
		return nil, err
	}
	return s.buildDatabase(ctx, deploy, containerID)
}
