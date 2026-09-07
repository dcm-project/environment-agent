package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dcm-project/environment-agent/api/network/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/network/store"
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

// List returns a paginated list of networks using Kubernetes Limit/Continue
// tokens as the AEP opaque page_token / next_page_token.
func (s *K8sNetworkStore) List(ctx context.Context, maxPageSize int32, pageToken string) (*v1alpha1.NetworkList, error) {
	maxPageSize = normalizePageSize(maxPageSize)

	services, err := s.client.CoreV1().Services(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: dcmSelector(),
		Limit:         int64(maxPageSize),
		Continue:      pageToken,
	})
	if err != nil {
		return nil, mapListError(err)
	}

	networks := make([]v1alpha1.Network, 0, len(services.Items))
	for i := range services.Items {
		svc := &services.Items[i]
		instanceID := svc.Labels[dcm.LabelInstanceID]
		// Skip services without instance ID or that aren't DCM-managed
		if instanceID == "" || !isDCMManagedService(svc, instanceID) {
			continue
		}
		networks = append(networks, serviceToNetwork(svc, instanceID))
	}

	result := &v1alpha1.NetworkList{
		Results: &networks,
	}
	if services.Continue != "" {
		token := services.Continue
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
				return &store.InvalidArgumentError{Field: "page_token", Message: "invalid page_token"}
			}
			if s.Message != "" {
				return &store.InvalidArgumentError{Message: s.Message}
			}
			return &store.InvalidArgumentError{Message: "invalid argument"}
		}
	}
	return fmt.Errorf("listing services: %w", err)
}
