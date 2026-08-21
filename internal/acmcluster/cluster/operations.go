package cluster

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"

	v1alpha1 "github.com/dcm-project/environment-agent/api/acm/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/acmcluster/config"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// findByInstanceID lists HostedClusters matching the dcm-instance-id label
// in the given namespace. Returns the first match or a NotFoundError if none exist.
func findByInstanceID(ctx context.Context, c client.Client, namespace, id string) (*hyperv1.HostedCluster, error) {
	var hcList hyperv1.HostedClusterList
	if err := c.List(ctx, &hcList,
		client.InNamespace(namespace),
		client.MatchingLabels{LabelInstanceID: id},
	); err != nil {
		return nil, service.NewInternalError("failed to list clusters", err)
	}

	if len(hcList.Items) == 0 {
		return nil, service.NewNotFoundError(fmt.Sprintf("cluster %s not found", id))
	}

	return &hcList.Items[0], nil
}

// deleteNodePools deletes all NodePools matching the given instance ID
// in the specified namespace.
func deleteNodePools(ctx context.Context, c client.Client, namespace, instanceID string) error {
	var npList hyperv1.NodePoolList
	if err := c.List(ctx, &npList,
		client.InNamespace(namespace),
		client.MatchingLabels{LabelInstanceID: instanceID},
	); err != nil {
		return service.NewInternalError("failed to list node pools for deletion", err)
	}
	for i := range npList.Items {
		if err := c.Delete(ctx, &npList.Items[i]); err != nil {
			if !k8serrors.IsNotFound(err) {
				return service.NewInternalError("failed to delete node pool", err)
			}
		}
	}
	return nil
}

// GetCluster retrieves a cluster by instance ID.
func GetCluster(ctx context.Context, c client.Client, cfg config.ClusterConfig, id string) (*v1alpha1.Cluster, error) {
	hc, err := findByInstanceID(ctx, c, cfg.ClusterNamespace, id)
	if err != nil {
		return nil, err
	}

	result := HostedClusterToCluster(ctx, c, cfg, hc)
	return &result, nil
}

// ListClusters returns a paginated list of all managed clusters in the configured namespace.
func ListClusters(ctx context.Context, c client.Client, cfg config.ClusterConfig, pageSize int, pageToken string) (*v1alpha1.ClusterList, error) {
	var hcList hyperv1.HostedClusterList
	if err := c.List(ctx, &hcList,
		client.InNamespace(cfg.ClusterNamespace),
		client.MatchingLabels{
			LabelManagedBy:   ValueManagedBy,
			LabelServiceType: ValueServiceType,
		},
	); err != nil {
		return nil, service.NewInternalError("failed to list clusters", err)
	}

	sort.Slice(hcList.Items, func(i, j int) bool {
		return hcList.Items[i].Name < hcList.Items[j].Name
	})

	offset := 0
	if pageToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(pageToken)
		if err != nil {
			return nil, service.NewInvalidArgumentError("invalid page_token")
		}
		v, err := strconv.Atoi(string(decoded))
		if err != nil || v < 0 {
			return nil, service.NewInvalidArgumentError("invalid page_token")
		}
		offset = v
	}

	total := len(hcList.Items)
	if offset > total {
		offset = total
	}
	end := offset + pageSize
	if end > total {
		end = total
	}
	page := hcList.Items[offset:end]

	clusters := make([]v1alpha1.Cluster, 0, len(page))
	for i := range page {
		clusters = append(clusters, HostedClusterToCluster(ctx, c, cfg, &page[i]))
	}

	result := &v1alpha1.ClusterList{
		Clusters: &clusters,
	}

	if end < total {
		nextToken := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d", end)))
		result.NextPageToken = &nextToken
	}

	return result, nil
}

// DeleteCluster deletes a cluster and its node pools by instance ID.
func DeleteCluster(ctx context.Context, c client.Client, cfg config.ClusterConfig, id string) error {
	hc, err := findByInstanceID(ctx, c, cfg.ClusterNamespace, id)
	if err != nil {
		return err
	}

	if err := deleteNodePools(ctx, c, hc.Namespace, id); err != nil {
		return err
	}

	if err := c.Delete(ctx, hc); err != nil {
		return service.NewInternalError("failed to delete cluster", err)
	}

	return nil
}
