package kubernetes

import (
	"context"
	"encoding/base64"
	"sort"
	"strconv"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/database/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const defaultPageSize = 50

// List returns a paginated list of databases using Kubernetes Limit/Continue
// tokens as the AEP opaque page_token / next_page_token.
func (s *CnpgDatabaseStore) List(ctx context.Context, maxPageSize int32, pageToken string) (*v1alpha1.DatabaseList, error) {
	if maxPageSize <= 0 {
		maxPageSize = defaultPageSize
	}

	offset, err := decodePageToken(pageToken)
	if err != nil {
		return nil, err
	}

	clustersUnstructued, err := s.dynamicClient.Resource(clustersResource).Namespace(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: dcmSelector(),
	})

	if err != nil {
		return nil, err
	}

	var clusters []cnpgv1.Cluster
	for _, cluster := range clustersUnstructued.Items {
		// tmpCluster is declared inside of the loop in oreder for it to not override previous clusters
		var tmpCluster cnpgv1.Cluster
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(cluster.Object, &tmpCluster)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, tmpCluster)
	}

	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	total := len(clusters)
	offset = min(offset, total)
	paged := clusters[offset:]

	limit := min(int(maxPageSize), len(paged))
	paged = paged[:limit]

	databases := make([]v1alpha1.Database, 0, len(paged))
	for i := range paged {
		cluster := &paged[i]
		instanceID := cluster.Labels[dcm.LabelInstanceID]
		c, err := s.buildDatabase(ctx, cluster, instanceID)
		if err != nil {
			return nil, err
		}

		databases = append(databases, *c)
	}

	result := &v1alpha1.DatabaseList{
		Results: &databases,
	}
	if offset+limit < total {
		token := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset + limit)))
		result.NextPageToken = &token
	}

	return result, nil
}

// decodePageToken parses a base64-encoded page token into an offset.
func decodePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, &store.InvalidArgumentError{Message: "invalid page_token"}
	}

	offset, err := strconv.Atoi(string(decoded))
	if err != nil || offset < 0 {
		return 0, &store.InvalidArgumentError{Message: "invalid page_token"}
	}

	return offset, nil
}
