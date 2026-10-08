package kubernetes

import (
	"context"
	"fmt"
	"time"

	"github.com/dcm-project/environment-agent/internal/ptr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Delete removes a database and its associated Kubernetes resources.
func (s *CnpgDatabaseStore) Delete(ctx context.Context, databaseId string) error {
	cluster, err := s.findCluster(ctx, databaseId)
	if err != nil {
		return err
	}

	deleteOptions := metav1.DeleteOptions{PropagationPolicy: ptr.To(metav1.DeletePropagationBackground)}

	// Delete the custom credentials secret (dependent resource, ignore if none found).
	secret, err := s.client.CoreV1().Secrets(s.cfg.Namespace).Get(ctx, fmt.Sprintf(customCredentialsSecretNamePattern, cluster.Name), metav1.GetOptions{})

	if err != nil {
		// Ignore not found error
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else {
		if delErr := s.client.CoreV1().Secrets(s.cfg.Namespace).Delete(ctx, secret.Name, deleteOptions); delErr != nil {
			return delErr
		}
	}

	// Delete Cluster (primary resource)
	// Use WithoutCancel so the Cluster deletion completes even if the
	// client disconnects after the Service is gone
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return s.dynamicClient.Resource(clustersResource).Namespace(s.cfg.Namespace).Delete(cleanupCtx, cluster.Name, deleteOptions)
}
