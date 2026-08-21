package cluster

import (
	"context"

	"github.com/dcm-project/environment-agent/internal/acmcluster/registration"
	"github.com/dcm-project/environment-agent/internal/acmcluster/service"
	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// VersionResolver resolves a K8s minor version to a release image
// by looking up ClusterImageSets via the compatibility matrix.
type VersionResolver struct {
	client client.Client
	matrix registration.CompatibilityMatrix
}

// NewVersionResolver creates a VersionResolver with the given compatibility matrix.
func NewVersionResolver(c client.Client, matrix registration.CompatibilityMatrix) *VersionResolver {
	return &VersionResolver{
		client: c,
		matrix: matrix,
	}
}

// Resolve translates a K8s minor version (e.g. "1.30") to a release image
// by reverse-looking up the OCP version and finding a matching ClusterImageSet.
func (r *VersionResolver) Resolve(ctx context.Context, k8sVersion string) (string, error) {
	ocpVersion, err := r.k8sToOCP(k8sVersion)
	if err != nil {
		return "", err
	}

	cisList := &unstructured.UnstructuredList{}
	cisList.SetGroupVersionKind(util.ClusterImageSetListGVK)

	if err := r.client.List(ctx, cisList); err != nil {
		return "", service.NewInternalError("failed to list cluster image sets", err)
	}

	for _, item := range cisList.Items {
		releaseImage, _, _ := unstructured.NestedString(item.Object, "spec", "releaseImage")
		if releaseImage == "" {
			continue
		}
		if registration.ExtractOCPVersion(releaseImage) == ocpVersion {
			return releaseImage, nil
		}
	}

	return "", service.NewUnprocessableEntityError("no ClusterImageSet found for version " + k8sVersion)
}

// k8sToOCP reverse-looks up a K8s minor version in the compatibility matrix
// to find the corresponding OCP version.
func (r *VersionResolver) k8sToOCP(k8sVersion string) (string, error) {
	for ocpVersion, k8sVer := range r.matrix {
		if k8sVer == k8sVersion {
			return ocpVersion, nil
		}
	}
	return "", service.NewUnprocessableEntityError("unsupported Kubernetes version: " + k8sVersion)
}

// ReleaseImageToK8sVersion reverse-maps a release image URL to a K8s minor version
// using the compatibility matrix. Returns empty string if the image cannot be mapped.
func ReleaseImageToK8sVersion(releaseImage string, matrix registration.CompatibilityMatrix) string {
	ocpMinor := registration.ExtractOCPVersion(releaseImage)
	if ocpMinor == "" {
		return ""
	}
	if k8sVer, ok := matrix[ocpMinor]; ok {
		return k8sVer
	}
	return ""
}
