package registration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/dcm-project/environment-agent/internal/acmcluster/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CompatibilityMatrix maps OCP minor versions to K8s minor versions.
type CompatibilityMatrix map[string]string

// DefaultCompatibilityMatrix contains the OCP 4.x = K8s 1.(x+13) mappings.
var DefaultCompatibilityMatrix = CompatibilityMatrix{
	"4.14": "1.27",
	"4.15": "1.28",
	"4.16": "1.29",
	"4.17": "1.30",
	"4.18": "1.31",
	"4.19": "1.32",
	"4.20": "1.33",
	"4.21": "1.34",
}

// VersionDiscoverer queries ClusterImageSets from K8s and maps OCP versions
// to K8s minor versions using the internal compatibility matrix.
type VersionDiscoverer struct {
	k8sClient client.Client
	matrix    CompatibilityMatrix
}

// NewVersionDiscoverer creates a VersionDiscoverer with the given compatibility matrix.
func NewVersionDiscoverer(k8sClient client.Client, matrix CompatibilityMatrix) *VersionDiscoverer {
	return &VersionDiscoverer{
		k8sClient: k8sClient,
		matrix:    matrix,
	}
}

// LoadCompatibilityMatrix loads a compatibility matrix from a JSON file.
// If path is empty, returns the DefaultCompatibilityMatrix.
func LoadCompatibilityMatrix(path string) (CompatibilityMatrix, error) {
	if path == "" {
		return DefaultCompatibilityMatrix, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("loading compatibility matrix from %s: %w", path, err)
	}
	var matrix CompatibilityMatrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		return nil, fmt.Errorf("parsing compatibility matrix from %s: %w", path, err)
	}
	return matrix, nil
}

// ExtractOCPVersion extracts the OCP major.minor from a release image reference.
// Example: "quay.io/openshift-release-dev/ocp-release:4.17.3-multi" -> "4.17"
func ExtractOCPVersion(releaseImage string) string {
	colonIdx := strings.LastIndex(releaseImage, ":")
	if colonIdx < 0 {
		return ""
	}
	tag := releaseImage[colonIdx+1:]
	parts := strings.SplitN(tag, ".", 3)
	if len(parts) < 2 {
		return ""
	}
	return parts[0] + "." + parts[1]
}

// DiscoverVersions queries ClusterImageSet resources, extracts OCP versions,
// filters through the compatibility matrix, and returns sorted K8s minor versions.
func (v *VersionDiscoverer) DiscoverVersions(ctx context.Context) ([]string, error) {
	cisList := &unstructured.UnstructuredList{}
	cisList.SetGroupVersionKind(util.ClusterImageSetListGVK)

	if err := v.k8sClient.List(ctx, cisList); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	for _, item := range cisList.Items {
		releaseImage, _, _ := unstructured.NestedString(item.Object, "spec", "releaseImage")
		if releaseImage == "" {
			continue
		}
		ocpVersion := ExtractOCPVersion(releaseImage)
		if k8sVersion, ok := v.matrix[ocpVersion]; ok {
			seen[k8sVersion] = struct{}{}
		}
	}

	versions := make([]string, 0, len(seen))
	for v := range seen {
		versions = append(versions, v)
	}
	sort.Strings(versions)
	return versions, nil
}
