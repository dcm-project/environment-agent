// Package units provides resource unit conversions between API and Kubernetes formats.
package units

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/dcm-project/environment-agent/api/container/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ConvertCPU converts CPU core or millicore strings (e.g. "2", "500m") to
// Kubernetes resource quantities for requests and limits.
func ConvertCPU(cpu v1alpha1.ContainerCpu) (requests, limits resource.Quantity, err error) {
	requests, err = resource.ParseQuantity(cpu.Min)
	if err != nil {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("invalid cpu.min %q: %w", cpu.Min, err)
	}
	limits, err = resource.ParseQuantity(cpu.Max)
	if err != nil {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("invalid cpu.max %q: %w", cpu.Max, err)
	}
	return requests, limits, nil
}

// CPUQuantityToAPI converts a Kubernetes resource.Quantity to a millicore
// string (e.g. "500m"). It always uses millicore format for consistency.
func CPUQuantityToAPI(q resource.Quantity) string {
	return fmt.Sprintf("%dm", q.MilliValue())
}

// apiToK8s maps API memory units to Kubernetes binary units.
var apiToK8s = map[string]string{
	"MB": "Mi",
	"GB": "Gi",
	"TB": "Ti",
}

// k8sToAPI maps Kubernetes binary memory suffixes back to API units.
var k8sToAPI = map[string]string{
	"Mi": "MB",
	"Gi": "GB",
	"Ti": "TB",
}

// ConvertMemory converts a memory string (e.g., "1GB") to a Kubernetes
// resource quantity.
func ConvertMemory(memoryStr string) (resource.Quantity, error) {
	for suffix, k8sUnit := range apiToK8s {
		if numStr, ok := strings.CutSuffix(memoryStr, suffix); ok {
			return resource.ParseQuantity(numStr + k8sUnit)
		}
	}
	return resource.Quantity{}, fmt.Errorf("unsupported memory unit in %q", memoryStr)
}

// MemoryQuantityToAPI converts a Kubernetes resource.Quantity to an API memory
// string (e.g., "1Gi" → "1GB"). Falls back to the raw quantity string if the
// suffix is not recognized.
func MemoryQuantityToAPI(q resource.Quantity) string {
	s := q.String()
	for k8sSuffix, apiUnit := range k8sToAPI {
		if numStr, ok := strings.CutSuffix(s, k8sSuffix); ok {
			return numStr + apiUnit
		}
	}
	return s
}
