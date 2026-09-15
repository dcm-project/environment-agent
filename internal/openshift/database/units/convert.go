// Package units provides resource unit conversions between API and Kubernetes formats.
package units

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ConvertCPU converts a DatabaseCpu spec to Kubernetes resource quantities
// for requests and limits.
func ConvertCPU(cpu v1alpha1.DatabaseCpu) (requests, limits resource.Quantity) {
	requests = resource.MustParse(cpu.Min)
	limits = resource.MustParse(cpu.Max)
	return requests, limits
}

// apiToK8s maps API memory units to Kubernetes binary units.
var apiToK8s = map[string]string{
	"MB":  "Mi",
	"GB":  "Gi",
	"TB":  "Ti",
	"MiB": "Mi",
	"GiB": "Gi",
	"TiB": "Ti",
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
