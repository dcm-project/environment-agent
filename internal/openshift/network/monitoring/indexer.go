package monitoring

import (
	"github.com/dcm-project/environment-agent/internal/openshift/network/dcm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ExtractInstanceID returns the dcm-instance-id label value from a
// resource's metadata.
func ExtractInstanceID(obj metav1.Object) string {
	labels := obj.GetLabels()
	return labels[dcm.LabelInstanceID]
}
