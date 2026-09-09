package kubernetes

import (
	"encoding/json"
	"fmt"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
)

type k8sProviderHints struct {
	StorageClass *string `json:"storage_class,omitempty"`
	VolumeMode   *string `json:"volume_mode,omitempty"`
	AccessMode   *string `json:"access_mode,omitempty"`
}

func (h k8sProviderHints) empty() bool {
	return h.StorageClass == nil && h.VolumeMode == nil && h.AccessMode == nil
}

func k8sHintsFromSpec(spec v1alpha1.StorageSpec) (*k8sProviderHints, error) {
	if spec.ProviderHints == nil {
		return nil, nil
	}
	raw, found := (*spec.ProviderHints)["kubernetes"]
	if !found {
		return nil, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshaling kubernetes provider hints: %w", err)
	}
	var hints k8sProviderHints
	if err := json.Unmarshal(data, &hints); err != nil {
		return nil, &store.InvalidArgumentError{Message: fmt.Sprintf("invalid kubernetes provider hints: %v", err)}
	}
	if hints.empty() {
		return nil, nil
	}
	return &hints, nil
}

func providerHintsFromK8s(hints k8sProviderHints) *v1alpha1.ProviderHints {
	if hints.empty() {
		return nil
	}
	ph := v1alpha1.ProviderHints{"kubernetes": hints}
	return &ph
}
