package kubernetes

import (
	"fmt"
	"time"

	v1alpha1 "github.com/dcm-project/environment-agent/api/storage/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/storage/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MapPVCToStatus maps a Kubernetes PVC to a DCM volume status and message.
func MapPVCToStatus(pvc *corev1.PersistentVolumeClaim) (v1alpha1.StorageStatus, string) {
	if pvc == nil {
		return v1alpha1.DELETED, "resource no longer exists"
	}

	if pvc.DeletionTimestamp != nil {
		return v1alpha1.DELETING, "PVC is terminating"
	}

	switch pvc.Status.Phase {
	case corev1.ClaimPending:
		return v1alpha1.PROVISIONING, "PVC is pending binding"
	case corev1.ClaimLost:
		return v1alpha1.FAILED, "PVC is lost"
	case corev1.ClaimBound:
		if resizing, msg := pvcResizeStatus(pvc); resizing {
			return v1alpha1.PROVISIONING, msg
		}
		msg := "PVC is bound"
		if pvc.Spec.VolumeName != "" {
			msg = fmt.Sprintf("PVC is bound to volume %s", pvc.Spec.VolumeName)
		}
		return v1alpha1.RUNNING, msg
	default:
		return v1alpha1.PROVISIONING, string(pvc.Status.Phase)
	}
}

func pvcResizeStatus(pvc *corev1.PersistentVolumeClaim) (bool, string) {
	for _, c := range pvc.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case corev1.PersistentVolumeClaimResizing:
			return true, "volume expansion in progress"
		case corev1.PersistentVolumeClaimFileSystemResizePending:
			return true, "filesystem resize pending"
		}
	}
	return false, ""
}

func volumeFromPVC(pvc *corev1.PersistentVolumeClaim, instanceID string) v1alpha1.Volume {
	path := fmt.Sprintf("volumes/%s", instanceID)
	ns := pvc.Namespace
	createTime := pvc.CreationTimestamp.Time
	updateTime := createTime
	if t := latestPVCConditionTime(pvc); t != nil {
		updateTime = *t
	}

	capacity := pvc.Spec.Resources.Requests.Storage().String()
	serviceType := v1alpha1.Storage

	spec := v1alpha1.StorageSpec{
		ServiceType: serviceType,
		Capacity:    capacity,
		Metadata: v1alpha1.VolumeMetadata{
			Name:      pvc.Name,
			Namespace: &ns,
		},
	}
	if sc := storageClassFromPVC(pvc); sc != "" {
		spec.Metadata.StorageClass = &sc
	}
	if pvc.Spec.VolumeName != "" {
		vn := pvc.Spec.VolumeName
		spec.Metadata.VolumeName = &vn
	}
	if hints := providerHintsFromPVC(pvc); hints != nil {
		spec.ProviderHints = hints
	}
	if userLabels := userLabelsFromPVC(pvc); len(userLabels) > 0 {
		spec.Metadata.Labels = &userLabels
	}

	status, _ := MapPVCToStatus(pvc)
	id := instanceID

	return v1alpha1.Volume{
		Id:         &id,
		Path:       &path,
		CreateTime: &createTime,
		UpdateTime: &updateTime,
		Spec:       spec,
		Status:     &status,
	}
}

func buildPVC(spec v1alpha1.StorageSpec, cfg K8sConfig, labels map[string]string) (*corev1.PersistentVolumeClaim, error) {
	qty, err := resource.ParseQuantity(spec.Capacity)
	if err != nil {
		return nil, &store.InvalidArgumentError{Message: fmt.Sprintf("invalid capacity %q: %v", spec.Capacity, err)}
	}

	accessMode, err := resolveAccessMode(spec, cfg.DefaultAccessMode)
	if err != nil {
		return nil, err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Metadata.Name,
			Namespace: cfg.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{accessMode},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: qty,
				},
			},
		},
	}

	storageClass := resolveStorageClass(spec, cfg.DefaultStorageClass)
	if storageClass != "" {
		pvc.Spec.StorageClassName = &storageClass
	}

	if spec.ProviderHints != nil && spec.ProviderHints.Kubernetes != nil {
		if vm := spec.ProviderHints.Kubernetes.VolumeMode; vm != nil {
			mode, err := resolveVolumeMode(*vm)
			if err != nil {
				return nil, err
			}
			pvc.Spec.VolumeMode = &mode
		}
	}

	return pvc, nil
}

func resolveVolumeMode(vm v1alpha1.VolumeMode) (corev1.PersistentVolumeMode, error) {
	mode := corev1.PersistentVolumeMode(vm)
	switch mode {
	case corev1.PersistentVolumeFilesystem, corev1.PersistentVolumeBlock:
		return mode, nil
	default:
		return "", &store.InvalidArgumentError{Message: fmt.Sprintf("unsupported volume mode %q", vm)}
	}
}

func resolveAccessMode(spec v1alpha1.StorageSpec, defaultMode string) (corev1.PersistentVolumeAccessMode, error) {
	if spec.ProviderHints != nil && spec.ProviderHints.Kubernetes != nil &&
		spec.ProviderHints.Kubernetes.AccessMode != nil {
		mode := corev1.PersistentVolumeAccessMode(*spec.ProviderHints.Kubernetes.AccessMode)
		switch mode {
		case corev1.ReadWriteOnce, corev1.ReadOnlyMany, corev1.ReadWriteMany:
			return mode, nil
		default:
			return "", &store.InvalidArgumentError{Message: fmt.Sprintf("unsupported access mode %q", mode)}
		}
	}
	if defaultMode != "" {
		mode := corev1.PersistentVolumeAccessMode(defaultMode)
		switch mode {
		case corev1.ReadWriteOnce, corev1.ReadOnlyMany, corev1.ReadWriteMany:
			return mode, nil
		default:
			return "", &store.InvalidArgumentError{Message: fmt.Sprintf("unsupported default access mode %q", defaultMode)}
		}
	}
	return corev1.ReadWriteOnce, nil
}

func resolveStorageClass(spec v1alpha1.StorageSpec, defaultClass string) string {
	if spec.ProviderHints != nil && spec.ProviderHints.Kubernetes != nil &&
		spec.ProviderHints.Kubernetes.StorageClass != nil {
		return *spec.ProviderHints.Kubernetes.StorageClass
	}
	return defaultClass
}

func storageClassFromPVC(pvc *corev1.PersistentVolumeClaim) string {
	if pvc.Spec.StorageClassName != nil {
		return *pvc.Spec.StorageClassName
	}
	return ""
}

func providerHintsFromPVC(pvc *corev1.PersistentVolumeClaim) *v1alpha1.ProviderHints {
	var k8sHints v1alpha1.KubernetesProviderHints
	hasHints := false

	if sc := storageClassFromPVC(pvc); sc != "" {
		k8sHints.StorageClass = &sc
		hasHints = true
	}
	if pvc.Spec.VolumeMode != nil {
		vm := v1alpha1.VolumeMode(*pvc.Spec.VolumeMode)
		k8sHints.VolumeMode = &vm
		hasHints = true
	}
	if len(pvc.Spec.AccessModes) > 0 {
		am := v1alpha1.VolumeAccessMode(pvc.Spec.AccessModes[0])
		k8sHints.AccessMode = &am
		hasHints = true
	}
	if !hasHints {
		return nil
	}
	return &v1alpha1.ProviderHints{Kubernetes: &k8sHints}
}

func userLabelsFromPVC(pvc *corev1.PersistentVolumeClaim) map[string]string {
	labels := make(map[string]string)
	for k, v := range pvc.Labels {
		if !dcm.ReservedLabelKeys[k] {
			labels[k] = v
		}
	}
	return labels
}

func latestPVCConditionTime(pvc *corev1.PersistentVolumeClaim) *time.Time {
	var latest *time.Time
	for i := range pvc.Status.Conditions {
		t := pvc.Status.Conditions[i].LastTransitionTime.Time
		if t.IsZero() {
			continue
		}
		if latest == nil || t.After(*latest) {
			latest = &t
		}
	}
	return latest
}
