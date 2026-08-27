package kubernetes

// K8sConfig holds configuration for the Kubernetes volume store.
type K8sConfig struct {
	Namespace           string
	DefaultStorageClass string
	DefaultAccessMode   string
}
