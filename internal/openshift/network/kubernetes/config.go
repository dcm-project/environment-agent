package kubernetes

// K8sConfig holds configuration for the Kubernetes network store.
type K8sConfig struct {
	// Namespace is the namespace where network services are managed.
	Namespace string
}
