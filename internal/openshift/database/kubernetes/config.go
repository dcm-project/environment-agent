package kubernetes

// CnpgK8sConfig holds configuration for the Kubernetes database store.
type CnpgK8sConfig struct {
	Namespace           string
	ExternalServiceType string
	ImageCatalogName    string
	DefaultStorageClass string
}
