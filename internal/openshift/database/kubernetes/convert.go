package kubernetes

import (
	"fmt"
	"slices"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/machinery/pkg/api"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	"github.com/dcm-project/environment-agent/internal/openshift/database/units"
	"github.com/dcm-project/environment-agent/internal/ptr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	engine                             = "postgresql"
	postgresLatest                     = 18
	port                               = 5432 // As of it right now, it is not possible to reconfigure cnpg's port
	imageRepo                          = "ghcr.io/cloudnative-pg/postgresql"
	imageType                          = "standard"
	imageOs                            = "trixie"
	defaultUser                        = "app"
	customCredentialsSecretNamePattern = "%s-credentials"
	managedServiceNamePattern          = "%s-external-%s"
	usernameSecretKey                  = "username"
	passwordSecretKey                  = "password"
)

type databaseReplica struct {
	Pod *corev1.Pod
	Pvc *corev1.PersistentVolumeClaim
}

type databaseReplicaPhase struct {
	PodPhase corev1.PodPhase
	PvcPhase corev1.PersistentVolumeClaimPhase
}

// mapReplicaStatus maps a Kubernetes Pod phase and PVC phase to a database status.
// Returns the mapped status and true if mapping exists, or ("", false)
// for phases that should be ignored (e.g., Succeeded per DD-020).
func mapReplicaStatus(rpPhase databaseReplicaPhase) (v1alpha1.DatabaseStatus, bool) {
	switch rpPhase.PodPhase {
	case corev1.PodPending:
		if rpPhase.PvcPhase != corev1.ClaimLost {
			return v1alpha1.PENDING, true
		}
		return v1alpha1.FAILED, true
	case corev1.PodRunning:
		return v1alpha1.RUNNING, true
	case corev1.PodFailed:
		return v1alpha1.FAILED, true
	case corev1.PodUnknown:
		return v1alpha1.UNKNOWN, true
	default:
		// Succeeded and any unknown phases are not mapped
		return "", false
	}
}

// databaseFromCluster reconstructs an API database from a Kubernetes Cluster.
// It reverse-maps Cluster spec fields back to the API representation.
func databaseFromCluster(cluster *cnpgv1.Cluster, instanceID string) *v1alpha1.Database {
	major := int8(cluster.Status.PGDataImageInfo.MajorVersion)
	replicas := v1alpha1.DatabaseReplicas(cluster.Spec.Instances)
	spec := v1alpha1.DatabaseSpec{
		ServiceType: v1alpha1.DatabaseSpecServiceTypeDatabase,
		Engine:      engine,
		Version:     ptr.To(major),
		Replicas:    ptr.To(replicas),
		Metadata: v1alpha1.DatabaseMetadata{
			Name:      cluster.Name,
			Namespace: ptr.To(cluster.Namespace),
		},
	}

	// Reconstruct resources from K8s resource requirements.
	spec.Resources = resourcesFromCluster(*cluster)

	spec.Network = &v1alpha1.DatabaseNetwork{
		Port:       ptr.To(port),
		Visibility: visibilityFromCluster(*cluster),
	}

	// Reconstruct user labels by filtering out DCM reserved labels.
	if userLabels := userLabelsFromCluster(cluster); len(userLabels) > 0 {
		spec.Metadata.Labels = &userLabels
	}
	return &v1alpha1.Database{
		Id:         ptr.To(instanceID),
		Spec:       spec,
		Path:       ptr.To(fmt.Sprintf("databases/%s", instanceID)),
		CreateTime: ptr.To(cluster.CreationTimestamp.Time),
	}
}

// resourcesFromCluster extracts CPU and memory resources from a K8s container spec.
func resourcesFromCluster(cluster cnpgv1.Cluster) (res v1alpha1.DatabaseResources) {

	if req, ok := cluster.Spec.Resources.Requests[corev1.ResourceCPU]; ok {
		res.Cpu.Min = (req.String())
	}
	if lim, ok := cluster.Spec.Resources.Limits[corev1.ResourceCPU]; ok {
		res.Cpu.Max = (lim.String())
	}
	if req, ok := cluster.Spec.Resources.Requests[corev1.ResourceMemory]; ok {
		res.Memory.Min = units.MemoryQuantityToAPI(req)
	}
	if lim, ok := cluster.Spec.Resources.Limits[corev1.ResourceMemory]; ok {
		res.Memory.Max = units.MemoryQuantityToAPI(lim)
	}

	return res
}

// visibilityFromCluster checks for additional services configured for external visibility
// that are managed by the cluster object.
func visibilityFromCluster(cluster cnpgv1.Cluster) *v1alpha1.DatabaseNetworkVisibility {
	for _, service := range cluster.Spec.Managed.Services.Additional {
		if service.ServiceTemplate.Spec.Type == corev1.ServiceTypeLoadBalancer ||
			service.ServiceTemplate.Spec.Type == corev1.ServiceTypeNodePort {
			return ptr.To(v1alpha1.External)
		}
	}

	return ptr.To(v1alpha1.Internal)
}

// userLabelsFromCluster extracts user-defined labels by filtering out DCM reserved labels.
func userLabelsFromCluster(cluster *cnpgv1.Cluster) map[string]string {
	labels := make(map[string]string)
	for k, v := range cluster.Labels {
		if !dcm.ReservedLabelKeys[k] {
			labels[k] = v
		}
	}
	return labels
}

// enrichWithReplica populates runtime data from a Replica into the Database.
func enrichWithReplica(database *v1alpha1.Database, rp databaseReplica) {
	rpPhase := databaseReplicaPhase{
		PodPhase: rp.Pod.Status.Phase,
		PvcPhase: rp.Pvc.Status.Phase,
	}
	if status, ok := mapReplicaStatus(rpPhase); ok {
		database.Status = &status
	}

	if t := latestReplicaTransitionTime(rp); t != nil {
		database.UpdateTime = t
	}
}

// statusHigherPriority compares two statuses and consolidates into one status
// as described in https://github.com/dcm-project/enhancements/blob/main/enhancements/cloudnative-pg-database-sp/cloudnative-pg-sp.md#status-mapping-from-kubernetes-to-dcm
func statusHigherPriority(status1, status2 v1alpha1.DatabaseStatus) v1alpha1.DatabaseStatus {
	statuses := []v1alpha1.DatabaseStatus{
		v1alpha1.DELETED,
		v1alpha1.RUNNING,
		v1alpha1.PENDING,
		v1alpha1.UNKNOWN,
		v1alpha1.FAILED,
	}

	return statuses[max(slices.Index(statuses, status1), slices.Index(statuses, status2))]
}

// latestTime compares two time.Time objects and returns the latest of the two
func latestTime(t1, t2 *time.Time) time.Time {
	if t1.Compare(*t2) > 0 {
		return *t1
	}

	return *t2
}

// enrichWithReplicas populates runtime data from an array of Replicas into the Database.
func enrichWithReplicas(database *v1alpha1.Database, replicas []databaseReplica) {
	// No replica exists
	database.Status = ptr.To(v1alpha1.DELETED)
	database.UpdateTime = database.CreateTime

	for _, replica := range replicas {
		replicaPhase := databaseReplicaPhase{
			PodPhase: replica.Pod.Status.Phase,
			PvcPhase: replica.Pvc.Status.Phase,
		}
		status, ok := mapReplicaStatus(replicaPhase)
		if !ok {
			database.Status = ptr.To(v1alpha1.UNKNOWN)
		}
		database.Status = ptr.To(statusHigherPriority(*database.Status, status))

		if t := latestReplicaTransitionTime(replica); t != nil {
			database.UpdateTime = ptr.To(latestTime(database.UpdateTime, t))
		}
	}
}

// credentialSecretFromCluster infers the name of the secret containing
// the database credentials from the cluster resource
func credentialSecretFromCluster(cluster *cnpgv1.Cluster) string {
	if cluster.Spec.Bootstrap.InitDB.Secret.Name == "" {
		return fmt.Sprintf("%s-app", cluster.ObjectMeta.Name)
	}
	return cluster.Spec.Bootstrap.InitDB.Secret.Name
}

// serviceFromCluster infers the name of the service that is managing
// a persistant address for the cluster from the cluster resource
func serviceFromCluster(cluster *cnpgv1.Cluster) string {

	for _, svc := range cluster.Spec.Managed.Services.Additional {
		if svc.ServiceTemplate.Spec.Type == corev1.ServiceTypeLoadBalancer &&
			svc.SelectorType == cnpgv1.ServiceSelectorTypeRW {
			return svc.ServiceTemplate.ObjectMeta.Name
		}
	}

	return fmt.Sprintf("%s-rw", cluster.ObjectMeta.Name)
}

// databaseNameFromCluster infers the name of the database from the cluster resource
func databaseNameFromCluster(cluster *cnpgv1.Cluster) string {
	if cluster.Spec.Bootstrap.InitDB.Database == "" {
		return "app"
	}

	return cluster.Spec.Bootstrap.InitDB.Database
}

// resolveStorageClass resolves the storage class for specific db instance
func resolveStorageClass(providerHints *v1alpha1.ProviderHints, defaultClass string) string {
	if providerHints != nil && providerHints.Postgres != nil &&
		providerHints.Postgres.Storage != nil &&
		providerHints.Postgres.Storage.StorageClass != nil {
		return *providerHints.Postgres.Storage.StorageClass
	}
	return defaultClass
}

// databaseUserFromSpec resolves the database user name
func databaseUserFromBootstrap(bootstrap *cnpgv1.BootstrapConfiguration) string {
	if bootstrap == nil ||
		bootstrap.InitDB == nil ||
		bootstrap.InitDB.Owner == "" {
		return defaultUser
	}
	return bootstrap.InitDB.Owner
}

func databasePasswordFromProviderHints(providerHints *v1alpha1.ProviderHints) (string, bool) {
	if providerHints == nil || providerHints.Postgres == nil ||
		providerHints.Postgres.Initdb == nil ||
		providerHints.Postgres.Initdb.Password == nil {
		return "", false
	}
	return *providerHints.Postgres.Initdb.Password, true
}

// populateConnectionString()populates the connectionString field using values
// from the credentials secret and rw service of the cluster
func populateConnectionString(database *v1alpha1.Database, secret *corev1.Secret, svc *corev1.Service, dbName string) error {
	connectionStringTemplate := "postgresql://%s:%s@%s:%d/%s"

	username := "app"
	usernameByte, ok := secret.Data["username"]
	if ok {
		username = string(usernameByte)
	}

	var password string
	passwordByte, ok := secret.Data["password"]
	if !ok {
		return fmt.Errorf("Broken secret: Missing field - password") // should not be reachable
	}

	password = string(passwordByte)

	var address string
	if svc.Spec.ClusterIP != "" {
		address = svc.Spec.ClusterIP
	}

	if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		if len(svc.Status.LoadBalancer.Ingress) > 0 && svc.Status.LoadBalancer.Ingress[0].IP != "" {
			address = svc.Status.LoadBalancer.Ingress[0].IP
		}
	}

	database.Spec.ConnectionString = ptr.To(fmt.Sprintf(connectionStringTemplate, username, password, address, 5432, dbName))
	return nil
}

// latestReplicaTransitionTime returns the most recent LastTransitionTime from replica conditions.
func latestReplicaTransitionTime(rp databaseReplica) (latest *time.Time) {
	for _, condition := range rp.Pod.Status.Conditions {
		t := condition.LastTransitionTime.Time
		if !t.IsZero() && (latest == nil || t.After(*latest)) {
			latest = &t
		}
	}

	for _, condition := range rp.Pvc.Status.Conditions {
		t := condition.LastTransitionTime.Time
		if !t.IsZero() && (latest == nil || t.After(*latest)) {
			latest = &t
		}
	}
	return
}

func latestClusterTransitionTime(cluster cnpgv1.Cluster) (latest *time.Time) {
	for _, condition := range cluster.Status.Conditions {
		t := condition.LastTransitionTime.Time
		if !t.IsZero() && (latest == nil || t.After(*latest)) {
			latest = &t
		}
	}
	return
}

// buildCluster creates a CNPG Cluster from a Database spec.
func buildCluster(spec v1alpha1.DatabaseSpec, id string, cfg CnpgK8sConfig, labels map[string]string) (*cnpgv1.Cluster, error) {
	var replicas int
	if spec.Replicas == nil {
		replicas = 1
	} else {
		replicas = *spec.Replicas
	}

	// CPU resources
	cpuReq, cpuLim := units.ConvertCPU(spec.Resources.Cpu)

	// Memory resources — errors handled upstream; safe to ignore here since
	// validation occurs before buildCluster is called.
	memReq, _ := units.ConvertMemory(spec.Resources.Memory.Min)
	memLim, _ := units.ConvertMemory(spec.Resources.Memory.Max)

	// Storage resource - errors handled upstream
	storageSize, _ := resource.ParseQuantity(*spec.Resources.Storage)

	cluster := cnpgv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:   spec.Metadata.Name + "-" + id,
			Labels: labels,
		},
		Spec: cnpgv1.ClusterSpec{

			Instances: replicas,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    cpuReq,
					corev1.ResourceMemory: memReq,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    cpuLim,
					corev1.ResourceMemory: memLim,
				},
			},
			StorageConfiguration: cnpgv1.StorageConfiguration{
				Size:         storageSize.String(),
				StorageClass: ptr.To(resolveStorageClass(spec.ProviderHints, cfg.DefaultStorageClass)),
			},
		},
	}

	version := postgresLatest
	if spec.Version != nil {
		version = int(*spec.Version)
	}

	// ImageCatalog is only configureable at cluster level. if ImageCatalog is configured,
	// uses the image relevant to the major version, else, uses cnpg's standard image for
	// the specified major version with the debian (trixie) OS.
	if cfg.ImageCatalogName != "" {
		cluster.Spec.ImageCatalogRef = &cnpgv1.ImageCatalogRef{
			Major: version,
		}
		cluster.Spec.ImageCatalogRef.APIGroup = ptr.To("postgresql.cnpg.io")
		cluster.Spec.ImageCatalogRef.Kind = cnpgv1.ClusterImageCatalogKind
		cluster.Spec.ImageCatalogRef.Name = cfg.ImageCatalogName
	} else {
		cluster.Spec.ImageName = fmt.Sprintf("%s:%d-%s-%s", imageRepo, version, imageType, imageOs)
	}

	// Disable read only and read services
	cluster.Spec.Managed = &cnpgv1.ManagedConfiguration{
		Services: &cnpgv1.ManagedServices{
			DisabledDefaultServices: []cnpgv1.ServiceSelectorType{cnpgv1.ServiceSelectorTypeR, cnpgv1.ServiceSelectorTypeRO},
		},
	}

	// Currently, port configuration is not possible via cnpg, thus, the port will be ignored
	if spec.Network != nil && spec.Network.Visibility != nil &&
		*spec.Network.Visibility == v1alpha1.External {
		cluster.Spec.Managed.Services.Additional = append(cluster.Spec.Managed.Services.Additional,
			buildExternalManagedService(id, spec, corev1.ServiceType(cfg.ExternalServiceType)),
		)
	}

	if spec.ProviderHints == nil || spec.ProviderHints.Postgres == nil ||
		spec.ProviderHints.Postgres.Initdb == nil {
		return ptr.To(cluster), nil
	}

	initDB := &cnpgv1.BootstrapInitDB{}
	if spec.ProviderHints.Postgres.Initdb.Database != nil {
		initDB.Database = *spec.ProviderHints.Postgres.Initdb.Database
	}
	if spec.ProviderHints.Postgres.Initdb.User != nil {
		initDB.Owner = *spec.ProviderHints.Postgres.Initdb.User
	}
	if spec.ProviderHints.Postgres.Initdb.Password != nil {
		initDB.Secret = &api.LocalObjectReference{
			Name: fmt.Sprintf(customCredentialsSecretNamePattern, id),
		}
	}

	cluster.Spec.Bootstrap = &cnpgv1.BootstrapConfiguration{
		InitDB: initDB,
	}
	return &cluster, nil

}

// buildExternalManagedService creates a CNPG Managed Service to inject
// into a CNPG cluster spec.
func buildExternalManagedService(id string, spec v1alpha1.DatabaseSpec, svcType corev1.ServiceType) cnpgv1.ManagedService {
	return cnpgv1.ManagedService{
		SelectorType: cnpgv1.ServiceSelectorTypeRW,
		ServiceTemplate: cnpgv1.ServiceTemplateSpec{
			ObjectMeta: cnpgv1.Metadata{
				Name:   fmt.Sprintf(managedServiceNamePattern, spec.Metadata.Name, cnpgv1.ServiceSelectorTypeRW),
				Labels: dcm.Labels(id),
			},
			Spec: corev1.ServiceSpec{
				Type: svcType,
			},
		},
	}
}

// newCredentialsSecret creates a k8s secret for cluster credentials
func newCredentialsSecret(clusterName, username, password string, lables map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf(customCredentialsSecretNamePattern, clusterName),
			Labels: lables,
		},
		Type: corev1.SecretTypeBasicAuth,
		StringData: map[string]string{
			usernameSecretKey: username,
			passwordSecretKey: password,
		},
	}
}
