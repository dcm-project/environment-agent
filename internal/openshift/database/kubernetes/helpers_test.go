package kubernetes_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1alpha1 "github.com/dcm-project/environment-agent/api/database/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/openshift/database/dcm"
	cnpgstore "github.com/dcm-project/environment-agent/internal/openshift/database/kubernetes"
	"github.com/dcm-project/environment-agent/internal/ptr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	imageRepo           = "ghcr.io/cloudnative-pg/postgresql"
	imageType           = "standard"
	imageOs             = "trixie"
	testInternalAddress = "10.96.0.1"
	testExternalAddress = "10.0.0.1"
	testPassword        = "abcd1234"
	testNS              = "default"
	clusterName         = "my-app"
	testId              = "abc-123"
	testDB              = "data"
	testUser            = "application"
)

// the clustersGvr object is used to map crds to their different names and list kinds
var clustersGvr = schema.GroupVersionResource{
	Group:    "postgresql.cnpg.io",
	Version:  "v1",
	Resource: "clusters",
}

func defaultConfig() cnpgstore.CnpgK8sConfig {
	return cnpgstore.CnpgK8sConfig{
		Namespace:           testNS,
		ExternalServiceType: string(corev1.ServiceTypeLoadBalancer),
	}
}

func newTestStore(cfg cnpgstore.CnpgK8sConfig) (*cnpgstore.CnpgDatabaseStore, kubernetes.Interface, dynamic.Interface) {
	client := fake.NewClientset()

	// the gvk object registers a specific "Kind", in this case, the single cluster resource
	gvk := schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "Cluster",
	}

	// this gvk object registers a list of cluster resources as a single resource to enable
	// usage of the "List" function
	gvkList := schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "ClusterList",
	}

	scheme := runtime.NewScheme()

	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvkList, &unstructured.UnstructuredList{})
	metav1.AddToGroupVersion(scheme, clustersGvr.GroupVersion())

	crds := map[schema.GroupVersionResource]string{
		clustersGvr: "ClusterList",
	}
	dynClient := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, crds)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return cnpgstore.NewCnpgDatabaseStore(client, dynClient, cfg, logger), client, dynClient
}

type databaseOpt func(*v1alpha1.DatabaseSpec)

func dbWithPassword(password string) databaseOpt {
	return func(spec *v1alpha1.DatabaseSpec) {
		spec.ProviderHints.Postgres.Initdb.Password = ptr.To(password)
	}
}

func minimalDatabase(name string, opts ...databaseOpt) v1alpha1.DatabaseSpec {
	spec := &v1alpha1.DatabaseSpec{
		ServiceType: v1alpha1.DatabaseSpecServiceTypeDatabase,
		Engine:      "postgresql",
		Metadata: v1alpha1.DatabaseMetadata{
			Name: name,
		},
		Resources: v1alpha1.DatabaseResources{
			Cpu: v1alpha1.DatabaseCpu{
				Min: "500m",
				Max: "2",
			},
			Memory: v1alpha1.DatabaseMemory{
				Min: "512MB",
				Max: "2GB",
			},
			Storage: ptr.To("10GB"),
		},
		Network: &v1alpha1.DatabaseNetwork{},
		ProviderHints: &v1alpha1.ProviderHints{
			Postgres: &v1alpha1.PostgresProviderHints{
				Initdb: &v1alpha1.InitdbProviderHints{},
			},
		},
	}
	for _, opt := range opts {
		opt(spec)
	}

	return *spec
}

func databaseWithExternalService(name string) v1alpha1.DatabaseSpec {
	return v1alpha1.DatabaseSpec{
		ServiceType: v1alpha1.DatabaseSpecServiceTypeDatabase,
		Engine:      "postgresql",
		Metadata: v1alpha1.DatabaseMetadata{
			Name: name,
		},
		Resources: v1alpha1.DatabaseResources{
			Cpu: v1alpha1.DatabaseCpu{
				Min: "500m",
				Max: "2",
			},
			Memory: v1alpha1.DatabaseMemory{
				Min: "512MB",
				Max: "2GB",
			},
			Storage: ptr.To("10GB"),
		},
		Network: &v1alpha1.DatabaseNetwork{
			Visibility: ptr.To(v1alpha1.External),
		},
	}

}

type fakeClusterOption func(*cnpgv1.Cluster) error

func withUser(username string) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		cluster.Spec.Bootstrap.InitDB.Owner = username
		return nil
	}
}

func withCredentialsSecret(secret string) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		cluster.Spec.Bootstrap.InitDB.Secret.Name = secret
		return nil
	}
}

func withDatabaseName(db string) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		cluster.Spec.Bootstrap.InitDB.Database = db
		return nil
	}
}

func withExternalVisibility(client *kubernetes.Interface, instanceId string) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		cluster.Spec.Managed.Services.Additional =
			append(cluster.Spec.Managed.Services.Additional, cnpgv1.ManagedService{
				SelectorType: cnpgv1.ServiceSelectorTypeRW,
				ServiceTemplate: cnpgv1.ServiceTemplateSpec{
					ObjectMeta: cnpgv1.Metadata{
						Name:   cluster.Name + "-external-rw",
						Labels: dcm.Labels(instanceId),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeLoadBalancer,
					},
				},
			})

		return createFakeRWService(client, cluster.Namespace, cluster.Name, instanceId, corev1.ServiceTypeLoadBalancer, withLoadBalancerIP(testExternalAddress))

	}
}

func withClusterConditions(conditions []metav1.Condition) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error { cluster.Status.Conditions = conditions; return nil }
}

func withMajorVersion(version int) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		image := fmt.Sprintf("%s:%d-%s-%s", imageRepo, version, imageType, imageOs)
		cluster.Spec.ImageName = image
		cluster.Status.PGDataImageInfo = &cnpgv1.ImageInfo{
			MajorVersion: version,
			Image:        image,
		}
		return nil
	}
}

func withArtifacts(client *kubernetes.Interface, instanceId string) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		if len(cluster.Spec.Bootstrap.InitDB.Secret.Name) == 0 {
			err := createFakeSecret(client, cluster.ObjectMeta.Namespace, cluster.ObjectMeta.Name+"-app", instanceId, withUsername("app"), withPassword(testPassword))
			if err != nil {
				return err
			}
		}

		err := createFakeRWService(client, cluster.ObjectMeta.Namespace, cluster.Name, instanceId, corev1.ServiceTypeClusterIP, withClusterIP(testInternalAddress))
		return err
	}
}

func withReplicas(client *kubernetes.Interface, storageclass, instanceId string, podOpts ...fakePodOption) fakeClusterOption {
	return func(cluster *cnpgv1.Cluster) error {
		var err error
		for i := 0; i < cluster.Spec.Instances; i++ {
			err = createFakePod(client, cluster.Namespace, fmt.Sprintf("%s-%d", cluster.Name, i+1), instanceId, corev1.PodRunning, podOpts...)
			if err != nil {
				return err
			}

			err = createFakePvc(client, cluster.Namespace, fmt.Sprintf("%s-%d", cluster.Name, i+1), instanceId, storageclass)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func createFakeCluster(client *dynamic.Interface, namespace, name, instanceID string, opts ...fakeClusterOption) error {
	labels := dcm.Labels(instanceID)
	replicas := 1
	cluster := &cnpgv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: cnpgv1.ClusterSpec{
			Instances: replicas,
			Bootstrap: &cnpgv1.BootstrapConfiguration{
				InitDB: &cnpgv1.BootstrapInitDB{
					Secret: &cnpgv1.LocalObjectReference{},
				},
			},
			Managed: &cnpgv1.ManagedConfiguration{
				Services: &cnpgv1.ManagedServices{},
			},
		},
		Status: cnpgv1.ClusterStatus{
			PGDataImageInfo: &cnpgv1.ImageInfo{},
		},
	}

	for _, opt := range opts {
		err := opt(cluster)
		if err != nil {
			return err
		}
	}
	clusterMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
	if err != nil {
		return err
	}
	unstructuredCluster := unstructured.Unstructured{Object: clusterMap}
	_, err = (*client).Resource(cnpgv1.SchemeGroupVersion.WithResource("clusters")).Namespace("default").Create(context.Background(), &unstructuredCluster, metav1.CreateOptions{})
	return err
}

func getCreatedCluster(client *dynamic.Interface, namespace string) (*cnpgv1.Cluster, error) {
	list, err := (*client).Resource(cnpgv1.SchemeGroupVersion.WithResource("clusters")).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no deployments found in namespace %s", namespace)
	}

	var cluster cnpgv1.Cluster
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[0].Object, &cluster)

	return &cluster, err
}

type fakeSecretOption func(*corev1.Secret)

func withUsername(username string) fakeSecretOption {
	return func(secret *corev1.Secret) { secret.Data["username"] = []byte(username) }
}

func withPassword(password string) fakeSecretOption {
	return func(secret *corev1.Secret) { secret.Data["password"] = []byte(password) }
}

func createFakeSecret(client *kubernetes.Interface, namespace, name, instanceID string, opts ...fakeSecretOption) error {
	labels := dcm.Labels(instanceID)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{},
	}
	for _, opt := range opts {
		opt(secret)
	}
	_, err := (*client).CoreV1().Secrets(namespace).Create(context.Background(), secret, metav1.CreateOptions{})
	return err
}

type fakePodOption func(*corev1.Pod)

func withPodConditions(conditions []corev1.PodCondition) fakePodOption {
	return func(p *corev1.Pod) { p.Status.Conditions = conditions }
}

func createFakePod(client *kubernetes.Interface, namespace, name, instanceId string, phase corev1.PodPhase, opts ...fakePodOption) error {
	labels := dcm.Labels(instanceId)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}

	for _, opt := range opts {
		opt(pod)
	}
	_, err := (*client).CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	return err
}

type fakeServiceOption func(*corev1.Service)

func withClusterIP(ip string) fakeServiceOption {
	return func(svc *corev1.Service) { svc.Spec.ClusterIP = ip }
}

func withLoadBalancerIP(ip string) fakeServiceOption {
	return func(svc *corev1.Service) {
		svc.Spec.Type = corev1.ServiceTypeLoadBalancer
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: ip}}
	}
}

func createFakeRWService(client *kubernetes.Interface, namespace, clusterName, instanceID string, svcType corev1.ServiceType, opts ...fakeServiceOption) error {
	labels := dcm.Labels(instanceID)
	selectorLabels := cnpgRWSelectorLabels(clusterName)
	svcPorts := make([]corev1.ServicePort, 1)
	svcPorts = append(svcPorts, corev1.ServicePort{
		Port:       5432,
		TargetPort: intstr.FromInt32(5432),
		Protocol:   corev1.ProtocolTCP,
	})
	name := clusterName + "-rw"
	if svcType == corev1.ServiceTypeLoadBalancer {
		name = clusterName + "-external-rw"
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     svcType,
			Selector: selectorLabels,
			Ports:    svcPorts,
		},
	}
	for _, opt := range opts {
		opt(svc)
	}

	_, err := (*client).CoreV1().Services(namespace).Create(context.Background(), svc, metav1.CreateOptions{})
	return err
}

func getCreatedSecret(client *kubernetes.Interface, namespace string) (*corev1.Secret, error) {
	list, err := (*client).CoreV1().Secrets(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no services found in namespace %s", namespace)
	}
	return &list.Items[0], nil
}

func cnpgRWSelectorLabels(clusterName string) map[string]string {
	return map[string]string{
		"cnpg.io/cluster":      clusterName,
		"cnpg.io/instanceRole": "primary",
	}
}

type fakePvcOption func(*corev1.PersistentVolumeClaim)

func createFakePvc(client *kubernetes.Interface, namespace, parentPod, instanceID, storageClass string, opts ...fakePvcOption) error {
	labels := dcm.Labels(instanceID)
	labels["cnpg.io/instanceName"] = parentPod
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      parentPod,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To(storageClass),
		},
	}
	for _, opt := range opts {
		opt(pvc)
	}

	_, err := (*client).CoreV1().PersistentVolumeClaims(namespace).Create(context.Background(), pvc, metav1.CreateOptions{})
	return err
}
