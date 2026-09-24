// Package kubevirt provides a client for the KubeVirt API.
package kubevirt

import (
	"context"
	"fmt"
	"log"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/config"
	"github.com/dcm-project/environment-agent/internal/openshift/kubevirtvm/constants"
	oshutil "github.com/dcm-project/environment-agent/internal/openshift/util"
)

// Client wraps a typed REST client for KubeVirt VM operations
type Client struct {
	restClient    *rest.RESTClient
	dynamicClient dynamic.Interface
	kubeClient    kubernetes.Interface
	namespace     string
	timeout       time.Duration
	maxRetries    int
}

var (
	kubevirtScheme         = runtime.NewScheme()
	kubevirtCodecs         serializer.CodecFactory
	kubevirtParameterCodec runtime.ParameterCodec
)

// virtualMachineGroupResource identifies the resource reported by the
// not-found errors this client synthesizes.
var virtualMachineGroupResource = schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachines"}

func init() {
	// Register KubeVirt types so the REST client can serialize/deserialize them
	schemeBuilder := runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
		s.AddKnownTypes(
			schema.GroupVersion{Group: "kubevirt.io", Version: "v1"},
			&kubevirtv1.VirtualMachine{},
			&kubevirtv1.VirtualMachineList{},
			&kubevirtv1.VirtualMachineInstance{},
			&kubevirtv1.VirtualMachineInstanceList{},
		)
		metav1.AddToGroupVersion(s, schema.GroupVersion{Group: "kubevirt.io", Version: "v1"})
		return nil
	})
	if err := schemeBuilder.AddToScheme(kubevirtScheme); err != nil {
		panic(fmt.Sprintf("failed to register KubeVirt types: %v", err))
	}
	kubevirtCodecs = serializer.NewCodecFactory(kubevirtScheme)
	kubevirtParameterCodec = runtime.NewParameterCodec(kubevirtScheme)
}

// NewClient creates a new KubeVirt client with a typed REST client for VM operations
// and a dynamic client for informers.
func NewClient(cfg *config.Config) (*Client, error) {
	if cfg == nil {
		panic("kubevirt client: config must not be nil")
	}

	var restConfig *rest.Config
	var err error

	if cfg.Kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig file %s: %w", cfg.Kubeconfig, err)
		}
	} else {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build in-cluster config: %w", err)
		}
	}

	// Create typed REST client for KubeVirt API
	kubevirtConfig := *restConfig
	kubevirtConfig.GroupVersion = &schema.GroupVersion{Group: "kubevirt.io", Version: "v1"}
	kubevirtConfig.APIPath = "/apis"
	kubevirtConfig.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: kubevirtCodecs}
	if kubevirtConfig.UserAgent == "" {
		kubevirtConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	restClient, err := rest.RESTClientFor(&kubevirtConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create KubeVirt REST client: %w", err)
	}

	// Create dynamic client
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Create standard Kubernetes clientset for health checks
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}

	return &Client{
		restClient:    restClient,
		dynamicClient: dynamicClient,
		kubeClient:    kubeClient,
		namespace:     cfg.Namespace,
		timeout:       cfg.Timeout,
		maxRetries:    cfg.MaxRetries,
	}, nil
}

// CheckHealth verifies the backing Kubernetes cluster is reachable by calling
// the API server's version discovery endpoint.
func (c *Client) CheckHealth(ctx context.Context) error {
	err := oshutil.ServerVersion(ctx, c.kubeClient)
	if err != nil {
		log.Printf("Warning: kubernetes health check failed: %v", err)
		return err
	}
	return nil
}

// CreateVirtualMachine creates a new VirtualMachine in the cluster
func (c *Client) CreateVirtualMachine(ctx context.Context, vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result := &kubevirtv1.VirtualMachine{}
	err := c.restClient.Post().
		Resource("virtualmachines").
		Namespace(c.namespace).
		Body(vm).
		Do(timeoutCtx).
		Into(result)
	if err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachine: %w", err)
	}
	result.SetGroupVersionKind(kubevirtv1.VirtualMachineGroupVersionKind)
	return result, nil
}

// GetVirtualMachine retrieves a VirtualMachine by DCM instance ID.
//
// A label-selector list that matches nothing is a 200 with an empty list, not a
// 404, so the absent-VM case is synthesized here as an *apierrors.StatusError
// rather than a plain error: callers distinguish "no such VM" from "the API
// call failed" with apierrors.IsNotFound, and the embedded handler maps it to
// a 404 SP response. A plain error would surface as a retryable 500.
func (c *Client) GetVirtualMachine(ctx context.Context, vmID string) (*kubevirtv1.VirtualMachine, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	vmList := &kubevirtv1.VirtualMachineList{}
	err := c.restClient.Get().
		Resource("virtualmachines").
		Namespace(c.namespace).
		VersionedParams(&metav1.ListOptions{
			LabelSelector: fmt.Sprintf("%s=%s", constants.DCMLabelInstanceID, vmID),
		}, kubevirtParameterCodec).
		Do(timeoutCtx).
		Into(vmList)
	if err != nil {
		return nil, fmt.Errorf("failed to get VirtualMachine by dcmlabelinstanceid: %w", err)
	}
	if len(vmList.Items) == 0 {
		return nil, apierrors.NewNotFound(virtualMachineGroupResource, vmID)
	}
	vmList.Items[0].SetGroupVersionKind(kubevirtv1.VirtualMachineGroupVersionKind)
	return &vmList.Items[0], nil
}

// ListVirtualMachines lists all VirtualMachines in the namespace
func (c *Client) ListVirtualMachines(ctx context.Context, options metav1.ListOptions) ([]kubevirtv1.VirtualMachine, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	vmList := &kubevirtv1.VirtualMachineList{}
	err := c.restClient.Get().
		Resource("virtualmachines").
		Namespace(c.namespace).
		VersionedParams(&options, kubevirtParameterCodec).
		Do(timeoutCtx).
		Into(vmList)
	if err != nil {
		return nil, err
	}
	for i := range vmList.Items {
		vmList.Items[i].SetGroupVersionKind(kubevirtv1.VirtualMachineGroupVersionKind)
	}
	return vmList.Items, nil
}

// DeleteVirtualMachine deletes a VirtualMachine by DCM instance ID
func (c *Client) DeleteVirtualMachine(ctx context.Context, vmId string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Returned unwrapped: GetVirtualMachine already annotates its own failures,
	// and a second "failed to get" prefix would misdescribe the not-found case
	// (the list call succeeded; the VM simply does not exist).
	item, err := c.GetVirtualMachine(ctx, vmId)
	if err != nil {
		return err
	}
	if item == nil {
		return apierrors.NewNotFound(virtualMachineGroupResource, vmId)
	}
	return c.restClient.Delete().
		Resource("virtualmachines").
		Namespace(c.namespace).
		Name(item.Name).
		Body(&metav1.DeleteOptions{}).
		Do(timeoutCtx).
		Error()
}

// UpdateVirtualMachine updates an existing VirtualMachine
func (c *Client) UpdateVirtualMachine(ctx context.Context, vm *kubevirtv1.VirtualMachine) (*kubevirtv1.VirtualMachine, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result := &kubevirtv1.VirtualMachine{}
	err := c.restClient.Put().
		Resource("virtualmachines").
		Namespace(c.namespace).
		Name(vm.Name).
		Body(vm).
		Do(timeoutCtx).
		Into(result)
	if err != nil {
		return nil, err
	}
	result.SetGroupVersionKind(kubevirtv1.VirtualMachineGroupVersionKind)
	return result, nil
}

// DynamicClient returns the underlying dynamic client
func (c *Client) DynamicClient() dynamic.Interface {
	return c.dynamicClient
}
