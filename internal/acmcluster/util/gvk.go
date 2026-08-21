package util

import "k8s.io/apimachinery/pkg/runtime/schema"

// API group coordinates used across the service provider.
const (
	HiveGroup   = "hive.openshift.io"
	HiveVersion = "v1"

	HypershiftGroup   = "hypershift.openshift.io"
	HypershiftVersion = "v1beta1"

	KubevirtGroup   = "kubevirt.io"
	KubevirtVersion = "v1"

	AgentInstallGroup   = "agent-install.openshift.io"
	AgentInstallVersion = "v1beta1"
)

// Pre-built GVKs and GVRs for unstructured / dynamic client lookups.
var (
	HostedClusterGVK = schema.GroupVersionKind{
		Group: HypershiftGroup, Version: HypershiftVersion, Kind: "HostedCluster",
	}
	HostedClusterListGVK = schema.GroupVersionKind{
		Group: HypershiftGroup, Version: HypershiftVersion, Kind: "HostedClusterList",
	}
	HostedClusterGVR = schema.GroupVersionResource{
		Group: HypershiftGroup, Version: HypershiftVersion, Resource: "hostedclusters",
	}

	NodePoolGVK = schema.GroupVersionKind{
		Group: HypershiftGroup, Version: HypershiftVersion, Kind: "NodePool",
	}
	NodePoolListGVK = schema.GroupVersionKind{
		Group: HypershiftGroup, Version: HypershiftVersion, Kind: "NodePoolList",
	}
	NodePoolGVR = schema.GroupVersionResource{
		Group: HypershiftGroup, Version: HypershiftVersion, Resource: "nodepools",
	}

	ClusterImageSetGVK = schema.GroupVersionKind{
		Group: HiveGroup, Version: HiveVersion, Kind: "ClusterImageSet",
	}
	ClusterImageSetListGVK = schema.GroupVersionKind{
		Group: HiveGroup, Version: HiveVersion, Kind: "ClusterImageSetList",
	}

	KubevirtVMIGVK = schema.GroupVersionKind{
		Group: KubevirtGroup, Version: KubevirtVersion, Kind: "VirtualMachineInstance",
	}
	KubevirtVMIListGVK = schema.GroupVersionKind{
		Group: KubevirtGroup, Version: KubevirtVersion, Kind: "VirtualMachineInstanceList",
	}

	AgentGVK = schema.GroupVersionKind{
		Group: AgentInstallGroup, Version: AgentInstallVersion, Kind: "Agent",
	}
	AgentListGVK = schema.GroupVersionKind{
		Group: AgentInstallGroup, Version: AgentInstallVersion, Kind: "AgentList",
	}
)
