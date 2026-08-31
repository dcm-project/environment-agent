# Agent in the Same Cluster as Workloads

Run the environment-agent as a Pod on the Kubernetes cluster where embedded Service Providers 
create workloads. The agent talks to the API server via the pod `ServiceAccount` (in-cluster auth).

Use this model when:

- DCM control-plane and NATS also run on the cluster (for example via the
  [control-plane Helm chart](https://github.com/dcm-project/control-plane/blob/main/deploy/helm/dcm/README.md))
- The agent runs as a Pod on the cluster (not in a compose stack on the host)

For Kind with the agent **outside** the cluster on compose, see [compose-kind.md](compose-kind.md).

## Prerequisites

- Kubernetes 1.24+ or OpenShift 4.12+
- **KubeVirt** installed on the cluster when `vm` is in `AGENT_EMBEDDED_SPS` (install before the
  agent Pod starts so vm SP registration succeeds)
- Workload namespaces exist if you change defaults (`SP_K8S_NAMESPACE`, `KUBERNETES_NAMESPACE`)
- Outbound reachability from the agent Pod to control-plane HTTP and NATS

## Try it on Kind (step by step)

This deploys NATS and the environment-agent **inside** the Kind cluster (`deploy/k8s/`).

### 1. Create a Kind cluster

```bash
kind create cluster --name dcm-local
kubectl config use-context kind-dcm-local
```

### 2. Install KubeVirt (when `vm` is in `AGENT_EMBEDDED_SPS`)

If the manifest (`deploy/k8s/environment-agent.yaml`) enables `vm` SP, install KubeVirt **before** the agent Pod starts:

```bash
make install-kubevirt
kubectl get kv -n kubevirt
```

### 3. Build the agent image and deploy it on Kind

From the environment-agent repo root:

```bash
make k8s-deploy
```

This builds `quay.io/dcm-project/environment-agent:dev`, loads it into the current Kind
cluster, applies `deploy/k8s/`, runs `nats-init`, and waits for the agent Deployment.

### 4. Verify

```bash
make k8s-verify
```

You should see agent health and embedded providers (if enabled). DCM registration retries
until a control-plane is reachable.

### 5. Publish sample create requests

```bash
make k8s-publish-creates
```

### 6. Teardown

```bash
kubectl delete namespace dcm
kind delete cluster --name dcm-local
```

## Configuration

Do **not** set `AGENT_KUBECONFIG` and do **not** mount a kubeconfig file. When unset, embedded SPs
use in-cluster configuration (see `internal/config/config.go` and `internal/openshift/kubeconfig/rest.go`).

Typical environment variables for a Pod in namespace `dcm`:

```yaml
env:
  - name: AGENT_EMBEDDED_SPS
    value: "container,vm"
  - name: AGENT_NAME
    value: "cluster-agent"
  - name: DCM_REGISTRATION_URL
    value: "http://dcm-control-plane:8080"
  - name: AGENT_MESSAGING_URL
    value: "nats://dcm-nats:4222"
  - name: SP_K8S_NAMESPACE
    value: default
  - name: SP_K8S_EXTERNAL_SVC_TYPE
    value: LoadBalancer
  - name: KUBERNETES_NAMESPACE
    value: default
  - name: AGENT_SP_PERSISTENCE_PATH
    value: /var/lib/environment-agent/data/registrations.json
```

## ServiceAccount and RBAC

Bind the agent Pod to a `ServiceAccount` with permissions in each namespace where SPs create
workloads (not only the agent's own namespace).

**Container SP** (namespace = `SP_K8S_NAMESPACE`):

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps", "secrets", "persistentvolumeclaims", "events"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments", "statefulsets", "replicasets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

**VM SP** (namespace = `KUBERNETES_NAMESPACE`):

```yaml
rules:
  - apiGroups: ["kubevirt.io"]
    resources: ["virtualmachines", "virtualmachineinstances"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

Example Role + RoleBinding when the agent runs in `dcm` and workloads land in `default`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: environment-agent
  namespace: dcm
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: environment-agent-workloads
  namespace: default
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps", "secrets", "persistentvolumeclaims", "events"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments", "statefulsets", "replicasets"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["kubevirt.io"]
    resources: ["virtualmachines", "virtualmachineinstances"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: environment-agent-workloads
  namespace: default
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: environment-agent-workloads
subjects:
  - kind: ServiceAccount
    name: environment-agent
    namespace: dcm
```

If container and VM namespaces differ, create a Role (or tailored rules) in each namespace and bind
the same `ServiceAccount`.

## Pod spec essentials

```yaml
spec:
  serviceAccountName: environment-agent
  containers:
    - name: environment-agent
      image: quay.io/dcm-project/environment-agent:main
      # No kubeconfig volume — in-cluster auth only
      volumeMounts:
        - name: registrations
          mountPath: /var/lib/environment-agent/data
  volumes:
    - name: registrations
      emptyDir: {}
```

`AGENT_SP_PERSISTENCE_PATH` must be a file (JSON store), not the mount path. Mount the volume on
the parent directory (e.g. `.../data`) and point the env var at `.../data/registrations.json`.

Use a PVC instead of `emptyDir` if SP registrations must survive Pod restarts.

## Verify

```bash
make k8s-verify
make k8s-publish-creates
```

## Troubleshooting

**Embedded SP missing or unhealthy after start**

- Confirm KubeVirt is Available before the agent Pod starts when `vm` is enabled.
- Check agent logs for RBAC `Forbidden` errors against workload namespaces.

**Agent not reachable when `make k8s-verify` fails**

- Confirm the agent pod is running: `kubectl -n dcm get pods,svc`
- On Kind, reach the agent via the node IP and NodePort `30081`:

```bash
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
curl "http://${NODE_IP}:30081/api/v1alpha1/health"
```