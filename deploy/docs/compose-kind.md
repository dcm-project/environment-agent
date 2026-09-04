# Compose + Kind Setup for Embedded SPs

Embedded Service Providers need a Kubernetes API reachable from the agent. This
guide covers the scenario where Agent runs in compose on a host, not on Kind. 
Embedded SP workloads run on the Kind cluster. 
When the agent runs in the same cluster as workloads, see [in-cluster.md](in-cluster.md).

Kind helper scripts (`kind-connect`, `kubeconfig-for-compose`, `install-kubevirt`) come from the
[utilities](https://github.com/dcm-project/utilities) repo (`../utilities` by default)

### Create a Kind cluster

```bash
kind create cluster --name dcm-local
kubectl config use-context kind-dcm-local
```

## KubeVirt (embedded vm SP)

When `vm` is in `AGENT_EMBEDDED_SPS`, install KubeVirt **before** `make compose-up`. The agent
registers embedded SPs on startup; the vm SP expects KubeVirt CRDs and the operator to be present.

```bash
make install-kubevirt
kubectl get kv -n kubevirt
```

## Connect Kind to compose

Kind and compose run on **separate Docker networks**, so the agent container cannot reach the API
using a normal host kubeconfig (`127.0.0.1` from inside compose fails and TLS needs a cert-trusted
hostname). So `make kubeconfig-for-compose` writes `deploy/.kube/config` with server URL
`https://kubernetes:6443` and `make kind-connect` joins the Kind node to the compose network with
alias `kubernetes` (an API server cert SAN).

Start compose before `kind-connect` so the target network exists. Kind and compose must use the
same container runtime (Docker vs Podman).

**Standalone agent stack:**

```bash
make kubeconfig-for-compose
make compose-up
make kind-connect
```

## Container SP external services

Set `SP_K8S_EXTERNAL_SVC_TYPE` when `container` is in `AGENT_EMBEDDED_SPS`:

| Value          | Use case                                                     |
|----------------|--------------------------------------------------------------|
| `NodePort`     | Default; works with Kind                                     |
| `LoadBalancer` | Clusters with a load-balancer controller (MetalLB, cloud LB) |

Compose defaults to `NodePort`.

## Teardown

Run `make compose-down`.

## Troubleshooting

**Getting `connection refused` from agent to cluster**

- Confirm Kind is on the compose network:
  `podman inspect kind-control-plane --format '{{json .NetworkSettings.Networks}}'`
- Regenerate kubeconfig and restart compose.
- Confirm `kubectl config current-context` is `kind-<cluster-name>` for your cluster.

**TLS / certificate errors**

Replace `kind-control-plane` with your node name if using a non-default cluster:

```bash
podman exec kind-control-plane \
  openssl x509 -in /etc/kubernetes/pki/apiserver.crt -noout -text \
  | grep -A1 "Subject Alternative Name"
```

**Registration loops / agent unhealthy**

- Standalone stack: ensure control-plane is running on the host or change `DCM_REGISTRATION_URL`.
- Full platform: use the control-plane compose profile (see control-plane deploy docs).
