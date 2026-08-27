# Kind Cluster Setup for Embedded SPs

Embedded container and VM Service Providers need a Kubernetes API reachable from the agent compose container. Kind runs
the cluster in a container on a separate network from compose by default, so this guide connects your existing Kind
cluster to the compose network and generates a kubeconfig that works in-container.

The pattern
matches [control-plane deploy/docs/k8s-container-sp-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/k8s-container-sp-kind.md).

## Prerequisites

- Kind installed with a running cluster (`kubectl cluster-info` succeeds)
- `kubectl` context set to your Kind cluster (e.g. `kind-kind` for the default cluster)

Deploy scripts use `kubectl config current-context` and require a Kind context name
`kind-<cluster-name>` (e.g. `kind-dcm-local` → node `dcm-local-control-plane`). They fail if
kubectl is missing, there is no current context, or the context is not `kind-<name>`.

## KubeVirt (embedded vm SP)

When `vm` is in `AGENT_EMBEDDED_SPS`, install KubeVirt **before** `make compose-up`. The agent
registers embedded SPs on startup; the vm SP expects KubeVirt CRDs and the operator to be present.

```bash
make install-kubevirt
```

Default operator version: `v1.5.0` (`KUBEVIRT_VERSION` to override).

Confirm (adjust context if needed):

```bash
kubectl get kv -n kubevirt
```

## Connect Kind to compose

Start compose **before** connecting Kind so the target network exists.

**Standalone agent stack:**

```bash
make install-kubevirt          # when vm is in AGENT_EMBEDDED_SPS
make kubeconfig-for-compose
make compose-up
make kind-connect
```

**Control-plane compose stack** (Kind on `control-plane_default`):

See [control-plane deploy/docs/environment-agent-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md).

The script connects the Kind control-plane node to the compose network with alias
`kubernetes` — a Subject Alternative Name on the API server certificate.

Kind and compose must use the **same container runtime**. If Kind was created with
Docker but compose uses Podman (common when both are installed), network connect fails.
Create Kind with Podman when using `podman compose`:

```bash
KIND_EXPERIMENTAL_PROVIDER=podman kind create cluster --name dcm-local
```

Or use `docker compose` for the agent stack when Kind uses Docker.

### Why this is needed

| Problem                                   | Cause                                                  |
|-------------------------------------------|--------------------------------------------------------|
| Agent container cannot reach Kind API     | Kind and compose use different container networks      |
| Kubeconfig uses `127.0.0.1:<random-port>` | Host port mapping is unreachable from other containers |
| TLS error with arbitrary hostname         | API server cert only trusts specific SANs              |

Connecting Kind with a certificate-valid alias and rewriting the server URL to
`https://kubernetes:6443` fixes all three.

### Manual SAN check

Replace `kind-control-plane` with your node name if using a non-default cluster:

```bash
podman exec kind-control-plane \
  openssl x509 -in /etc/kubernetes/pki/apiserver.crt -noout -text \
  | grep -A1 "Subject Alternative Name"
```

## Compose-friendly kubeconfig

`make kubeconfig-for-compose` writes `deploy/.kube/config` (API URL `https://kubernetes:6443`).
Run it before `compose-up` so the agent bind-mounts the file. Set `AGENT_KUBECONFIG_HOST=.kube/config`
in `deploy/.env` (default in `.env.example`).

## Container SP external services

Set `SP_K8S_EXTERNAL_SVC_TYPE` when `container` is in `AGENT_EMBEDDED_SPS`:

| Value          | Use case                                                     |
|----------------|--------------------------------------------------------------|
| `NodePort`     | Default; works with Kind                                     |
| `LoadBalancer` | Clusters with a load-balancer controller (MetalLB, cloud LB) |

Compose defaults to `NodePort`.

## Teardown

Before `make compose-down`, Kind is disconnected automatically.

## Troubleshooting

**`connection refused` from agent to Kubernetes**

- Confirm Kind is on the compose network:
  `podman inspect kind-control-plane --format '{{json .NetworkSettings.Networks}}'`
- Regenerate kubeconfig and restart compose.
- Confirm `kubectl config current-context` is `kind-<cluster-name>` for your cluster.

**Registration loops / agent unhealthy**

- Standalone stack: ensure control-plane is running on the host or change `DCM_REGISTRATION_URL`.
- Full platform: use the control-plane compose profile (see control-plane deploy docs).

**KubeVirt install timeout**

- Kind nodes need sufficient memory (4 GB+ recommended for KubeVirt).
- Check operator pods: `kubectl -n kubevirt get pods`.
