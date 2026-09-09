# Deploying the Environment Agent

Standalone compose stack: NATS + environment-agent. For control-plane + agent together, see
[control-plane deploy/docs/environment-agent-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md).

## Deployment models

| Model | When                                                | Guide |
|-------|-----------------------------------------------------|--------|
| **Compose outside the cluster** | Local dev with Kind or OpenShift; agent in compose | [Compose + Kubernetes](docs/compose-kind.md) + [control-plane agent guide](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md) |
| **In-cluster** | Agent Pod on the same cluster as SP workloads       | [In-cluster agent](docs/in-cluster.md) |

## Prerequisites

- [Docker](https://www.docker.com/) or [Podman](https://podman.io/)
- [Kind](https://kind.sigs.k8s.io/) with a running cluster (`kubectl cluster-info` succeeds) and `kubectl`
  context set (e.g. `kind-dcm-local` for cluster `dcm-local`)
- [utilities](https://github.com/dcm-project/utilities) repo as a sibling directory (`../utilities`) for
  Kind, compose network, and KubeVirt helper scripts

## Create a cluster (if you do not have one):

```bash
kind create cluster --name dcm-local --config deploy/k8s/kind-local.yaml
kubectl config use-context kind-dcm-local
```

The `deploy/k8s/kind-local.yaml` maps NodePorts `30081` and `30422` to localhost
so `make k8s-verify` works on Docker Desktop and similar hosts.

## Quick start (Agent on host and not on cluster)
Kind and compose must use the **same container runtime** (Docker vs Podman).

```bash
cp deploy/.env.example deploy/.env
make install-kubevirt          # when vm is in AGENT_EMBEDDED_SPS (before compose-up registers the SP)
make kubeconfig-for-compose    # deploy/.kube/config ready before compose bind-mounts it
make compose-up
# make compose-up-with-nats    # agent + bundled NATS (--profile nats)
make kind-connect              # join Kind to compose network (after compose-up)
make deploy-verify
```

Agent API: `http://localhost:8081`. Registration defaults to control-plane on the host at
`http://host.docker.internal:8080` (retries until reachable).

```bash
make compose-down              # disconnects Kind, tears down volumes
```

## Quick start (Agent on cluster)

See [in-cluster.md](docs/in-cluster.md) for full detail.

```bash
kubectl config use-context kind-dcm-local
make install-kubevirt          # when vm is in AGENT_EMBEDDED_SPS
make k8s-deploy
# make k8s-deploy-with-nats    # bundled in-cluster NATS + nats-init
make k8s-verify
```

```bash
kubectl -n dcm delete deployment,service environment-agent --ignore-not-found
kubectl -n dcm delete serviceaccount environment-agent --ignore-not-found
kubectl -n default delete role,rolebinding environment-agent-workloads --ignore-not-found
```

Full teardown when nothing else uses the namespace: `kubectl delete namespace dcm`

## Test with sample create requests

After the agent is healthy, publish sample container and VM `dcm.request.create` CloudEvents to
NATS (`deploy/samples/`). The agent routes them to the embedded SPs on Kind.

**Compose stack** (NATS running on `localhost:4222` and agent on `localhost:8081`):

```bash
make publish-creates
```

**In-cluster** (`make k8s-verify` must succeed first to resolve agent and NATS URLs via NodePort):

```bash
make k8s-publish-creates
```

Watch workloads on Kind:

```bash
kubectl get deploy,svc -l dcm.project/managed-by=dcm
kubectl get virtualmachines -A -l dcm.project/managed-by=dcm
```

## Configuration

Copy `deploy/.env.example` to `deploy/.env`. With `-f deploy/compose.yaml`, Compose uses `deploy/` as the
project directory, so `.env` and paths like `.kube/config` resolve there automatically.

Compose and in-cluster deploy share the default image tag `main` (`ENVIRONMENT_AGENT_VERSION`).
Override to pin another release or a local tag (e.g. when building with `make image-build`).

| Variable | Default | Notes |
|----------|-------|------------------------|
| `AGENT_EMBEDDED_SPS` | _empty_ (set in .env) | e.g. `container`, `vm`, `cluster`|
| `ENVIRONMENT_AGENT_VERSION` | `main` | Image tag for compose and `k8s-deploy` |
| `AGENT_KUBECONFIG_HOST` | `.kube/config` | Host kubeconfig; written by `make kubeconfig-for-compose` |
| `SP_DEFAULT_KUBECONFIG` | `/kubeconfig` | In-container path (set in `compose.yaml`; do not set in `.env`) |
| `SP_CONTAINER_NAMESPACE` | `default` | Container SP workloads — create on the cluster if changed |
| `SP_VM_NAMESPACE` | `default` | VM SP workloads — create on the cluster if changed |
| `SP_STORAGE_NAMESPACE` | `default` | Storage SP workloads — create on the cluster if changed |
| `AGENT_PORT` | `8081` | Host port for agent API |
| `DCM_REGISTRATION_URL` | `http://host.docker.internal:8080` | Standalone compose default (CP on host base URL only). See [in-cluster.md](docs/in-cluster.md) for other models. |
| `SP_K8S_EXTERNAL_SVC_TYPE` | `NodePort` | Required for container SP on Kind |

## Scripts

| Script | Purpose |
|--------|---------|
| `k8s-deploy.sh` | Apply `deploy/k8s/` in-cluster stack |
| `verify.sh` | Health and provider checks |
| `publish-create-requests.sh` | Sample NATS create requests (`deploy/samples/`) |

Shared Kind, compose, KubeVirt helpers live in [dcm-project/utilities](https://github.com/dcm-project/utilities)

## Further reading

- [Agent in the same cluster as workloads](docs/in-cluster.md)
- [Compose + Kind setup](docs/compose-kind.md)
- [Control-plane deploy integration](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md)
