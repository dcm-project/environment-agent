# Deploying the Environment Agent

Standalone compose stack: NATS + environment-agent. For control-plane + agent together, see
[control-plane deploy/docs/environment-agent-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md).

## Deployment models

| Model | When                                                | Guide |
|-------|-----------------------------------------------------|--------|
| **Compose outside the cluster** | Local dev with Kind; agent in Docker/Podman compose | Quick start below + [Compose + Kind](docs/compose-kind.md) |
| **In-cluster** | Agent Pod on the same cluster as SP workloads       | [In-cluster agent](docs/in-cluster.md) |

## Prerequisites

- [Docker](https://www.docker.com/) or [Podman](https://podman.io/)
- [Kind](https://kind.sigs.k8s.io/) with a running cluster (`kubectl cluster-info` succeeds) and `kubectl`
  context set (e.g. `kind-dcm-local` for cluster `dcm-local`)

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
make k8s-verify
```

```bash
kubectl delete namespace dcm
```

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

Compose + Kind and in-cluster Kind are both local workflows. They share
the default image tag `dev` (`ENVIRONMENT_AGENT_VERSION`) and `make image-build` output, so you can
switch between compose and in-cluster without rebuilding under a different tag. For production
deployments, pin `ENVIRONMENT_AGENT_VERSION`  to a release image.

| Variable | Default                       | Notes |
|----------|-------------------------------|------------------------|
| `AGENT_EMBEDDED_SPS` | _empty_ | e.g. `container`, `vm`, `cluster`|
| `ENVIRONMENT_AGENT_VERSION` | `dev` | Local image tag for compose and `k8s-deploy`. Pin a release tag for production |
| `AGENT_KUBECONFIG_HOST` | `.kube/config` | Written by `make kubeconfig-for-compose` |
| `SP_K8S_NAMESPACE` | `default` | Container SP workloads — create on the cluster if changed |
| `KUBERNETES_NAMESPACE` | `default` | VM SP workloads — create on the cluster if changed |
| `AGENT_PORT` | `8081` | Host port for agent API |
| `DCM_REGISTRATION_URL` | `http://host.docker.internal:8080` | Standalone compose default (CP on host base URL only). See [in-cluster.md](docs/in-cluster.md) for other models. |
| `SP_K8S_EXTERNAL_SVC_TYPE` | `NodePort`| Required for container SP on Kind |

## Scripts

| Script | Purpose |
|--------|---------|
| `kubeconfig-for-compose.sh` | Write `deploy/.kube/config` for compose |
| `kind-local.yaml` | Kind config with host port mappings for in-cluster NodePorts |
| `k8s-deploy.sh` | Apply `deploy/k8s/` in-cluster stack |
| `make k8s-verify` / `make k8s-publish-creates` | Verify agent and publish samples via NodePorts |
| `kind-connect.sh` | Join Kind to compose network |
| `kind-disconnect.sh` | Disconnect Kind before `compose-down` |
| `install-kubevirt.sh` | KubeVirt on Kind (vm SP) |
| `verify.sh` | Health and provider checks |
| `publish-create-requests.sh` | Sample NATS create requests (`deploy/samples/`) |

## Further reading

- [Agent in the same cluster as workloads](docs/in-cluster.md)
- [Compose + Kind setup](docs/compose-kind.md)
- [Control-plane deploy integration](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md)
