# Deploying the Environment Agent

Standalone compose stack: NATS + environment-agent. For control-plane + agent together, see
[control-plane deploy/docs/environment-agent-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md).

## Prerequisites

- [Docker](https://www.docker.com/) or [Podman](https://podman.io/)
- [Kind](https://kind.sigs.k8s.io/) with a **running cluster** (`kubectl cluster-info` succeeds) and `kubectl`
  context set (e.g. `kind-dcm-local` for cluster `dcm-local`)
- [Go 1.25+](https://go.dev/) (optional — compose builds the image from the repo)

Create a cluster if you do not have one (example):

```bash
kind create cluster --name dcm-local
```

Kind and compose must use the **same container runtime** (Docker vs Podman).

## Quick start

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

## Configuration

Copy `deploy/.env.example` to `deploy/.env`. With `-f deploy/compose.yaml`, Compose uses `deploy/` as the
project directory, so `.env` and paths like `.kube/config` resolve there automatically.

| Variable | Default | Notes |
|----------|---------|--------|
| `AGENT_EMBEDDED_SPS` | _(in example)_ | `container`, `vm`, `cluster` |
| `AGENT_KUBECONFIG_HOST` | `.kube/config` | Written by `make kubeconfig-for-compose` |
| `SP_K8S_NAMESPACE` | `default` | Container SP workloads — create on the cluster if changed |
| `KUBERNETES_NAMESPACE` | `default` | VM SP workloads — create on the cluster if changed |
| `AGENT_PORT` | `8081` | Host port for agent API |
| `DCM_REGISTRATION_URL` | `http://host.docker.internal:8080` | Base URL only |
| `SP_K8S_EXTERNAL_SVC_TYPE` | `NodePort` | Required for container SP on Kind |

**Namespaces:** container and vm SPs default to `default`. If you set `SP_K8S_NAMESPACE` or
`KUBERNETES_NAMESPACE` in `.env`, create those namespaces on the Kind cluster before creating
resources (`kubectl create namespace <name>`).

**VM SP:** when `vm` is in `AGENT_EMBEDDED_SPS`, run `make install-kubevirt` on the Kind cluster
**before** `make compose-up` so KubeVirt is ready when the agent starts and registers the vm SP.

See `deploy/.env.example` for the full list.

## Scripts

| Script | Purpose |
|--------|---------|
| `kubeconfig-for-compose.sh` | Write `deploy/.kube/config` for compose |
| `kind-connect.sh` | Join Kind to compose network |
| `kind-disconnect.sh` | Disconnect Kind before `compose-down` |
| `install-kubevirt.sh` | KubeVirt on Kind (vm SP) |
| `verify.sh` | Health and provider checks |
| `publish-create-requests.sh` | Sample NATS create requests (`deploy/samples/`) |

```bash
make publish-creates
```

## Further reading

- [Kind cluster setup](docs/kind.md)
- [Control-plane deploy integration](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md)
