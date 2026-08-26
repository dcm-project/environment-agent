# Deploying the Environment Agent

Compose stack and Kind helpers for running the environment-agent locally with embedded Service Providers (container, vm,
cluster).

For end-to-end DCM workflows (control-plane, UI, agent on one NATS cluster), use
the [control-plane deploy stack](https://github.com/dcm-project/control-plane/tree/main/deploy)
— see `deploy/docs/environment-agent-kind.md` there.

## Prerequisites

- [Podman](https://podman.io/) or [Docker](https://www.docker.com/)
- [Kind](https://kind.sigs.k8s.io/) with an existing cluster and `kubectl` context configured
- [Go 1.25+](https://go.dev/) (optional — compose builds the image from the repo)

The root Makefile auto-detects `podman` or `docker` for compose and image builds.

## Quick start

Standalone mode runs NATS and the agent in compose. Registration targets a control-plane on the host at
`http://host.docker.internal:8080` by default (the agent retries until it succeeds).

### 1. Connect Kind to compose networking

Ensure your Kind cluster is running (`kubectl cluster-info`). Scripts detect the cluster from your current `kubectl`
context (`kind-<name>`) or `KIND_CLUSTER_NAME`.

```bash
make compose-up                    # creates environment-agent_default network
make kind-connect
make kubeconfig-for-compose
export AGENT_KUBECONFIG_HOST="$(pwd)/deploy/.kube/config"
```

For the embedded **vm** SP, install KubeVirt on Kind:

```bash
make install-kubevirt
```

See [Kind cluster setup](docs/kind.md) for networking details and troubleshooting.

### 2. Start the agent stack

```bash
make compose-up
```

The agent API is at `http://localhost:8081` (mapped from container port 8080).

### 3. Verify

```bash
make deploy-verify
curl http://localhost:8081/api/v1alpha1/health
curl http://localhost:8081/api/v1alpha1/providers
```

## Stopping services

```bash
make compose-down
```

`compose-down` disconnects Kind from compose networks before tearing down volumes.

## Configuration

Copy `deploy/.env.example` to `.env` in the repo root for persistent overrides.

| Variable                    | Default                            | Description                                                                                                      |
|-----------------------------|------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `AGENT_NAME`                | `local-agent`                      | Agent display name sent to DCM                                                                                   |
| `AGENT_ENVIRONMENT`         | `dev`                              | Environment classification                                                                                       |
| `AGENT_COST`                | `low`                              | Cost classification                                                                                              |
| `AGENT_PORT`                | `8081`                             | Host port for the agent HTTP API                                                                                 |
| `DCM_REGISTRATION_URL`      | `http://host.docker.internal:8080` | **Base URL only** — agent posts to `/api/v1alpha1/agents`                                                        |
| `AGENT_MESSAGING_URL`       | `nats://nats:4222`                 | NATS JetStream URL                                                                                               |
| `AGENT_EMBEDDED_SPS`        | _(none)_                           | Comma-separated embedded SP types: `container`, `vm`, `cluster` — set explicitly to enable                       |
| `AGENT_KUBECONFIG_HOST`     | `~/.kube/config`                   | Host kubeconfig bind-mounted to `/kubeconfig` (must be world-readable, e.g. `chmod 644`, or use `deploy/.kube/config`) |
| `SP_K8S_NAMESPACE`          | `default`                          | Namespace for container SP workloads                                                                             |
| `SP_K8S_EXTERNAL_SVC_TYPE`  | `NodePort`                         | Kubernetes Service type for external ports (`NodePort` or `LoadBalancer`)                                        |
| `KUBERNETES_NAMESPACE`      | `default`                          | Namespace for VM SP workloads                                                                                    |
| `SP_CLUSTER_NAMESPACE`      | _(required for cluster SP)_        | Namespace for ACM hosted clusters                                                                                |
| `SP_PULL_SECRET`            | _(required for cluster SP)_        | Base64-encoded dockerconfigjson for cluster SP                                                                   |
| `SP_BASE_DOMAIN`            | _(optional)_                       | Base DNS domain for hosted clusters                                                                              |
| `ENVIRONMENT_AGENT_VERSION` | `main`                             | Image tag when not building locally                                                                              |
| `KIND_CLUSTER_NAME`         | _(from kubectl context)_           | Kind cluster name when not `kind-<name>` context                                                                 |
| `COMPOSE_NETWORK`           | `environment-agent_default`        | Network for `kind-connect.sh`                                                                                    |

See `deploy/.env.example` for the full embedded SP variable list (container, vm, cluster).

### Embedded SP requirements

| SP        | `AGENT_EMBEDDED_SPS` value | Extra setup                                                             |
|-----------|----------------------------|-------------------------------------------------------------------------|
| Container | `container`                | Kind + compose-friendly kubeconfig; `SP_K8S_EXTERNAL_SVC_TYPE` required |
| VM        | `vm`                       | Kind + KubeVirt operator; `KUBERNETES_NAMESPACE`                        |
| Cluster   | `cluster`                  | OpenShift ACM/MCE; `SP_CLUSTER_NAMESPACE`, `SP_PULL_SECRET` required    |

## Scripts

| Script                              | Purpose                                                |
|-------------------------------------|--------------------------------------------------------|
| `scripts/kind-connect.sh`           | Join Kind to a compose network with `kubernetes` alias |
| `scripts/kubeconfig-for-compose.sh` | Write `deploy/.kube/config` for in-network API access  |
| `scripts/install-kubevirt.sh`       | Install KubeVirt on Kind (vm SP)                       |
| `scripts/kind-disconnect.sh`        | Disconnect Kind before compose teardown                |
| `scripts/verify.sh`                 | Agent health and provider checks                       |
| `scripts/publish-create-requests.sh` | Publish sample container + VM creates (see below)      |
| `scripts/ensure-nats-stream.sh`      | Create `dcm-agent-requests` stream for local publish   |

## Sample create requests

After the agent is running with `AGENT_EMBEDDED_SPS=container,vm` and Kind + KubeVirt
(for VM) are ready:

```bash
make publish-creates
```

Requires `jq` and the NATS CLI (`nats`) or podman/docker for `nats-box` fallback.
Publishes `dcm.request.create` CloudEvents to `dcm.agent.{AGENT_NAME}` using fixtures in `deploy/fixtures/`:

| File | Service type | Spec |
|------|--------------|------|
| `fixtures/container-create-spec.json` | `container` | nginx Deployment (`docker.io/library/nginx:latest`) |
| `fixtures/vm-create-spec.json` | `vm` | Fedora VM (2 vCPU, 2Gi RAM, 10Gi disk) |

Optional `.env` overrides:

```bash
CONTAINER_RESOURCE_ID=my-container-1
VM_RESOURCE_ID=my-vm-1
```

Verify on the cluster:

```bash
kubectl get deploy,svc -l dcm.project/managed-by=dcm
kubectl get virtualmachines -A -l dcm.project/managed-by=dcm
```

For Kind, pre-load images if pulls fail:

```bash
docker pull docker.io/library/nginx:latest
kind load docker-image docker.io/library/nginx:latest
docker pull quay.io/containerdisks/fedora:latest
kind load docker-image quay.io/containerdisks/fedora:latest
```

## Troubleshooting

### Empty `GET /providers`

Ensure `AGENT_EMBEDDED_SPS` is set in the repo root `.env` (compose loads `.env` at the repo
root, not `deploy/.env` unless you copy or symlink it). The standalone compose stack persists
registrations under `/app/data` in the container filesystem (no named volume).

### `stream not found` or `no response from stream`

JetStream only accepts publishes when a stream binds the subject. The standalone stack
does not run control-plane (which normally creates `dcm-status` and `dcm-agent-responses`).
`make compose-up` runs `ensure-nats-stream.sh`, which creates:

| Stream | Subjects | Used for |
|--------|----------|----------|
| `dcm-agent-requests` | `dcm.agent.>` | inbound create/delete from control-plane or `publish-creates` |
| `dcm-status` | `dcm.*` | VM status events (`dcm.vm`) from the embedded vm SP |
| `dcm-agent-responses` | `dcm.agents.responses` | agent create/error ack CloudEvents |

Run manually if needed:

```bash
./deploy/scripts/ensure-nats-stream.sh
```

VM creates still succeed without `dcm-status`; only status **notifications** fail until the stream exists.

### Kubeconfig permission denied

The container runs as UID 1001 and cannot read a mode-600 host kubeconfig. Use:

```bash
make kubeconfig-for-compose
export AGENT_KUBECONFIG_HOST="$(pwd)/deploy/.kube/config"
```

## Running without compose

```bash
make build
export AGENT_NAME=local-agent
export AGENT_ENVIRONMENT=dev
export AGENT_COST=low
export DCM_REGISTRATION_URL=http://localhost:8080
export AGENT_MESSAGING_URL=nats://localhost:4222
export AGENT_EMBEDDED_SPS=container,vm
export AGENT_KUBECONFIG="${HOME}/.kube/config"
export SP_K8S_EXTERNAL_SVC_TYPE=NodePort
./bin/environment-agent
```

## Further reading

- [Kind cluster setup](docs/kind.md)
- [KubeVirt installation](https://kubevirt.io/user-guide/operations/installation/)
- [Control-plane deploy integration](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md)
