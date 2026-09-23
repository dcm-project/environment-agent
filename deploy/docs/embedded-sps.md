# Enabling Embedded Service Providers

This document explains **how** the environment-agent enables an embedded Service Provider
(SP) — the mechanism, precedence rules, and shared configuration — and then lists **every**
environment variable each embedded SP reads, sourced directly from each SP's Go config
struct under `internal/openshift/<sp>/config/config.go` (and `internal/config/config.go` for
agent-wide settings). No defaults, formats, or required fields are assumed; everything below
is what the code enforces at the time of writing.

This document is the SP-configuration reference. It intentionally does **not** repeat the
deployment-model walkthroughs (Kind/compose, in-cluster, RBAC, teardown, troubleshooting) —
see [Further reading](#further-reading) for those.

## 1. General mechanism

### 1.1 The enabling switch: `AGENT_EMBEDDED_SPS`

| Env var | Location | Default | Notes |
|---|---|---|---|
| `AGENT_EMBEDDED_SPS` | `internal/config/config.go` (`ProviderConfig.EmbeddedSPs`) | _empty_ | Comma-separated list of embedded SP identifiers, e.g. `AGENT_EMBEDDED_SPS=container,vm`. No SP is embedded unless it is explicitly named here — there is no "enable all" default. |

Rules the code enforces (`internal/embedded/*/setup.go`, `internal/provider/service/service.go`):

- **One identifier enables one SP.** An SP is enabled when its identifier appears exactly
  in `AGENT_EMBEDDED_SPS` (whitespace trimmed, case-sensitive). Empty entries are ignored.
- **Unknown identifiers are not rejected.** `AGENT_EMBEDDED_SPS` is not validated against a
  fixed enum. An identifier that matches no embedded package's `ServiceType` (e.g. a typo, or
  a service type that has no embedded implementation) still gets a provider **record**
  registered internally (`RegisterEmbedded` in `internal/provider/service/service.go`), but no
  request-routing handler or provider-specific health checker exist behind it. It falls back to
  a generic environment-controlled health checker (`AGENT_EMBEDDED_SP_{TYPE}_HEALTH`) that
  reports **healthy** when the variable is unset — so an unknown identifier can appear healthy
  in the providers API despite having no working handler. Operation requests for that service
  type will fail to route. There is currently no startup-time warning for this case, so
  double-check spelling and the supported identifiers table below.
- **Currently implemented embedded SPs** (identifiers you can safely use), from
  `internal/embedded/*/handler.go`'s `ServiceType` constants:

  | Identifier | Go package | Technology | Config table |
  |---|---|---|---|
  | `container` | `internal/openshift/container` | Kubernetes container workloads (Deployments/Pods) | [§2.1](#21-container-sp-container) |
  | `vm` | `internal/openshift/kubevirtvm` | KubeVirt `VirtualMachine`/`VirtualMachineInstance` | [§2.2](#22-vm-sp-vm) |
  | `cluster` | `internal/openshift/acmcluster` | HyperShift `HostedCluster`/`NodePool` provisioning via ACM/MCE (KubeVirt or bare-metal/Agent-based) | [§2.3](#23-cluster-sp-cluster) |
  | `storage` | `internal/openshift/storage` | Kubernetes `PersistentVolumeClaim`s | [§2.4](#24-storage-sp-storage) |
  | `network` | `internal/openshift/network` | Kubernetes `Service`/network resources | [§2.5](#25-network-sp-network) |

  > **Not embeddable today:** the database (Postgres) SP is not available as an embedded SP.
  > There is no `internal/embedded/database` package and no `database` entry in
  > `embedded.Bundles`. Putting `database` in `AGENT_EMBEDDED_SPS` will register an inert
  > provider record per the bullet above, not a working SP.

- **One SP per service type, agent-wide.** Whether embedded or external, only one SP may hold
  the slot for a given `service_type` (REQ-SPR-200). Embedded SPs register at startup, before
  any external SP can connect over REST, so on a clean agent they always win the slot for their
  service type. If a *persisted* external registration from a prior session already occupies a
  service type, the embedded SP for that type is skipped at startup (a warning is logged) and
  the agent keeps starting normally. For example, enabling `container` embedded does not
  forcibly evict an external `container` SP that registered in an earlier session.
- **No hot reload.** `AGENT_EMBEDDED_SPS` (and every other setting below) is read once at
  process start. Changing it requires restarting the agent.

### 1.2 Settings shared by every embedded SP

These come from `shared.Config` (`internal/openshift/shared/config.go`) and
`internal/config/config.go`'s `SPConfig`/`MessagingConfig`, and apply uniformly to whichever
SPs you enable:

| Env var | Applies to | Default | Required? | Notes |
|---|---|---|---|---|
| `AGENT_MESSAGING_URL` | agent (`MessagingConfig.URL`) | _empty_ | **Yes**, whenever any SP is embedded | Each embedded SP's `shared.Apply` fails config loading with `"messaging URL is required"` if this is unset, regardless of which SP you enable. This is the NATS URL the SP's status-monitor workers publish updates on. |
| `SP_DEFAULT_KUBECONFIG` | agent (`SPConfig.DefaultKubeconfig`) | _empty_ | No | Agent-wide default kubeconfig path used by **all** embedded SPs that talk to Kubernetes (all five). When empty, each SP falls back to in-cluster `ServiceAccount` auth (`rest.InClusterConfig()` in `internal/openshift/kubeconfig/rest.go`) — the model used when the agent itself runs as a Pod on the target cluster. |
| `SP_KUBECONFIG` | per-SP (`shared.Config.Kubeconfig`) | falls back to `SP_DEFAULT_KUBECONFIG` | No | Per-SP override if one embedded SP needs a *different* cluster/kubeconfig than the others. Only set this when SPs must target different clusters; otherwise use `SP_DEFAULT_KUBECONFIG`. Because all embedded SPs currently read the *same* `SP_KUBECONFIG` variable name, you cannot set distinct overrides for two different SPs in a single process today — this is a real limitation, not a documentation gap. |
| `SP_NAME` | per-SP (`shared.Config.Name`) | SP-specific (`acm-cluster-sp`, `container-sp`, `network`, `storage`, `kubevirt-vm-sp`) | No | SP-internal runtime name used by provider-specific components (e.g. NATS publisher identity, logging). This does **not** override the provider's registered name or API identity — embedded registration always uses the service type as the provider name. All embedded SPs currently read the same `SP_NAME` variable name, so this has the same one-value-shared-by-all-enabled-SPs limitation as `SP_KUBECONFIG`. |
| `AGENT_SP_PERSISTENCE_PATH` | agent (`ProviderConfig.PersistencePath`) | `/var/lib/environment-agent/registrations` | No | File path where SP registrations (embedded and external) persist across restarts, so slot ownership survives an agent restart. |
| `AGENT_HEALTH_CHECK_INTERVAL` | agent (`HealthConfig.CheckInterval`) | `10s` | No | How often the agent polls each embedded SP's health checker. Range enforced: `1s`–`5m`. |
| `AGENT_HEALTH_CHECK_TIMEOUT` | agent (`HealthConfig.CheckTimeout`) | `5s` | No | Per-check timeout. Range enforced: `500ms`–`AGENT_HEALTH_CHECK_INTERVAL`. |
| `AGENT_HEALTH_FAILURE_THRESHOLD` | agent (`HealthConfig.FailureThreshold`) | `3` | No | Consecutive failed checks before an SP flips to `Unhealthy`. Range enforced: `1`–`100`. |

`SP_HEALTH_CHECK_TIMEOUT` (cluster SP only, see §2.3) is a **different**, SP-scoped variable
from `AGENT_HEALTH_CHECK_TIMEOUT` above — don't conflate the two.

### 1.3 Generic steps to enable any embedded SP

1. Add the SP's identifier to `AGENT_EMBEDDED_SPS` (comma-separated with any others you enable).
2. Set `AGENT_MESSAGING_URL` (required unconditionally once any SP is embedded).
3. Provide cluster access: either run the agent in-cluster (nothing to set) or set
   `SP_DEFAULT_KUBECONFIG` (or per-SP `SP_KUBECONFIG`) to a kubeconfig readable by the agent
   process.
4. Set the SP-specific required/relevant variables from the tables in §2.
5. Ensure any cluster-side prerequisite is met before the agent starts (e.g. KubeVirt installed
   for `vm`; the target namespace exists for `container`/`vm`/`storage`; RBAC granted to the
   agent's identity in the workload namespace(s)). See
   [in-cluster.md](in-cluster.md#serviceaccount-and-rbac) for RBAC examples.
6. Restart the agent — configuration is read once at process start.
7. Confirm via `GET /api/v1alpha1/providers` (or `make deploy-verify` / `make k8s-verify`) that
   the SP appears with `type: embedded` and a healthy status.

## 2. Per-SP configuration reference

Each table lists **only** the variables read by that SP's own config struct — shared variables
from §1.2 are not repeated per SP.

### 2.1 Container SP (`container`)

Package: `internal/openshift/container/config/config.go`. Manages Kubernetes
Deployments/Services for container-type resources.

| Env var | Default | Required? | Valid values | Notes |
|---|---|---|---|---|
| `SP_CONTAINER_NAMESPACE` | `default` | No | any existing namespace | Namespace where container workloads are created. If changed from `default`, the namespace must already exist on the target cluster — the SP does not create it. |
| `SP_K8S_EXTERNAL_SVC_TYPE` | _empty_ | **Yes** | `LoadBalancer` or `NodePort` | Config loading fails (`invalid SP_K8S_EXTERNAL_SVC_TYPE`) for any other value, including unset/empty. Use `NodePort` on Kind/local clusters, `LoadBalancer` on OpenShift/cloud. |
| `SP_MONITOR_DEBOUNCE_MS` | `500` | No | integer milliseconds | Debounce window for the workload status-watch monitor before publishing a status update. |
| `SP_MONITOR_RESYNC_PERIOD` | `10m` | No | Go duration | Informer resync period for the underlying Kubernetes watch. |

### 2.2 VM SP (`vm`)

Package: `internal/openshift/kubevirtvm/config/config.go`. Manages KubeVirt
`VirtualMachine`/`VirtualMachineInstance` resources. Requires **KubeVirt installed on the
target cluster before the agent starts** (`make install-kubevirt`, or OpenShift CNV).

| Env var | Default | Required? | Notes |
|---|---|---|---|
| `SP_VM_NAMESPACE` | `default` | No | Namespace for VM workloads; must exist on the cluster if changed. |
| `KUBERNETES_TIMEOUT` | `60s` | No | Timeout for Kubernetes API calls made by this SP. |
| `KUBERNETES_MAX_RETRIES` | `3` | No | Retry count for Kubernetes API calls. |
| `NATS_MAX_RECONNECT` | `-1` | No | Max NATS reconnect attempts for this SP's own NATS usage (`-1` = unlimited). |
| `NATS_SUBJECT` | `dcm.vm` | No | NATS subject this SP publishes VM status events to. |
| `EVENTS_ENABLED` | `true` | No | Whether the VM event/status monitor runs at all. |
| `EVENTS_RESYNC_PERIOD` | `30m` | No | Informer resync period for the VM event monitor. |

> Note the generic-looking names (`KUBERNETES_TIMEOUT`, `KUBERNETES_MAX_RETRIES`,
> `NATS_MAX_RECONNECT`, `NATS_SUBJECT`, `EVENTS_ENABLED`, `EVENTS_RESYNC_PERIOD`) have **no**
> `SP_` or `AGENT_` prefix — this is specific to the VM SP's config struct as written in the
> source; every other embedded SP's variables are prefixed `SP_`.

### 2.3 Cluster SP (`cluster`)

Package: `internal/openshift/acmcluster/config/config.go`. Provisions OpenShift clusters
via HyperShift `HostedCluster` and `NodePool` resources on an ACM/MCE hub (KubeVirt-hosted
or bare-metal/Agent-based platforms). Requires HyperShift APIs on an ACM/MCE hub — **not**
applicable to a plain Kind cluster.

| Env var | Default | Required? | Notes |
|---|---|---|---|
| `SP_CLUSTER_NAMESPACE` | _empty_ | **Yes** | Config loading fails if unset (`env:"...,required"`). Namespace where HyperShift `HostedCluster` and `NodePool` resources (plus the pull-secret `Secret`) are created. |
| `SP_PULL_SECRET` | _empty_ | **Yes** | Config loading fails if unset. Must be a **base64-encoded `.dockerconfigjson`** (with a non-empty `auths` map) — the SP base64-decodes it, parses it as `.dockerconfigjson`, and fails at runtime (`EnsurePullSecret`) if decoding, JSON-parsing, or the `auths` check fails. |
| `SP_BASE_DOMAIN` | _empty_ | No | Base DNS domain substituted into `SP_CONSOLE_URI_PATTERN`'s `{base_domain}` placeholder. |
| `SP_CONSOLE_URI_PATTERN` | `https://console-openshift-console.apps.{name}.{base_domain}` | No | Template for the generated cluster's console URL. `{name}` and `{base_domain}` are literal placeholders substituted at runtime. |
| `SP_VERSION_MATRIX_PATH` | _empty_ (uses built-in matrix) | No | Path to a **JSON** file mapping OCP minor versions to Kubernetes minor versions (`map[string]string`, e.g. `{"4.18": "1.31"}`). When unset, the built-in `DefaultCompatibilityMatrix` (OCP 4.14–4.21 → K8s 1.27–1.34, see `internal/openshift/acmcluster/version/matrix.go`) is used. Loading fails if the file can't be read or doesn't parse as this JSON shape. |
| `SP_DEFAULT_INFRA_ENV` | _empty_ | No | Default Assisted Installer `InfraEnv` name, used by the bare-metal/Agent-based platform path only. |
| `SP_AGENT_NAMESPACE` | _empty_ | No | Namespace holding `Agent` CRs on the ACM hub (agent-install namespace) for bare-metal provisioning — unrelated to the environment-agent process itself. |
| `SP_INFRA_ENV_LABEL_KEY` | `infraenvs.agent-install.openshift.io` | No | Label key used to correlate discovered `Agent` CRs back to an `InfraEnv`, bare-metal platform only. |
| `SP_ENABLED_PLATFORMS` | `kubevirt,baremetal` | No | Comma-separated list controlling both (a) which provisioning platforms `dispatcher.New` accepts and (b) which platform-specific health checks the SP's health checker runs (`internal/openshift/acmcluster/health/health.go`). |
| `SP_HEALTH_CHECK_TIMEOUT` | `5s` | No | Timeout for this SP's own internal health checks — distinct from the agent-wide `AGENT_HEALTH_CHECK_TIMEOUT` in §1.2. |
| `SP_STATUS_DEBOUNCE_INTERVAL` | `1s` | No | Debounce window before publishing a cluster status update. |
| `SP_STATUS_RESYNC_INTERVAL` | `10m` | No | Resync interval for the cluster status watch. |
| `SP_NATS_PUBLISH_RETRY_MAX` | `3` | No | Max retry attempts when publishing a status event to NATS fails. |
| `SP_NATS_PUBLISH_RETRY_INTERVAL` | `2s` | No | Delay between those retry attempts. |

### 2.4 Storage SP (`storage`)

Package: `internal/openshift/storage/config/config.go`. Manages Kubernetes
`PersistentVolumeClaim`s.

| Env var | Default | Required? | Valid values | Notes |
|---|---|---|---|---|
| `SP_STORAGE_NAMESPACE` | `default` | No | any existing namespace | Namespace for storage workloads; must exist on the cluster if changed. |
| `SP_K8S_DEFAULT_STORAGE_CLASS` | _empty_ | No | any `StorageClass` name | Default `StorageClass` applied when a create request doesn't specify one. Empty means no default is injected (cluster's own default `StorageClass` applies, if any). |
| `SP_K8S_DEFAULT_ACCESS_MODE` | `ReadWriteOnce` | No | `ReadWriteOnce`, `ReadOnlyMany`, `ReadWriteMany`, or empty | Any other value fails config loading (`invalid SP_K8S_DEFAULT_ACCESS_MODE`). |
| `SP_MONITOR_DEBOUNCE_MS` | `500` | No | integer milliseconds | Debounce window for the PVC status-watch monitor. |
| `SP_MONITOR_RESYNC_PERIOD` | `10m` | No | Go duration | Informer resync period. |
| `SP_MONITOR_PUBLISH_MAX_ATTEMPTS` | `5` | No | integer | Max attempts publishing a status update to NATS before giving up on that update. |

### 2.5 Network SP (`network`)

Package: `internal/openshift/network/config/config.go`. Manages Kubernetes network-facing
resources (Services).

| Env var | Default | Required? | Notes |
|---|---|---|---|
| `SP_NETWORK_NAMESPACE` | `default` | No | Namespace for network workloads; must exist on the cluster if changed. |
| `SP_MONITOR_DEBOUNCE_MS` | `500` | No | Debounce window for the network status-watch monitor. |
| `SP_MONITOR_RESYNC_PERIOD` | `10m` | No | Informer resync period. |
| `SP_MONITOR_PUBLISH_MAX_ATTEMPTS` | `5` | No | Max attempts publishing a status update to NATS. |

> ℹ️ **Kind manifest gap:** unlike `container` and `vm`, `deploy/k8s/environment-agent.yaml` has
> no example env vars for `network` (nor for `cluster`/`storage`, which also aren't demoed
> there — that manifest is a minimal Kind quick-start, not full coverage). `deploy/.env.example`
> and `deploy/compose.yaml` now wire `network` like the other Kubernetes-namespace-scoped SPs;
> add it to `AGENT_EMBEDDED_SPS` and set `SP_NETWORK_NAMESPACE` if you need it there too.

## Further reading

- [deploy/DEPLOY.md](../DEPLOY.md) — deployment models, quick-start commands, `make` targets
- [compose-kind.md](compose-kind.md) — agent on host (compose) + workloads on Kind
- [in-cluster.md](in-cluster.md) — agent as a Pod on the workload cluster, RBAC, teardown,
  troubleshooting
- [control-plane deploy/docs/environment-agent-kind.md](https://github.com/dcm-project/control-plane/blob/main/deploy/docs/environment-agent-kind.md) — full control-plane + agent stack
- `README.md` — hybrid embedded/external SP model overview, control-plane authentication
- `.ai/specs/environment-agent.spec.md` (REQ-SPR-010–REQ-SPR-222) — normative embedded SP
  registration requirements referenced in §1.1
