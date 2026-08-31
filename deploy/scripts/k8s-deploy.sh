#!/usr/bin/env bash
# Deploy NATS + environment-agent on the current kubectl cluster (in-cluster auth).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${ROOT}/deploy/k8s"

TAG="${ENVIRONMENT_AGENT_VERSION:-dev}"
IMAGE="${CONTAINER_IMAGE_NAME:-quay.io/dcm-project/environment-agent}:${TAG}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
LOAD_INTO_KIND="${LOAD_INTO_KIND:-1}"

# shellcheck source=kind-env.sh
source "${SCRIPT_DIR}/kind-env.sh"
if kind_resolve_from_context; then
	KIND_CLUSTER="${KIND_CONTEXT#kind-}"
else
	KIND_CLUSTER=""
fi

if [[ "${BUILD_IMAGE}" == "1" ]]; then
	echo "==> Building ${IMAGE}"
	make -C "${ROOT}" image-build CONTAINER_IMAGE_TAG="${TAG}"
fi

if [[ "${LOAD_INTO_KIND}" == "1" ]] && [[ -n "${KIND_CLUSTER}" ]] && command -v kind >/dev/null 2>&1; then
	echo "==> Loading ${IMAGE} into kind cluster ${KIND_CLUSTER}"
	kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}"
fi

echo "==> Applying manifests"
kubectl apply -k "${K8S_DIR}"
kubectl -n dcm set image deployment/environment-agent environment-agent="${IMAGE}"

echo "==> Waiting for NATS"
kubectl -n dcm wait --for=condition=available deployment/nats --timeout=120s

echo "==> JetStream streams (nats-init)"
kubectl -n dcm delete job nats-init --ignore-not-found
kubectl apply -f "${K8S_DIR}/nats-init-job.yaml"
kubectl -n dcm wait --for=condition=complete job/nats-init --timeout=180s

echo "==> Waiting for environment-agent"
kubectl -n dcm rollout status deployment/environment-agent --timeout=180s

echo ""
if bash "${SCRIPT_DIR}/k8s-host-urls.sh" check; then
	echo ""
	echo "  make k8s-verify"
	echo "  make k8s-publish-creates"
else
	echo ""
	echo "  fix host access, then: make k8s-verify"
fi
