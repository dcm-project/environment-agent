#!/usr/bin/env bash
# Deploy environment-agent on the current kubectl/OpenShift cluster (in-cluster auth).
# Bundled NATS is off by default; set K8S_DEPLOY_NATS=1 for a standalone in-cluster stack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${ROOT}/deploy/k8s"

TAG="${ENVIRONMENT_AGENT_VERSION:-main}"
IMAGE="${CONTAINER_IMAGE_NAME:-quay.io/dcm-project/environment-agent}:${TAG}"
BUILD_IMAGE="${BUILD_IMAGE:-1}"
K8S_DEPLOY_NATS="${K8S_DEPLOY_NATS:-0}"

UTILITIES_DIR="${UTILITIES_DIR:-${ROOT}/../utilities}"
# shellcheck disable=SC1091
source "${UTILITIES_DIR}/scripts/kind/kind-env.sh"

CLUSTER_TYPE="kubernetes"
KIND_CLUSTER=""

if kind_try_resolve_from_context; then
	CLUSTER_TYPE="kind"
	KIND_CLUSTER="${KIND_CONTEXT#kind-}"
elif kubectl api-resources -o name 2>/dev/null | grep -qE '^routes\.route\.openshift\.io$'; then
	CLUSTER_TYPE="openshift"
elif command -v oc >/dev/null 2>&1 && oc api-resources -o name 2>/dev/null | grep -qE '^routes\.route\.openshift\.io$'; then
	CLUSTER_TYPE="openshift"
fi

CTX="$(kubectl config current-context 2>/dev/null || true)"
echo "==> Cluster type: ${CLUSTER_TYPE} (context: ${CTX:-<none>})"

if [[ -z "${LOAD_INTO_KIND:-}" ]]; then
	LOAD_INTO_KIND="$([[ "${CLUSTER_TYPE}" == "kind" ]] && echo 1 || echo 0)"
fi

if [[ "${BUILD_IMAGE}" == "1" ]]; then
	echo "==> Building ${IMAGE}"
	make -C "${ROOT}" image-build CONTAINER_IMAGE_TAG="${TAG}"
fi

if [[ "${LOAD_INTO_KIND}" == "1" ]] && [[ -n "${KIND_CLUSTER}" ]] && command -v kind >/dev/null 2>&1; then
	echo "==> Loading ${IMAGE} into kind cluster ${KIND_CLUSTER}"
	kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}"
elif [[ "${CLUSTER_TYPE}" == "openshift" ]] && [[ "${BUILD_IMAGE}" == "1" ]]; then
	echo "==> OpenShift: push ${IMAGE} to a registry this cluster can pull, or skip the local build"
	echo "    Example: BUILD_IMAGE=0 make k8s-deploy"
	echo "    Continuing with manifest apply — image must be pullable or the Deployment will fail."
fi

apply_manifests() {
	local work_dir
	work_dir=$(mktemp -d)
	# shellcheck disable=SC2064
	trap "rm -rf '${work_dir}'" RETURN
	cp -a "${K8S_DIR}/." "${work_dir}/"

	if [[ "${K8S_DEPLOY_NATS}" == "1" ]]; then
		if ! grep -q 'nats.yaml' "${work_dir}/kustomization.yaml"; then
			sed -i '/^  - namespace.yaml$/a\  - nats.yaml' "${work_dir}/kustomization.yaml"
		fi
	fi

	if command -v kustomize >/dev/null 2>&1; then
		(cd "${work_dir}" && kustomize edit set image "quay.io/dcm-project/environment-agent=${IMAGE}")
		kubectl apply -k "${work_dir}"
		return
	fi

	echo "warning: kustomize not found; applying with sed image substitution" >&2
	kubectl kustomize "${work_dir}" \
		| sed "s|image: quay.io/dcm-project/environment-agent:.*|image: ${IMAGE}|g" \
		| kubectl apply -f -
}

echo "==> Applying manifests (image: ${IMAGE}, bundled NATS: ${K8S_DEPLOY_NATS})"
apply_manifests

if [[ "${K8S_DEPLOY_NATS}" == "1" ]]; then
	echo "==> Waiting for NATS"
	kubectl -n dcm wait --for=condition=available deployment/nats --timeout=120s

	echo "==> JetStream streams (nats-init)"
	kubectl -n dcm delete job nats-init --ignore-not-found
	kubectl apply -f "${K8S_DIR}/nats-init-job.yaml"
	kubectl -n dcm wait --for=condition=complete job/nats-init --timeout=180s
else
	echo "==> Skipping bundled NATS (using platform NATS; set K8S_DEPLOY_NATS=1 to deploy in-cluster NATS)"
fi

echo "==> Waiting for environment-agent"
kubectl -n dcm rollout status deployment/environment-agent --timeout=180s

echo ""
if bash "${SCRIPT_DIR}/k8s-host-urls.sh" check; then
	echo ""
	echo "  make k8s-verify"
	echo "  make k8s-publish-creates"
else
	echo ""
	if [[ "${CLUSTER_TYPE}" == "openshift" ]]; then
		echo "  OpenShift: see deploy/docs/in-cluster.md for platform NATS / DCM URLs (oc get svc)"
	else
		echo "  fix host access (see deploy/docs/in-cluster.md), then: make k8s-verify"
	fi
fi
