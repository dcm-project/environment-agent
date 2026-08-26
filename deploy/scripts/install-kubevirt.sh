#!/usr/bin/env bash
# Install KubeVirt on the Kind cluster (required for the embedded vm SP).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-env.sh
source "${SCRIPT_DIR}/kind-env.sh"

KUBEVIRT_VERSION="${KUBEVIRT_VERSION:-v1.5.0}"

if ! command -v kubectl >/dev/null 2>&1; then
	echo "Error: kubectl is required" >&2
	exit 1
fi

echo "Installing KubeVirt ${KUBEVIRT_VERSION} on context ${KIND_CONTEXT}"
kubectl --context "${KIND_CONTEXT}" apply -f "https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}/kubevirt-operator.yaml"
kubectl --context "${KIND_CONTEXT}" apply -f "https://github.com/kubevirt/kubevirt/releases/download/${KUBEVIRT_VERSION}/kubevirt-cr.yaml"

echo "Waiting for KubeVirt to become available..."
kubectl --context "${KIND_CONTEXT}" -n kubevirt wait kv kubevirt --for=condition=Available --timeout=300s
echo "KubeVirt is ready."
