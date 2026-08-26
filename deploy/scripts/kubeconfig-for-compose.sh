#!/usr/bin/env bash
# Generate a kubeconfig that works from compose containers on the shared network.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-env.sh
source "${SCRIPT_DIR}/kind-env.sh"

OUT_DIR="${ROOT}/deploy/.kube"
OUT_FILE="${OUT_DIR}/config"
API_ALIAS="${KIND_NETWORK_ALIAS:-kubernetes}"

mkdir -p "${OUT_DIR}"

if ! command -v kubectl >/dev/null 2>&1; then
	echo "Error: kubectl is required" >&2
	exit 1
fi

kubectl config view --minify --flatten --context "${KIND_CONTEXT}" \
	| sed -E "s|https://[^:]+:[0-9]+|https://${API_ALIAS}:6443|" \
	> "${OUT_FILE}"

chmod 644 "${OUT_FILE}"

echo "Wrote ${OUT_FILE} (context: ${KIND_CONTEXT})"
echo "Export for compose: AGENT_KUBECONFIG_HOST=${OUT_FILE}"
