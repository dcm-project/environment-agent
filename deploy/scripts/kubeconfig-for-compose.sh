#!/usr/bin/env bash
# Write deploy/.kube/config for compose (API URL https://kubernetes:6443). Run before compose-up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=kind-env.sh
source "${SCRIPT_DIR}/kind-env.sh"
kind_resolve_from_context || exit 1

ALIAS="${KIND_NETWORK_ALIAS:-kubernetes}"
OUT_DIR="${ROOT}/deploy/.kube"
OUT_FILE="${OUT_DIR}/config"

if [[ "$(id -u)" -eq 0 ]]; then
	echo "Error: do not run with sudo — kubectl will use root's kubeconfig, not yours." >&2
	exit 1
fi

if [[ -e "${OUT_DIR}" ]] && [[ ! -w "${OUT_DIR}" ]]; then
	echo "Error: ${OUT_DIR} is not writable (often root-owned from an earlier compose run)." >&2
	echo "  Fix: sudo rm -rf ${OUT_DIR}" >&2
	exit 1
fi

mkdir -p "${OUT_DIR}"
if [[ -d "${OUT_FILE}" ]]; then
	if [[ -z "$(ls -A "${OUT_FILE}")" ]]; then
		echo "Removing empty directory ${OUT_FILE}"
		rm -rf "${OUT_FILE}"
	else
		echo "Error: ${OUT_FILE} is a directory — remove it manually: rm -rf ${OUT_FILE}" >&2
		exit 1
	fi
fi

kubectl config view --minify --flatten --context "${KIND_CONTEXT}" \
	| sed -E "s|https://[^:]+:[0-9]+|https://${ALIAS}:6443|" \
	> "${OUT_FILE}"
chmod 644 "${OUT_FILE}"

echo "Wrote ${OUT_FILE} (context: ${KIND_CONTEXT})"
