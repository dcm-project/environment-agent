#!/usr/bin/env bash
# Resolve host-reachable agent URL for in-cluster deploy (NodePort on Kind).
set -euo pipefail

AGENT_NODE_PORT="${K8S_AGENT_NODE_PORT:-30081}"

agent_healthy() {
	local base="$1"
	curl -sf --max-time 2 "${base}/api/v1alpha1/health" >/dev/null 2>&1
}

node_internal_ip() {
	kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true
}

resolve_agent_url() {
	local node_ip

	if agent_healthy "http://127.0.0.1:${AGENT_NODE_PORT}"; then
		echo "http://127.0.0.1:${AGENT_NODE_PORT}"
		return 0
	fi

	node_ip="$(node_internal_ip)"
	if [[ -n "${node_ip}" ]] && agent_healthy "http://${node_ip}:${AGENT_NODE_PORT}"; then
		echo "http://${node_ip}:${AGENT_NODE_PORT}"
		return 0
	fi

	return 1
}

usage() {
	echo "usage: $0 {agent|check}" >&2
	exit 1
}

cmd="${1:-check}"

if ! AGENT_URL="$(resolve_agent_url)"; then
	echo "error: agent not reachable on NodePort ${AGENT_NODE_PORT}" >&2
	echo "  kubectl -n dcm get pods,svc" >&2
	node_ip="$(node_internal_ip)"
	if [[ -n "${node_ip}" ]]; then
		echo "  try: curl http://${node_ip}:${AGENT_NODE_PORT}/api/v1alpha1/health" >&2
	fi
	exit 1
fi

case "${cmd}" in
agent) echo "${AGENT_URL}" ;;
check) echo "AGENT_URL=${AGENT_URL}" ;;
*) usage ;;
esac
