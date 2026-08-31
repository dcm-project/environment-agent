#!/usr/bin/env bash
# Resolve host-reachable URLs for the in-cluster stack (NodePort on Kind).
set -euo pipefail

AGENT_NODE_PORT="${K8S_AGENT_NODE_PORT:-30081}"
NATS_NODE_PORT="${K8S_NATS_NODE_PORT:-30422}"

agent_healthy() {
	local base="$1"
	curl -sf --max-time 2 "${base}/api/v1alpha1/health" >/dev/null 2>&1
}

node_internal_ip() {
	kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || true
}

resolve_agent_url() {
	local base node_ip

	# Docker Desktop Kind sometimes exposes NodePorts on localhost.
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

resolve_nats_url() {
	local agent_url="$1"
	local host

	case "${agent_url}" in
		*127.0.0.1*) echo "nats://127.0.0.1:${NATS_NODE_PORT}"; return 0 ;;
	esac

	host="${agent_url#http://}"
	host="${host%%:*}"
	echo "nats://${host}:${NATS_NODE_PORT}"
}

usage() {
	echo "usage: $0 {agent|nats|export|check}" >&2
	exit 1
}

cmd="${1:-check}"
AGENT_URL=""
NATS_URL=""

if ! AGENT_URL="$(resolve_agent_url)"; then
	echo "error: agent not reachable on NodePort ${AGENT_NODE_PORT}" >&2
	echo "  kubectl -n dcm get pods,svc" >&2
	node_ip="$(node_internal_ip)"
	if [[ -n "${node_ip}" ]]; then
		echo "  try: curl http://${node_ip}:${AGENT_NODE_PORT}/api/v1alpha1/health" >&2
	fi
	exit 1
fi

NATS_URL="$(resolve_nats_url "${AGENT_URL}")"

case "${cmd}" in
agent) echo "${AGENT_URL}" ;;
nats) echo "${NATS_URL}" ;;
export)
	echo "export AGENT_URL=${AGENT_URL}"
	echo "export AGENT_MESSAGING_URL=${NATS_URL}"
	;;
check)
	echo "AGENT_URL=${AGENT_URL}"
	echo "AGENT_MESSAGING_URL=${NATS_URL}"
	;;
*) usage ;;
esac
