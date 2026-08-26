#!/usr/bin/env bash
# Basic health checks for the agent.
set -euo pipefail

AGENT_URL="${AGENT_URL:-http://localhost:8081}"

curl_health() {
	curl -sf --max-time 5 "${AGENT_URL}/api/v1alpha1/health"
}

if ! curl_health >/dev/null 2>&1; then
	echo "error: agent not reachable at ${AGENT_URL}" >&2
	if command -v docker >/dev/null 2>&1; then
		status="$(docker ps -a --filter name=environment-agent --format '{{.Names}} {{.Status}}' 2>/dev/null | head -1)"
		if [[ -n "${status}" ]]; then
			echo "  container: ${status}" >&2
			name="${status%% *}"
			echo "  logs: docker logs ${name}" >&2
		fi
	fi
	if command -v podman >/dev/null 2>&1; then
		status="$(podman ps -a --filter name=environment-agent --format '{{.Names}} {{.Status}}' 2>/dev/null | head -1)"
		if [[ -n "${status}" ]]; then
			echo "  container: ${status}" >&2
			name="${status%% *}"
			echo "  logs: podman logs ${name}" >&2
		fi
	fi
	echo "  hint: if logs show 'permission denied' on /kubeconfig, use deploy/.kube/config (make kubeconfig-for-compose) or chmod o+r your kubeconfig file" >&2
	exit 1
fi

echo "==> Agent health (${AGENT_URL})"
curl_health | head -c 500
echo ""

echo "==> Registered providers (${AGENT_URL})"
curl -sf --max-time 5 "${AGENT_URL}/api/v1alpha1/providers" | head -c 1000
echo ""
