# Shared Kind cluster detection for deploy scripts.
# Uses kubectl current-context (must be kind-<cluster-name>).

KIND_NODE=""
KIND_CONTEXT=""

kind_resolve_from_context() {
	if ! command -v kubectl >/dev/null 2>&1; then
		echo "Error: kubectl is required to detect the Kind cluster" >&2
		return 1
	fi

	local ctx
	ctx="$(kubectl config current-context 2>/dev/null || true)"
	if [[ -z "${ctx}" ]]; then
		echo "Error: no kubectl current-context; set one with kubectl config use-context kind-<cluster-name>" >&2
		return 1
	fi

	if [[ "${ctx}" =~ ^kind-(.*)$ ]]; then
		local name="${BASH_REMATCH[1]}"
		KIND_NODE="${name}-control-plane"
		KIND_CONTEXT="${ctx}"
		return 0
	fi

	echo "Error: kubectl current-context must be a Kind context (kind-<cluster-name>)" >&2
	echo "  Current: ${ctx}" >&2
	echo "  Example: kubectl config use-context kind-dcm-local" >&2
	return 1
}

# Pick the container engine that can see both the Kind node and the compose network.
# Kind and compose must use the same runtime (docker vs podman).
kind_pick_engine() {
	local node="$1"
	local network="$2"

	try_engine() {
		local candidate="$1"
		command -v "${candidate}" >/dev/null 2>&1 \
			&& "${candidate}" inspect "${node}" >/dev/null 2>&1 \
			&& "${candidate}" network inspect "${network}" >/dev/null 2>&1
	}

	if [[ -n "${CONTAINER_ENGINE:-}" ]] && try_engine "${CONTAINER_ENGINE}"; then
		return 0
	fi

	if [[ -n "${CONTAINER_ENGINE:-}" ]]; then
		echo "warning: CONTAINER_ENGINE=${CONTAINER_ENGINE} cannot reach Kind node and compose network; auto-detecting runtime" >&2
	fi

	for candidate in podman docker; do
		if try_engine "${candidate}"; then
			CONTAINER_ENGINE="${candidate}"
			return 0
		fi
	done

	local kind_engine=""
	for candidate in podman docker; do
		if command -v "${candidate}" >/dev/null 2>&1 \
			&& "${candidate}" inspect "${node}" >/dev/null 2>&1; then
			kind_engine="${candidate}"
			break
		fi
	done

	echo "Error: cannot connect Kind node '${node}' to compose network '${network}'." >&2
	echo "  kubectl context: ${KIND_CONTEXT}" >&2
	if [[ -n "${kind_engine}" ]]; then
		echo "  Kind node is on ${kind_engine}, but '${network}' was not found on that runtime." >&2
		echo "  Recreate the stack with the same runtime (e.g. COMPOSE=\"docker compose\" CONTAINER_ENGINE=docker make compose-up)." >&2
	else
		echo "  Start compose first (make compose-up), or ensure the Kind node '${node}' exists." >&2
	fi
	return 1
}
