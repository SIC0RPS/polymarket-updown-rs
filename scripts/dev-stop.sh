#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCK_FILE="${ROOT_DIR}/.dev-stack.lock"
STATE_FILE="${ROOT_DIR}/.dev-stack.state"
HEALTH_PORT="${PMHFT_DEV_BACKEND_PORT:-3000}"
STOP_GRACE_SECS="${PMHFT_DEV_STOP_GRACE_SECS:-120}"

declare -A TARGETS=()
SELF_PID="$$"
PARENT_PID="${PPID:-}"
SELF_PGID="$(ps -o pgid= -p "${SELF_PID}" 2>/dev/null | tr -d '[:space:]' || true)"
RUNTIME_GUARD_REQUIRED=0

collect_backend_listener_pids() {
  if ! command -v ss >/dev/null 2>&1; then
    return 0
  fi

  # Extract pids for LISTEN sockets on the configured backend port.
  ss -ltnp 2>/dev/null \
    | awk -v p=":${HEALTH_PORT}" '
      $1 == "LISTEN" && $4 ~ p "$" {
        while (match($0, /pid=[0-9]+/)) {
          print substr($0, RSTART + 4, RLENGTH - 4);
          $0 = substr($0, RSTART + RLENGTH);
        }
      }
    ' \
    | sort -u
}

add_target_pid() {
  local pid="${1:-}"
  [[ -z "${pid}" ]] && return 0
  [[ "${pid}" == "${SELF_PID}" ]] && return 0
  [[ -n "${PARENT_PID}" && "${pid}" == "${PARENT_PID}" ]] && return 0
  [[ ! -d "/proc/${pid}" ]] && return 0
  local pgid=""
  pgid="$(ps -o pgid= -p "${pid}" 2>/dev/null | tr -d '[:space:]' || true)"
  [[ -n "${SELF_PGID}" && "${pgid}" == "${SELF_PGID}" ]] && return 0
  TARGETS["${pid}"]=1
}

load_state_targets() {
  [[ -f "${STATE_FILE}" ]] || return 0
  local rust_pid="" rust_pgid="" ui_pid="" ui_pgid=""
  while IFS='=' read -r key value; do
    case "${key}" in
      RUST_PID) rust_pid="${value}" ;;
      RUST_PGID) rust_pgid="${value}" ;;
      UI_PID) ui_pid="${value}" ;;
      UI_PGID) ui_pgid="${value}" ;;
    esac
  done < "${STATE_FILE}"

  if [[ -n "${rust_pid}" && -d "/proc/${rust_pid}" ]]; then
    RUNTIME_GUARD_REQUIRED=1
    add_target_pid "${rust_pid}"
  fi
  add_target_pid "${ui_pid}"

  for pgid in "${rust_pgid}" "${ui_pgid}"; do
    [[ -z "${pgid}" ]] && continue
    while IFS= read -r pid; do
      add_target_pid "${pid}"
    done < <(pgrep -g "${pgid}" 2>/dev/null || true)
  done
}

is_repo_trader_process() {
  local pid="$1"
  local cmd="$2"
  local cwd="$3"
  local exe=""
  exe="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"

  [[ "${exe}" == "${ROOT_DIR}"/target/*/trader ]] && return 0
  [[ "${cmd}" == *"cargo run --bin trader"* && "${cwd}" == "${ROOT_DIR}"* ]] && return 0
  [[ "${cmd}" == *"target/"*"/trader"* && "${cwd}" == "${ROOT_DIR}"* ]] && return 0
  [[ "${cmd}" == "trader"* && "${cwd}" == "${ROOT_DIR}"* ]] && return 0
  return 1
}

load_state_targets

while IFS= read -r pid; do
  pid="${pid//[[:space:]]/}"
  [[ -z "${pid}" ]] && continue
  [[ "${pid}" == "${SELF_PID}" ]] && continue
  [[ -n "${PARENT_PID}" && "${pid}" == "${PARENT_PID}" ]] && continue
  [[ ! -d "/proc/${pid}" ]] && continue

  cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
  cwd="$(readlink -f "/proc/${pid}/cwd" 2>/dev/null || true)"

  if [[ "${cmd}" == *"scripts/dev-stop.sh"* ]]; then
    continue
  fi
  if is_repo_trader_process "${pid}" "${cmd}" "${cwd}"; then
    RUNTIME_GUARD_REQUIRED=1
    add_target_pid "${pid}"
    continue
  fi
  if [[ "${cmd}" == *"${ROOT_DIR}/frontend/node_modules/.bin/vite"* ]]; then
    add_target_pid "${pid}"
    continue
  fi
  if [[ "${cmd}" == *"scripts/dev-stack.sh"* && "${cwd}" == "${ROOT_DIR}"* ]]; then
    add_target_pid "${pid}"
    continue
  fi
  if [[ "${cmd}" == *"npm run dev"* && "${cwd}" == "${ROOT_DIR}/frontend"* ]]; then
    add_target_pid "${pid}"
    continue
  fi
  if [[ "${cmd}" == *"npm exec vite"* && "${cwd}" == "${ROOT_DIR}/frontend"* ]]; then
    add_target_pid "${pid}"
    continue
  fi
  if [[ "${cmd}" == *"sh -c vite"* && "${cwd}" == "${ROOT_DIR}/frontend"* ]]; then
    add_target_pid "${pid}"
    continue
  fi
done < <(ps -eo pid=)

# Always include any process currently binding the configured backend port.
while IFS= read -r pid; do
  [[ -z "${pid}" ]] && continue
  [[ "${pid}" == "${SELF_PID}" ]] && continue
  RUNTIME_GUARD_REQUIRED=1
  add_target_pid "${pid}"
done < <(collect_backend_listener_pids)

if [[ "${#TARGETS[@]}" -eq 0 ]]; then
  echo "no Polymarket dev-stack processes found"
  exit 0
fi

operator_url="http://127.0.0.1:${HEALTH_PORT}/api/command"
deadline=$((SECONDS + STOP_GRACE_SECS))

if [[ "${RUNTIME_GUARD_REQUIRED}" -eq 1 ]]; then
  curl --silent --show-error --fail \
    --header 'content-type: application/json' \
    --data '{"command":"pause","enabled":true}' \
    "${operator_url}" >/dev/null

  while ! PYTHONDONTWRITEBYTECODE=1 python3 \
    "${ROOT_DIR}/scripts/runtime_stop_guard.py" --root "${ROOT_DIR}"; do
    if (( SECONDS >= deadline )); then
      echo "dev-stop: safety conditions did not clear within ${STOP_GRACE_SECS}s; trader remains running" >&2
      exit 1
    fi
    sleep 0.5
  done

  while ! curl --silent --fail \
    --header 'content-type: application/json' \
    --data '{"command":"shutdown"}' \
    "${operator_url}" >/dev/null; do
    if (( SECONDS >= deadline )); then
      echo "dev-stop: orderly shutdown was not admitted within ${STOP_GRACE_SECS}s; trader remains running" >&2
      exit 1
    fi
    sleep 0.5
  done

  while :; do
    trader_alive=0
    for pid in "${!TARGETS[@]}"; do
      [[ -d "/proc/${pid}" ]] || continue
      cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
      cwd="$(readlink -f "/proc/${pid}/cwd" 2>/dev/null || true)"
      if is_repo_trader_process "${pid}" "${cmd}" "${cwd}"; then
        trader_alive=1
        break
      fi
    done
    if [[ "${trader_alive}" -eq 0 ]]; then
      break
    fi
    if (( SECONDS >= deadline )); then
      echo "dev-stop: trader teardown exceeded ${STOP_GRACE_SECS}s; trader remains running" >&2
      exit 1
    fi
    sleep 0.5
  done
fi

echo "stopping remaining Polymarket dev-stack processes..."
for pid in "${!TARGETS[@]}"; do
  [[ -d "/proc/${pid}" ]] || continue
  cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
  cwd="$(readlink -f "/proc/${pid}/cwd" 2>/dev/null || true)"
  if is_repo_trader_process "${pid}" "${cmd}" "${cwd}"; then
    echo "dev-stop: refusing to signal resident trader pid ${pid}" >&2
    exit 1
  fi
  kill -TERM "${pid}" >/dev/null 2>&1 || true
done

while (( SECONDS < deadline )); do
  remaining=0
  for pid in "${!TARGETS[@]}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      remaining=1
      break
    fi
  done
  [[ "${remaining}" -eq 0 ]] && break
  sleep 0.5
done

for pid in "${!TARGETS[@]}"; do
  [[ -d "/proc/${pid}" ]] || continue
  cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
  cwd="$(readlink -f "/proc/${pid}/cwd" 2>/dev/null || true)"
  if is_repo_trader_process "${pid}" "${cmd}" "${cwd}"; then
    echo "dev-stop: refusing to kill resident trader pid ${pid}" >&2
    exit 1
  fi
  kill -KILL "${pid}" >/dev/null 2>&1 || true
done

rm -f "${LOCK_FILE}"
rm -f "${STATE_FILE}"
echo "done"
