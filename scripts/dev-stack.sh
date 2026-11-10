#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="${ROOT_DIR}/frontend"
HEALTH_HOST="${PMHFT_DEV_BACKEND_HOST:-127.0.0.1}"
HEALTH_PORT="${PMHFT_DEV_BACKEND_PORT:-3000}"
RUNTIME_HEALTH_FILE="${ROOT_DIR}/data/runtime/health.json"
LOCK_FILE="${ROOT_DIR}/.dev-stack.lock"
STATE_FILE="${ROOT_DIR}/.dev-stack.state"
SANITIZED_NATIVE_ENV=(
  env
  -u CC
  -u CXX
  -u CC_FOR_BUILD
  -u CXX_FOR_BUILD
  -u CFLAGS
  -u CXXFLAGS
  -u CPPFLAGS
  -u LDFLAGS
  -u LIBRARY_PATH
  -u LD_LIBRARY_PATH
  -u CPATH
  -u C_INCLUDE_PATH
  -u CPLUS_INCLUDE_PATH
  -u PKG_CONFIG_PATH
  -u PKG_CONFIG_LIBDIR
  -u PKG_CONFIG_SYSROOT_DIR
  -u OPENSSL_DIR
  -u OPENSSL_LIB_DIR
  -u OPENSSL_INCLUDE_DIR
)

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required but was not found in PATH" >&2
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "error: npm is required but was not found in PATH" >&2
  exit 1
fi

if [[ ! -d "${FRONTEND_DIR}" ]]; then
  echo "error: frontend directory not found at ${FRONTEND_DIR}" >&2
  exit 1
fi

if ! command -v flock >/dev/null 2>&1; then
  echo "error: flock is required but was not found in PATH" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required but was not found in PATH" >&2
  exit 1
fi

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "error: another dev stack is already running for this repo." >&2
  echo "run: make dev-stop   (or stop the existing stack) and retry." >&2
  exit 1
fi

listener_on_backend_port() {
  if ! command -v ss >/dev/null 2>&1; then
    return 1
  fi

  ss -ltnp 2>/dev/null | awk -v p=":${HEALTH_PORT}" '
    $1 == "LISTEN" && $4 ~ p "$" { print; found=1 }
    END { if (!found) exit 1 }
  '
}

health_probe_ok() {
  [[ -f "$RUNTIME_HEALTH_FILE" ]] || return 1
  jq -e --argjson startup_ts_ms "$BACKEND_START_TS_MS" '
    (.status == "running" or .status == "pause")
    and (.updated_at_ms | type == "number")
    and (.updated_at_ms >= $startup_ts_ms)
  ' "$RUNTIME_HEALTH_FILE" >/dev/null 2>&1
}

if listener_info="$(listener_on_backend_port)"; then
  echo "error: backend port ${HEALTH_PORT} is already in use before startup:" >&2
  echo "${listener_info}" >&2
  echo "stop the existing process (or change PMHFT_DEV_BACKEND_PORT) and retry." >&2
  exit 1
fi

if [[ ! -d "${FRONTEND_DIR}/node_modules" ]]; then
  echo "frontend deps missing: running npm install in frontend/"
  (cd "${FRONTEND_DIR}" && npm install)
fi

DEV_STDOUT_LOGS="${PMHFT_DEV_STDOUT_LOGS:-true}"

if ! command -v setsid >/dev/null 2>&1; then
  echo "error: setsid is required but was not found in PATH" >&2
  exit 1
fi

if [[ "${DEV_STDOUT_LOGS}" == "true" ]]; then
  echo "starting Rust engine with stdout logs enabled (cargo run --bin trader)"
else
  echo "starting Rust engine with stdout logs disabled (cargo run --bin trader)"
fi
setsid "${SANITIZED_NATIVE_ENV[@]}" bash -lc "cd '${ROOT_DIR}' && PMHFT__OBSERVABILITY__LOGGING__STDOUT='${DEV_STDOUT_LOGS}' cargo run --bin trader" &
BACKEND_START_TS_MS="$(date +%s%3N)"
RUST_PID=$!
RUST_PGID="$(ps -o pgid= -p "${RUST_PID}" 2>/dev/null | tr -d '[:space:]' || true)"
UI_PID=""
UI_PGID=""
CLEANED_UP=0

cleanup() {
  if [[ "${CLEANED_UP}" -eq 1 ]]; then
    return 0
  fi
  CLEANED_UP=1

  set +e
  if ! bash "${ROOT_DIR}/scripts/dev-stop.sh"; then
    echo "dev-stack: orderly shutdown was not admitted; trader remains running" >&2
    return 1
  fi

  if [[ -n "${UI_PID:-}" ]]; then
    wait "${UI_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${RUST_PID:-}" ]]; then
    wait "${RUST_PID}" >/dev/null 2>&1 || true
  fi

  rm -f "${LOCK_FILE}" >/dev/null 2>&1 || true
  rm -f "${STATE_FILE}" >/dev/null 2>&1 || true
}

on_signal() {
  local sig="$1"
  echo "received ${sig}, shutting down dev stack" >&2
  cleanup
  exit 130
}

trap cleanup EXIT
trap 'on_signal INT' INT
trap 'on_signal TERM' TERM
trap 'on_signal HUP' HUP
trap 'on_signal QUIT' QUIT
trap 'on_signal TSTP' TSTP

wait_for_backend() {
  local max_wait_secs="${PMHFT_DEV_BACKEND_WAIT_SECS:-90}"
  local deadline=$((SECONDS + max_wait_secs))

  echo "waiting for backend runtime files (${RUNTIME_HEALTH_FILE})"
  while (( SECONDS < deadline )); do
    if ! kill -0 "${RUST_PID}" >/dev/null 2>&1; then
      echo "error: Rust engine exited before backend became healthy" >&2
      return 1
    fi

    if listener_on_backend_port >/dev/null 2>&1 && health_probe_ok; then
      echo "backend runtime files are live and port ${HEALTH_PORT} is listening"
      return 0
    fi

    sleep 1
  done

  echo "error: backend did not publish fresh runtime state within ${max_wait_secs}s" >&2
  if listener_info="$(listener_on_backend_port)"; then
    echo "backend port listener at timeout:" >&2
    echo "${listener_info}" >&2
  fi
  return 1
}

if ! wait_for_backend; then
  kill "${RUST_PID}" >/dev/null 2>&1 || true
  wait "${RUST_PID}" >/dev/null 2>&1 || true
  exit 1
fi

cat >"${STATE_FILE}" <<EOF
ROOT_DIR=${ROOT_DIR}
HEALTH_PORT=${HEALTH_PORT}
RUST_PID=${RUST_PID}
RUST_PGID=${RUST_PGID}
UI_PID=${UI_PID}
UI_PGID=${UI_PGID}
EOF

echo "starting Poly-updown UI (cd frontend && npm run dev)"
setsid bash -lc "cd '${FRONTEND_DIR}' && npm run dev" &
UI_PID=$!
UI_PGID="$(ps -o pgid= -p "${UI_PID}" 2>/dev/null | tr -d '[:space:]' || true)"

cat >"${STATE_FILE}" <<EOF
ROOT_DIR=${ROOT_DIR}
HEALTH_PORT=${HEALTH_PORT}
RUST_PID=${RUST_PID}
RUST_PGID=${RUST_PGID}
UI_PID=${UI_PID}
UI_PGID=${UI_PGID}
EOF

wait -n "${RUST_PID}" "${UI_PID}"
EXIT_CODE=$?

if [[ ${EXIT_CODE} -ne 0 ]]; then
  echo "dev stack exited with code ${EXIT_CODE}" >&2
fi

exit ${EXIT_CODE}
