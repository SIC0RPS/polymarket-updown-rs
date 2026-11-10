#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/backtest_strategy_auto_optimize.sh

Runs the Rust CMA-ES real replay/backtest optimizer through scripts/cargo_native.sh.

Optional:
  BACKTEST_STRATEGY_CAPTURE=<explicit selection capture>
  BACKTEST_STRATEGY_AUTO_OPTIMIZE_JOBS=<explicit emergency override>
  BACKTEST_STRATEGY_AUTO_OPTIMIZE_NO_APPLY=1
  BACKTEST_STRATEGY_OPTIMIZER_SPEC=<explicit optimizer specification>

Default specification: optimization.buy.yml when oracle_buy_and_hold=true;
otherwise optimization.yml, selected from the run's frozen configuration.
USAGE
}

if (($# > 0)); then
  case "$1" in
    --help | -h)
      usage
      exit 0
      ;;
  esac
  usage
  exit 2
fi

bool_enabled() {
  case "${1:-}" in
    1 | true | TRUE | yes | YES | on | ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

selection_capture="${BACKTEST_STRATEGY_CAPTURE:-}"
capture_args=()
capture_policy=latest_strict_pair
if [[ -n "${selection_capture}" ]]; then
  capture_args=(--selection-capture "${selection_capture}")
  capture_policy=explicit_selection_random_other
fi

echo "AUTO_OPTIMIZE_SCOPE mode=sequential_symbol_searches order=BTC,ETH,SOL,XRP final_validation=frozen_participating_symbol_pair capture_policy=${capture_policy} no_apply=${BACKTEST_STRATEGY_AUTO_OPTIMIZE_NO_APPLY:-0}"

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lane_lock="${root_dir}/data/runtime/.retained_run_lane.lock"
mkdir -p "$(dirname "${lane_lock}")"
exec 9>"${lane_lock}"
if ! flock -n 9; then
  echo "error: another backtest or optimizer owns the retained-run lane" >&2
  exit 1
fi
export POLYMARKET_OFFLINE_RUN_LANE_FD=9

bin=target/release/backtest_auto_optimize
needs_build=1
if [[ -x "${bin}" ]] && ! bool_enabled "${BACKTEST_STRATEGY_AUTO_OPTIMIZE_FORCE_BUILD:-}"; then
  newer_source="$(
    find Cargo.toml Cargo.lock src crates \
      -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' \) \
      -newer "${bin}" -print -quit
  )"
  if [[ -z "${newer_source}" ]]; then
    needs_build=0
  fi
fi
if ((needs_build)); then
  scripts/cargo_native.sh build --release --bin backtest_auto_optimize
else
  echo "AUTO_OPTIMIZE_BUILD skip fresh_binary=${bin}"
fi

optimizer_spec="${BACKTEST_STRATEGY_OPTIMIZER_SPEC:-}"
spec_args=()
if [[ -n "${optimizer_spec}" ]]; then
  spec_args=(--spec "${optimizer_spec}")
fi
exec "${bin}" "${spec_args[@]}" "${capture_args[@]}"
