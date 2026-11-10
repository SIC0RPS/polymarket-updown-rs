#!/usr/bin/env bash
set -euo pipefail

# Full four-symbol retained proof (optimized PnL score is via make optimize, not defaults).
# Fast structural debug (same engine, one/few symbols, does not clobber matrix proof):
#   BACKTEST_STRATEGY_SYMBOLS=sol make backtest-strategy-debug
#   BACKTEST_STRATEGY_SYMBOLS=btc,eth make backtest-strategy-debug

if (($# != 0)); then
  echo "usage: make backtest-strategy | make backtest-strategy-debug" >&2
  exit 2
fi

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lane_lock="${root_dir}/data/runtime/.retained_run_lane.lock"
mkdir -p "$(dirname "${lane_lock}")"
exec 9>"${lane_lock}"
if ! flock -n 9; then
  echo "error: another backtest or optimizer owns the retained-run lane" >&2
  exit 1
fi
export POLYMARKET_OFFLINE_RUN_LANE_FD=9

scripts/cargo_native.sh build --profile dev-live --bin backtest_strategy_matrix
if [[ -z "${BACKTEST_STRATEGY_JOBS:-}" ]]; then
  if command -v nproc >/dev/null 2>&1; then
    export BACKTEST_STRATEGY_JOBS="$(nproc)"
  else
    export BACKTEST_STRATEGY_JOBS="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
  fi
fi
echo "BACKTEST_STRATEGY_JOBS detected=${BACKTEST_STRATEGY_JOBS}"
capture="${BACKTEST_STRATEGY_CAPTURE:-latest}"
learning_seed="${BACKTEST_STRATEGY_ORACLE_LEARNING_SEED:-}"
learning_seed_sha256="${BACKTEST_STRATEGY_ORACLE_LEARNING_SEED_SHA256:-}"
research_variant="${BACKTEST_STRATEGY_ORACLE_RESEARCH_VARIANT:-}"
research_root="${BACKTEST_STRATEGY_ORACLE_RESEARCH_ROOT:-}"
if { [[ -n "${learning_seed}" ]] && [[ -z "${learning_seed_sha256}" ]]; } ||
  { [[ -z "${learning_seed}" ]] && [[ -n "${learning_seed_sha256}" ]]; }; then
  echo "error: BACKTEST_STRATEGY_ORACLE_LEARNING_SEED and BACKTEST_STRATEGY_ORACLE_LEARNING_SEED_SHA256 must be supplied together" >&2
  exit 2
fi
if { [[ -n "${research_variant}" ]] && [[ -z "${research_root}" ]]; } ||
  { [[ -z "${research_variant}" ]] && [[ -n "${research_root}" ]]; }; then
  echo "error: BACKTEST_STRATEGY_ORACLE_RESEARCH_VARIANT and BACKTEST_STRATEGY_ORACLE_RESEARCH_ROOT must be supplied together" >&2
  exit 2
fi
extra_args=()
if [[ -n "${BACKTEST_STRATEGY_SYMBOLS:-}" ]]; then
  echo "BACKTEST_STRATEGY_SYMBOLS=${BACKTEST_STRATEGY_SYMBOLS} (debug subset; not optimized PnL score)"
  extra_args+=(--symbols "${BACKTEST_STRATEGY_SYMBOLS}")
fi
if [[ -n "${learning_seed}" ]]; then
  extra_args+=(
    --oracle-learning-seed "${learning_seed}"
    --oracle-learning-seed-sha256 "${learning_seed_sha256}"
  )
fi
if [[ -n "${research_variant}" ]]; then
  extra_args+=(
    --oracle-research-variant "${research_variant}"
    --oracle-research-root "${research_root}"
  )
fi
exec target/dev-live/backtest_strategy_matrix --capture-session "${capture}" "${extra_args[@]}"
