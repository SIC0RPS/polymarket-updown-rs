SHELL := /bin/bash
FRONTEND_PORT ?= 5188
FRONTEND_LOG_MAX_BYTES ?= 10485760
TRADER_DEV_PROFILE ?= dev-live
TRADER_DEV_BIN := ./target/$(TRADER_DEV_PROFILE)/trader
TRADER_RELEASE_PROFILE ?= release-hft
TRADER_RELEASE_BIN := ./target/$(TRADER_RELEASE_PROFILE)/trader
CARGO_CMD := scripts/cargo_native.sh
BACKTEST_STRATEGY_CAPTURE ?= latest
unexport CC CXX CC_FOR_BUILD CXX_FOR_BUILD CFLAGS CXXFLAGS CPPFLAGS LDFLAGS LIBRARY_PATH LD_LIBRARY_PATH CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH PKG_CONFIG_PATH PKG_CONFIG_LIBDIR PKG_CONFIG_SYSROOT_DIR OPENSSL_DIR OPENSSL_LIB_DIR OPENSSL_INCLUDE_DIR

.PHONY: help setup fmt fmt-check size-check lint test check check-fast test-runtime test-contracts test-surface deny run dev dev-stop release release-stop build build-release logs logs-all status ci clean clean-heavy-artifacts backtest-strategy backtest-strategy-debug optimize backtest-strategy-auto-optimize

help:
	@echo "Common workflow:"
	@echo "  make backtest-strategy            - full four-symbol retained proof (engine CTF table)"
	@echo "  make backtest-strategy-debug      - same engine, one symbol (default SOL); structural issues in minutes"
	@echo "  make optimize                     - one joint BTC/ETH/SOL/XRP CMA-ES search and validation report"
	@echo ""
	@echo "Targets:"
	@echo "  dev        - build optimized dev trader + frontend (full stack)"
	@echo "  dev-stop   - kill trader + frontend"
	@echo "  release    - build release-hft trader + frontend (full stack)"
	@echo "  release-stop - kill trader + frontend"
	@echo "  build      - cargo build --release --bin trader"
	@echo "  logs       - tail process/runtime.log"
	@echo "  logs-all   - tail runtime + frontend logs"
	@echo "  status     - check if trader/vite running + runtime files"
	@echo "  setup      - install local git hooks"
	@echo "  fmt        - format code"
	@echo "  fmt-check  - verify formatting"
	@echo "  size-check - enforce production Rust line-count ratchet"
	@echo "  lint       - run clippy with warnings denied"
	@echo "  check-fast - size-check + cargo build --profile dev-live --bin trader"
	@echo "  test-runtime - runtime control + runtime file tests only"
	@echo "  test-contracts - execution, hold-to-resolution, portfolio, risk contract tests"
	@echo "  test-surface - minimal dashboard/ws smoke"
	@echo "  backtest-strategy - full four-symbol retained capture proof + engine CTF table"
	@echo "  backtest-strategy-debug - BACKTEST_STRATEGY_SYMBOLS (default sol); structural only, not PnL score"
	@echo "  optimize - one-step Rust CMA-ES optimizer using real replay/backtest scoring"
	@echo "  backtest-strategy-auto-optimize - optimizer implementation target used by make optimize"
	@echo "  test       - run full tests"
	@echo "  check      - cargo check"
	@echo "  deny       - supply-chain checks (cargo-deny)"
	@echo "  run        - run trader binary"
	@echo "  build-release        - alias for build"
	@echo "  ci         - fmt-check + size-check + lint + test + deny"
	@echo "  clean      - remove build artifacts"
	@echo "  clean-heavy-artifacts - remove generated debug/dev-live/recovery bloat"

# ---------------------------------------------------------------------------
# Dev stack
# ---------------------------------------------------------------------------

dev:
	@set -euo pipefail; \
	echo "==> Stopping existing processes..."; \
	bash scripts/dev-stop.sh; \
	rm -f logs/trader.pid logs/process/frontend.pid; \
	echo "==> Building trader ($(TRADER_DEV_PROFILE))..."; \
	$(CARGO_CMD) build --profile $(TRADER_DEV_PROFILE) --bin trader; \
	mkdir -p logs/process; \
	echo "==> Starting frontend dev server..."; \
	ROOTDIR="$$(pwd)"; \
	: > "$$ROOTDIR/logs/process/frontend.log"; \
	nohup bash -lc "cd \"$$ROOTDIR/frontend\" && exec npx vite --host 127.0.0.1 --port $(FRONTEND_PORT) --strictPort >> \"$$ROOTDIR/logs/process/frontend.log\" 2>&1" >/dev/null 2>&1 & echo $$! > "$$ROOTDIR/logs/process/frontend.pid"; \
	cd "$$ROOTDIR"; \
	echo "Poly-updown Engine running — Dashboard: http://127.0.0.1:$(FRONTEND_PORT)"; \
	echo "==> Starting trader in foreground (use make dev-stop from another terminal)..."; \
	$(TRADER_DEV_BIN)

dev-stop:
	@set -e; \
	bash scripts/dev-stop.sh; \
	rm -f logs/trader.pid logs/process/frontend.pid; \
	echo "Poly-updown Engine stopped"

release:
	@set -euo pipefail; \
	echo "==> Stopping existing processes..."; \
	bash scripts/dev-stop.sh; \
	rm -f logs/trader.pid logs/process/frontend.pid; \
	echo "==> Building trader ($(TRADER_RELEASE_PROFILE))..."; \
	$(CARGO_CMD) build --profile $(TRADER_RELEASE_PROFILE) --bin trader; \
	mkdir -p logs/process; \
	echo "==> Starting frontend dev server..."; \
	ROOTDIR="$$(pwd)"; \
	: > "$$ROOTDIR/logs/process/frontend.log"; \
	nohup bash -lc "cd \"$$ROOTDIR/frontend\" && exec npx vite --host 127.0.0.1 --port $(FRONTEND_PORT) --strictPort >> \"$$ROOTDIR/logs/process/frontend.log\" 2>&1" >/dev/null 2>&1 & echo $$! > "$$ROOTDIR/logs/process/frontend.pid"; \
	cd "$$ROOTDIR"; \
	echo "Poly-updown Engine running — Dashboard: http://127.0.0.1:$(FRONTEND_PORT)"; \
	echo "==> Starting trader in foreground (use make release-stop from another terminal)..."; \
	exec $(TRADER_RELEASE_BIN)

release-stop:
	@set -e; \
	bash scripts/dev-stop.sh; \
	rm -f logs/trader.pid logs/process/frontend.pid; \
	echo "Poly-updown Engine stopped"

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build:
	@$(CARGO_CMD) build --release --bin trader

build-release: build

# ---------------------------------------------------------------------------
# Logs & status
# ---------------------------------------------------------------------------

logs:
	@tail -f logs/process/runtime.log

logs-all:
	@tail -f logs/process/runtime.log logs/process/frontend.log

status:
	@echo "--- Processes ---"; \
	root_dir=$$(pwd -P); trader_pid=; vite_pid=; \
	for process in /proc/[0-9]*; do \
		pid=$${process##*/}; \
		exe=$$(readlink "$$process/exe" 2>/dev/null || true); \
		exe=$${exe%" (deleted)"}; \
		if [[ -z "$$trader_pid" ]]; then \
			case "$$exe" in \
				"$$root_dir"/target/*/trader) \
					profile=$${exe#"$$root_dir"/target/}; profile=$${profile%/trader}; \
					if [[ -n "$$profile" && "$$profile" != */* ]]; then trader_pid=$$pid; fi ;; \
			esac; \
		fi; \
		if [[ -z "$$vite_pid" && "$$exe" == */node ]]; then \
			cwd=$$(readlink -f "$$process/cwd" 2>/dev/null || true); \
			if [[ "$$cwd" == "$$root_dir/frontend" ]]; then \
				argv=(); \
				if mapfile -d '' -t argv < "$$process/cmdline" 2>/dev/null; then \
					script=$${argv[1]-}; \
					if [[ "$$script" != /* ]]; then script="$$cwd/$$script"; fi; \
					if [[ "$$(readlink -f "$$script" 2>/dev/null || true)" == "$$root_dir/frontend/node_modules/vite/bin/vite.js" ]]; then \
						host_ok=0; port_ok=0; strict_ok=0; mode_ok=1; \
						for ((i=2; i<$${#argv[@]}; i++)); do \
							case "$${argv[i]}" in \
								serve) if (( i != 2 )); then mode_ok=0; fi ;; \
								--host) ((i++)); if [[ "$${argv[i]-}" == 127.0.0.1 ]]; then host_ok=1; else mode_ok=0; fi ;; \
								--port) ((i++)); if [[ "$${argv[i]-}" =~ ^[0-9]+$$ ]]; then port_ok=1; else mode_ok=0; fi ;; \
								--strictPort) strict_ok=1 ;; \
								*) mode_ok=0 ;; \
							esac; \
						done; \
						if (( host_ok && port_ok && strict_ok && mode_ok )); then vite_pid=$$pid; fi; \
					fi; \
				fi; \
			fi; \
		fi; \
		if [[ -n "$$trader_pid" && -n "$$vite_pid" ]]; then break; fi; \
	done; \
	if [ -n "$$trader_pid" ]; then \
		echo "  trader: running (pid $$trader_pid)"; \
	else \
		echo "  trader: not running"; \
	fi; \
	if [ -n "$$vite_pid" ]; then \
		echo "  vite: running (pid $$vite_pid)"; \
	else \
		echo "  vite: not running"; \
	fi; \
	echo ""; \
	echo "--- Runtime Files ---"; \
	if [ -f data/runtime/health.json ]; then \
		echo "  health.json:"; \
		cat data/runtime/health.json; \
	else \
		echo "  health.json: missing"; \
	fi; \
	echo ""; \
	if [ -f data/runtime/debug_state.json ]; then \
		echo "  debug_state.json:"; \
		cat data/runtime/debug_state.json; \
	else \
		echo "  debug_state.json: missing"; \
	fi

# ---------------------------------------------------------------------------
# Existing targets
# ---------------------------------------------------------------------------

setup:
	@bash scripts/dev-setup.sh

fmt:
	@$(CARGO_CMD) fmt --all

fmt-check:
	@$(CARGO_CMD) fmt --all -- --check

size-check:
	@bash scripts/check_rust_file_sizes.sh

lint:
	@$(CARGO_CMD) clippy --workspace --all-targets --all-features -- -D warnings

check-fast: size-check
	@CARGO_MAKEFLAGS= $(CARGO_CMD) build --profile $(TRADER_DEV_PROFILE) --bin trader

test-runtime:
	@$(CARGO_CMD) test --lib runtime_control::tests::

test-contracts:
	@$(CARGO_CMD) test --lib execution::supervisor::tests::
	@$(CARGO_CMD) test --lib strategy::hold_to_resolution::tests::
	@$(CARGO_CMD) test --lib portfolio::
	@$(CARGO_CMD) test --lib risk::

test-surface:
	@$(CARGO_CMD) test --lib dashboard::ws::tests::
	@$(CARGO_CMD) test --lib dashboard::tests::live_account::
	@$(CARGO_CMD) test --lib dashboard::handlers::state::tests::
	@$(CARGO_CMD) test --lib dashboard::status_diagnostics::tests::

backtest-strategy:
	@scripts/backtest_strategy.sh

# Same shipped engine as backtest-strategy; subset of symbols for a fast structural loop.
# Does not clobber matrix/ proof. Not an optimized PnL score (use make optimize for that).
#   make backtest-strategy-debug
#   BACKTEST_STRATEGY_SYMBOLS=btc make backtest-strategy-debug
#   BACKTEST_STRATEGY_CAPTURE=data/captures/... BACKTEST_STRATEGY_SYMBOLS=eth,sol make backtest-strategy-debug
backtest-strategy-debug:
	@BACKTEST_STRATEGY_SYMBOLS="$${BACKTEST_STRATEGY_SYMBOLS:-sol}" scripts/backtest_strategy.sh

optimize:
	@$(MAKE) backtest-strategy-auto-optimize

backtest-strategy-auto-optimize:
	@scripts/backtest_strategy_auto_optimize.sh

test:
	@$(CARGO_CMD) test --workspace --all-targets --all-features

check:
	@$(CARGO_CMD) check --workspace --all-targets --all-features

deny:
	@cargo deny check

run:
	@$(CARGO_CMD) run --profile $(TRADER_DEV_PROFILE) --bin trader

ci: fmt-check size-check lint test deny

clean:
	@cargo clean

clean-heavy-artifacts:
	@rm -rf target/debug target/dev-live data/capture_recovery
