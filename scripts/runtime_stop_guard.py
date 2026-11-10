#!/usr/bin/env python3
"""Fail-closed guard for stopping a live Polymarket runtime."""

from __future__ import annotations

import argparse
import json
import sys
import time
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any


TRADING_ACTIVITY_EVENTS = {
    "entry_intent",
    "exit_intent",
    "exit_completed",
    "post_proceeds_sell_expired_or_unfilled",
    "settlement_completed",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate typed runtime state before scripts/dev-stop.sh terminates "
            "a trader process."
        )
    )
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository root. Defaults to the parent of scripts/.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid json {path}: {exc}") from exc


def load_toml(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError:
        return None
    except tomllib.TOMLDecodeError as exc:
        raise SystemExit(f"invalid toml {path}: {exc}") from exc


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    try:
        handle = path.open("r", encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return
    with handle:
        for idx, raw in enumerate(handle, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"invalid jsonl {path}:{idx}: {exc}") from exc
            if isinstance(row, dict):
                yield row


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def nested(mapping: dict[str, Any] | None, path: str) -> Any:
    cur: Any = mapping
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def capture_min_save_duration_ms(root: Path, debug: dict[str, Any] | None) -> int:
    config_paths: list[Path] = []
    config_hash = str(nested(debug, "runtime.config_hash") or "")
    if config_hash:
        config_paths.append(root / "data" / "config" / "objects" / f"{config_hash}.toml")
    config_paths.append(root / "config" / "main.toml")
    for path in config_paths:
        config = load_toml(path)
        value = nested(config, "capture.min_save_duration_ms")
        min_save_ms = as_int(value)
        if min_save_ms > 0:
            return min_save_ms
    return 0


def current_active_capture(root: Path, run_id: str | None) -> tuple[str, Path, int] | None:
    if not run_id:
        return None
    latest_enabled: tuple[str, Path, int] | None = None
    closed_sessions: set[str] = set()
    for row in iter_jsonl(root / "data" / "events" / "events.jsonl"):
        if row.get("run_id") != run_id or row.get("subject") != "capture":
            continue
        context = row.get("context")
        refs = row.get("refs")
        if not isinstance(context, dict) or not isinstance(refs, dict):
            continue
        session_id = str(refs.get("session_id") or "")
        action = str(context.get("action") or "")
        if not session_id:
            continue
        if action == "capture_enabled":
            base_dir = Path(str(refs.get("base_dir") or "data/captures"))
            capture_path = base_dir / session_id
            if not capture_path.is_absolute():
                capture_path = root / capture_path
            latest_enabled = (session_id, capture_path, as_int(row.get("ts_ms")))
        elif action in {"capture_finalized", "capture_deleted_short"}:
            closed_sessions.add(session_id)
    if latest_enabled is None or latest_enabled[0] in closed_sessions:
        return None
    return latest_enabled


def run_trading_activity_count(root: Path, run_id: str | None) -> int:
    if not run_id:
        return 0
    session_path = root / "logs" / "runs" / run_id / "session.jsonl"
    return sum(
        1
        for row in iter_jsonl(session_path)
        if str(row.get("event_kind") or "") in TRADING_ACTIVITY_EVENTS
    )


def active_capture_latency_rows(capture_path: Path) -> int:
    return sum(1 for _ in iter_jsonl(capture_path / "order_submit_latency.jsonl"))


def active_capture_stop_blocks(
    root: Path,
    run_id: str | None,
    debug: dict[str, Any] | None,
) -> list[str]:
    active_capture = current_active_capture(root, run_id)
    if active_capture is None:
        return []
    session_id, capture_path, start_ts_ms = active_capture
    min_save_ms = capture_min_save_duration_ms(root, debug)
    now_ms = max(as_int(nested(debug, "ts_ms")), int(time.time() * 1000))
    duration_ms = now_ms - start_ts_ms if start_ts_ms > 0 else 0
    if min_save_ms <= 0 or duration_ms >= min_save_ms:
        return []
    trading_rows = run_trading_activity_count(root, run_id)
    latency_rows = active_capture_latency_rows(capture_path)
    if trading_rows <= 0 and latency_rows <= 0:
        return []
    return [
        "active_capture_under_min_save "
        f"session_id={session_id} duration_ms={duration_ms} "
        f"min_save_duration_ms={min_save_ms} trading_rows={trading_rows} "
        f"order_submit_latency_rows={latency_rows}"
    ]


def stopped_runtime_facts(root: Path, debug: dict[str, Any] | None) -> list[str]:
    config = load_toml(root / "config" / "main.toml") or {}
    enabled_assets = nested(config, "strategy.maker_rebate_mm.enabled_assets")
    facts = [
        f"config.PAUSE={nested(config, 'polymarket.execution.PAUSE')}",
        f"config.max_order_qty={nested(config, 'max_order_qty')}",
        f"config.enabled_assets={enabled_assets}",
    ]
    if debug is None:
        return [*facts, "debug_state=missing"]
    return [
        *facts,
        f"runtime.pause={nested(debug, 'runtime.pause')}",
        f"execution.open_orders={nested(debug, 'execution.open_orders')}",
        f"portfolio.has_owned_inventory={nested(debug, 'portfolio.has_owned_inventory')}",
        f"settlement.status={nested(debug, 'settlement.status')}",
        f"settlement.redeemable_positions={nested(debug, 'settlement.redeemable_positions')}",
        f"settlement.redeemable_inventory_qty={nested(debug, 'settlement.redeemable_inventory_qty')}",
    ]


def guard(root: Path) -> tuple[bool, list[str]]:
    health = load_json(root / "data/runtime/health.json")
    debug = load_json(root / "data/runtime/debug_state.json")
    health_status = str((health or {}).get("status") or "")
    if health_status == "stopped":
        return True, ["health.status=stopped", *stopped_runtime_facts(root, debug)]

    reasons: list[str] = []
    if debug is None:
        reasons.append("missing data/runtime/debug_state.json")
        return False, reasons

    run_id = str((health or {}).get("run_id") or nested(debug, "runtime.run_id") or "")
    has_owned_inventory = nested(debug, "portfolio.has_owned_inventory")
    settlement_status = str(nested(debug, "settlement.status") or "")

    if nested(debug, "runtime.pause") is not True:
        reasons.append("runtime.pause is not true")
    if as_int(nested(debug, "execution.open_orders")) != 0:
        reasons.append(f"open_orders={nested(debug, 'execution.open_orders')}")
    if has_owned_inventory is not False:
        reasons.append(f"has_owned_inventory={has_owned_inventory}")

    if settlement_status and settlement_status != "idle":
        reasons.append(f"settlement.status={settlement_status}")
    if as_int(nested(debug, "settlement.redeemable_positions")) != 0:
        reasons.append(f"redeemable_positions={nested(debug, 'settlement.redeemable_positions')}")
    if abs(as_float(nested(debug, "settlement.redeemable_inventory_qty"))) > 0.000001:
        reasons.append(
            f"redeemable_inventory_qty={nested(debug, 'settlement.redeemable_inventory_qty')}"
        )

    reasons.extend(active_capture_stop_blocks(root, run_id, debug))

    return not reasons, reasons


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    ok, reasons = guard(root)
    if ok:
        print("runtime stop guard passed: " + "; ".join(reasons))
        return 0
    print("runtime stop guard blocked unsafe stop:", file=sys.stderr)
    for reason in reasons:
        print(f"  - {reason}", file=sys.stderr)
    print(
        "pause first, wait for lifecycle/settlement and active capture save floor, "
        "then retry",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
