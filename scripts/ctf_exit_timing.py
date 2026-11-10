#!/usr/bin/env python3
"""Report CTF exit timing from one saved joint optimizer proof."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


SYMBOLS = ("BTC", "ETH", "SOL", "XRP")
TERMINAL_KINDS = ("redeem_confirmed", "merge_confirmed")


def aggregate(values_ms: list[int]) -> dict[str, int | float | None]:
    return {
        "count": len(values_ms),
        "sum_ms": sum(values_ms),
        "average_seconds": (
            None if not values_ms else sum(values_ms) / len(values_ms) / 1000
        ),
    }


def classify_exit(cohort: dict[str, Any]) -> str:
    exit_class = cohort["exit_class"]
    if exit_class == "first_to_post_proceeds":
        return "DeadCat" if cohort["sibling_reason"] == "DeadCat" else "ProfitNow"
    if exit_class in (
        "first_to_settlement",
        "settlement_only_no_canonical_oracle",
        "settlement_only_failed_mandatory_execution",
    ):
        return "Hold"
    if exit_class == "merge":
        return "Merge"
    raise ValueError(f"unsupported exit class {exit_class!r}")


def resolve_candidate_result(run_dir: Path) -> Path:
    summary_path = run_dir / "summary.json"
    with summary_path.open("r", encoding="utf-8") as stream:
        summary = json.load(stream)
    pointer = Path(str(summary["candidate_result_path"]))
    if not pointer.is_absolute():
        pointer = (Path(__file__).resolve().parents[1] / pointer).resolve()
    expected = (run_dir / "candidate_result.json").resolve()
    if pointer.resolve() != expected:
        raise ValueError(
            f"summary candidate result does not identify {expected}: {pointer}"
        )
    return expected


def scope_proofs(document: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    selection_role = (
        "selection_candidate"
        if document.get("selection_candidate") is not None
        else "selection_incumbent"
    )
    scopes = [(selection_role, document[selection_role])]
    if document.get("validation_candidate") is not None:
        scopes.append(("validation_candidate", document["validation_candidate"]))
    return scopes


def load_scope(
    role: str, proof: dict[str, Any]
) -> tuple[str, str, dict[str, list[dict[str, Any]]]]:
    stats = proof["canonical_session_stats"]
    capture_path = str(proof["capture_path"])
    candidate_id = str(proof["candidate_id"])
    canonical: dict[str, dict[str, dict[str, Any]]] = {
        symbol: {} for symbol in SYMBOLS
    }
    for row in stats["canonical"]["ctf_exit_cohorts"]:
        symbol = str(row["symbol"])
        if symbol not in canonical:
            continue
        key = str(row["paired_cohort_key"])
        if key in canonical[symbol]:
            raise ValueError(f"{role} duplicate {symbol} cohort {key}")
        canonical[symbol][key] = {
            "paired_cohort_key": key,
            "market_id": row["market_id"],
            "split_ts_ms": int(row["split_ts_ms"]),
            "split_principal_units_6dp": int(row["split_principal_units_6dp"]),
            "exit_group": classify_exit(row),
            "first_authorized_quantity_units_6dp": (
                0
                if row["first_authorized_quantity_units_6dp"] is None
                else int(row["first_authorized_quantity_units_6dp"])
            ),
        }

    closed: dict[str, dict[str, dict[str, Any]]] = {
        symbol: {} for symbol in SYMBOLS
    }
    for row in stats["closed_cohorts"]:
        symbol = str(row["symbol"])
        if symbol not in closed:
            continue
        cashflow = row["cashflow"]
        key = str(row["paired_cohort_key"])
        closed[symbol][key] = {
            "receipts": [
                {
                    "confirmed_at_ms": int(receipt["confirmed_at_ms"]),
                    "qty_units_6dp": int(receipt["qty_units_6dp"]),
                    "close_reason": receipt["close_reason"],
                    "token": receipt["token"],
                }
                for receipt in cashflow["managed_sell_receipts"]
            ],
            "terminal_times_ms": [
                int(entry["ts_ms"])
                for entry in cashflow["events"]
                if entry["event"]["ctf_event_kind"] in TERMINAL_KINDS
            ],
        }

    cohorts_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for symbol in SYMBOLS:
        cohorts = []
        for key, row in canonical[symbol].items():
            if key not in closed[symbol]:
                raise ValueError(f"{role} {symbol} missing closed cohort {key}")
            cohorts.append({**row, **closed[symbol][key]})
        if len(closed[symbol]) != len(cohorts):
            raise ValueError(
                f"{role} {symbol} canonical and closed cohort counts differ"
            )
        cohorts_by_symbol[symbol] = cohorts
    return capture_path, candidate_id, cohorts_by_symbol


def load_market_windows(
    capture_path: Path, wanted: set[str]
) -> dict[str, tuple[int, int]]:
    discovery = capture_path / "polymarket_discovery_snapshot.jsonl"
    windows: dict[str, tuple[int, int]] = {}
    with discovery.open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            for market in json.loads(line)["snapshot"]["markets"]:
                market_id = market["market_id"]
                if market_id in wanted and market_id not in windows:
                    windows[market_id] = (
                        int(market["window_start_ms"]),
                        int(market["window_end_ms"]),
                    )
            if len(windows) == len(wanted):
                break
    missing = wanted - windows.keys()
    if missing:
        raise ValueError(
            f"discovery artifact lacks {len(missing)} required market windows"
        )
    return windows


def completion_time(receipts: list[dict[str, Any]], required: int) -> int | None:
    if required <= 0:
        return None
    confirmed = 0
    for receipt in receipts:
        confirmed += receipt["qty_units_6dp"]
        if confirmed == required:
            return receipt["confirmed_at_ms"]
        if confirmed > required:
            raise ValueError(
                f"confirmed quantity {confirmed} exceeds required quantity {required}"
            )
    return None


def timing_summary(
    scope: str,
    candidate_id: str,
    capture: str,
    symbol: str,
    cohorts: list[dict[str, Any]],
    windows: dict[str, tuple[int, int]],
) -> dict[str, Any]:
    first_fill_entry: list[int] = []
    first_fill_open: list[int] = []
    first_full_entry: list[int] = []
    first_full_open: list[int] = []
    active: list[int] = []
    capital: list[int] = []
    exits: dict[str, dict[str, list[int]]] = {
        name: {
            "sell_after_entry": [],
            "market_close_after_entry": [],
            "terminal_wait_after_close": [],
            "capital_lifecycle": [],
        }
        for name in ("DeadCat", "ProfitNow", "Hold", "Merge")
    }

    for cohort in cohorts:
        split = cohort["split_ts_ms"]
        opening, closing = windows[cohort["market_id"]]
        receipts = sorted(
            cohort["receipts"], key=lambda receipt: receipt["confirmed_at_ms"]
        )
        first = [
            receipt
            for receipt in receipts
            if receipt["close_reason"] == "paired_first_managed_sell"
        ]
        post = [
            receipt
            for receipt in receipts
            if receipt["close_reason"] == "paired_post_proceeds_sell"
        ]
        terminal = sorted(cohort["terminal_times_ms"])

        if first:
            first_fill_entry.append(first[0]["confirmed_at_ms"] - split)
            first_fill_open.append(first[0]["confirmed_at_ms"] - opening)
        first_complete = completion_time(
            first, cohort["first_authorized_quantity_units_6dp"]
        )
        if first_complete is not None:
            first_full_entry.append(first_complete - split)
            first_full_open.append(first_complete - opening)

        group = cohort["exit_group"]
        measures = exits[group]
        if group in ("DeadCat", "ProfitNow"):
            post_complete = completion_time(
                post, cohort["split_principal_units_6dp"]
            )
            if post_complete is not None:
                measures["sell_after_entry"].append(post_complete - split)
        else:
            measures["market_close_after_entry"].append(closing - split)
            if not terminal:
                raise ValueError(
                    f"{scope} {symbol} {cohort['paired_cohort_key']} lacks a terminal receipt"
                )
        if terminal:
            measures["terminal_wait_after_close"].append(terminal[-1] - closing)

        disposal_times = [
            receipt["confirmed_at_ms"] for receipt in receipts
        ] + terminal
        if not disposal_times:
            raise ValueError(
                f"{scope} {symbol} {cohort['paired_cohort_key']} has no inventory disposal"
            )
        disposal = max(disposal_times)
        active.append(max(0, min(disposal, closing) - split))
        lifecycle = max(0, disposal - split)
        capital.append(lifecycle)
        measures["capital_lifecycle"].append(lifecycle)

    return {
        "scope": scope,
        "candidate_id": candidate_id,
        "capture_path": capture,
        "symbol": symbol,
        "markets": len(cohorts),
        "first_fill": {
            "after_entry": aggregate(first_fill_entry),
            "after_market_open": aggregate(first_fill_open),
        },
        "first_full_completion": {
            "after_entry": aggregate(first_full_entry),
            "after_market_open": aggregate(first_full_open),
        },
        "active_exposure": aggregate(active),
        "capital_lifecycle": aggregate(capital),
        "exits": {
            name: {
                measure: aggregate(values) for measure, values in measures.items()
            }
            for name, measures in exits.items()
        },
    }


def seconds(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f} s"


def print_text(results: list[dict[str, Any]]) -> None:
    print(
        "Scope                 Symbol  First fills  After entry  "
        "After market opening  Full first sell  Active exposure  Capital lifecycle"
    )
    for result in results:
        first = result["first_fill"]
        full = result["first_full_completion"]["after_entry"]
        print(
            f"{result['scope']:<21} {result['symbol']:<6}  "
            f"{first['after_entry']['count']:>11}  "
            f"{seconds(first['after_entry']['average_seconds']):>11}  "
            f"{seconds(first['after_market_open']['average_seconds']):>20}  "
            f"{seconds(full['average_seconds']):>15} ({full['count']})  "
            f"{seconds(result['active_exposure']['average_seconds']):>15}  "
            f"{seconds(result['capital_lifecycle']['average_seconds']):>17}"
        )

    print()
    print(
        "Scope                 Symbol  Exit       Count  Sell after entry  "
        "Close after entry  Terminal wait  Capital lifecycle"
    )
    for result in results:
        for name, measures in result["exits"].items():
            count = measures["capital_lifecycle"]["count"]
            if not count:
                continue
            print(
                f"{result['scope']:<21} {result['symbol']:<6}  {name:<9}  "
                f"{count:>5}  "
                f"{seconds(measures['sell_after_entry']['average_seconds']):>16}  "
                f"{seconds(measures['market_close_after_entry']['average_seconds']):>17}  "
                f"{seconds(measures['terminal_wait_after_close']['average_seconds']):>13}  "
                f"{seconds(measures['capital_lifecycle']['average_seconds']):>17}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report CTF exit timing from a saved joint optimizer proof only."
    )
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    result_path = resolve_candidate_result(run_dir)
    with result_path.open("r", encoding="utf-8") as stream:
        document = json.load(stream)

    results: list[dict[str, Any]] = []
    for role, proof in scope_proofs(document):
        capture_path, candidate_id, cohorts_by_symbol = load_scope(role, proof)
        capture = Path(capture_path)
        if not capture.is_absolute():
            capture = (Path(__file__).resolve().parents[1] / capture).resolve()
        market_ids = {
            cohort["market_id"]
            for cohorts in cohorts_by_symbol.values()
            for cohort in cohorts
        }
        windows = load_market_windows(capture, market_ids)
        results.extend(
            timing_summary(
                role,
                candidate_id,
                str(capture),
                symbol,
                cohorts_by_symbol[symbol],
                windows,
            )
            for symbol in SYMBOLS
        )

    output = {
        "run_dir": str(run_dir),
        "candidate_result_path": str(result_path),
        "source": "saved_joint_native_artifact",
        "results": results,
    }
    if args.json:
        json.dump(output, sys.stdout, indent=2, sort_keys=True)
        print()
    else:
        print_text(results)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"ctf_exit_timing: {error}", file=sys.stderr)
        raise SystemExit(2)
