#!/usr/bin/env python3
"""Offline public-data diagnostic. Run with python -B; output must be new."""
import argparse
from collections import Counter
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import time
import zipfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

CONTRACT_SHA256 = "b2f52684f9adb3e86bac8e56c24390ba88bf1c8cc1538ddd2cd786576e2f3bf5"
OWNER = "src/strategy/support/oracle_authority/"
BATCH_SIZE = 131072


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(clean(value), stream, indent=2, allow_nan=False)
        stream.write("\n")


def ms(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError("Dates must specify UTC or an explicit timezone")
    return int(stamp.timestamp() * 1000)


def batches(paths, columns, timestamp, low, high):
    for path in sorted(paths):
        file = pq.ParquetFile(path)
        index = file.schema_arrow.names.index(timestamp)
        for group in range(file.num_row_groups):
            stats = file.metadata.row_group(group).column(index).statistics
            if stats and stats.has_min_max and (stats.max < low or stats.min > high):
                continue
            offset = 0
            for batch in file.iter_batches(
                row_groups=[group], columns=columns, batch_size=BATCH_SIZE,
            ):
                times = batch.column(timestamp)
                mask = pc.and_(pc.greater_equal(times, low), pc.less_equal(times, high))
                row = np.flatnonzero(mask.fill_null(False).to_numpy(zero_copy_only=False))
                if len(row):
                    frame = batch.filter(mask).to_pandas()
                    frame["row"] = row + offset
                    yield frame, str(path), group
                offset += batch.num_rows



def collect(frame, keys, values, result, source, group):
    """Retain distinct latest-timestamp values."""
    if frame.empty:
        return
    latest = frame.groupby(keys, sort=False)["ts"].transform("max")
    frame = frame[frame.ts == latest]
    aggregation = {"row": ("row", "first"), "count": ("row", "size")}
    for field in ("best_bid_size", "best_ask_size"):
        if field in frame:
            aggregation[field + "_min"] = (field, "min")
            aggregation[field + "_max"] = (field, "max")
    reduced = frame.groupby(keys + ["ts"] + values, dropna=False,
                            sort=False).agg(**aggregation).reset_index()
    for row in reduced.to_dict("records"):
        key = tuple(row[k] for k in keys)
        stamp = int(row["ts"])
        old = result.get(key)
        if old is not None and old["ts"] > stamp:
            continue
        if old is None or old["ts"] < stamp:
            old = {"ts": stamp, "pairs": {}}
            result[key] = old
        pair = tuple(clean(row[v]) for v in values)
        record = {
            "values": list(pair), "count": int(row["count"]),
            "source_file": source, "source_row_group": group,
            "source_row": int(row["row"]),
        }
        for field in aggregation:
            if field not in ("row", "count"):
                record[field] = clean(row[field])
        previous = old["pairs"].get(pair)
        if previous is None:
            old["pairs"][pair] = record
            continue
        previous["count"] += record["count"]
        for field in record:
            if field.endswith(("_min", "_max")):
                numbers = [v for v in (previous[field], record[field])
                           if v is not None]
                previous[field] = (
                    (min(numbers) if field.endswith("_min") else max(numbers))
                    if numbers else None
                )


def observation(record, at, age, kind):
    if record is None:
        return {"status": "missing"}
    variants = list(record["pairs"].values())
    result = {"ts": record["ts"], "age": at - record["ts"],
              "variants": variants}
    if at - record["ts"] > age:
        status = "stale"
    elif len(variants) != 1:
        status = "ambiguous"
    else:
        values = variants[0]["values"]
        finite = all(v is not None and math.isfinite(v) for v in values)
        if kind == "quote":
            valid = finite and 0 < values[0] <= values[1] < 1
        elif kind == "history":
            valid = finite and 0 <= values[0] <= 1
        else:
            valid = finite and values[0] > 0
        status = "valid" if valid else "invalid"
    result["status"] = status
    return result


def value(record):
    return record["variants"][0]["values"][0] if record["status"] == "valid" else None


def markets(args, low, high):
    found, excluded = {}, []
    pattern = re.compile(r"^(btc|eth|sol|xrp)-updown-5m-(\d+)$")
    for path in sorted(args.gamma_dir.glob("markets_*.json")):
        source = str(path)
        for index, market in enumerate(json.loads(path.read_text())):
            match = pattern.fullmatch(market.get("slug", ""))
            if not match or match[1].upper() not in args.symbols:
                continue
            start = int(match[2]) * 1000
            if not low <= start < high:
                continue
            identity = {"market_id": str(market["id"]), "slug": market["slug"],
                        "symbol": match[1].upper(), "start_ms": start,
                        "gamma_file": source, "gamma_index": index}
            try:
                end = ms(market["endDate"])
                if ms(market["eventStartTime"]) != start or end != start + 300000:
                    raise ValueError("window_identity")
                if end > high:
                    raise ValueError("window_outside_scope")
                outcomes = json.loads(market["outcomes"])
                prices = [Decimal(v) for v in json.loads(market["outcomePrices"])]
                tokens = json.loads(market["clobTokenIds"])
                if (len(outcomes) != 2 or set(outcomes) != {"Up", "Down"}
                        or len(prices) != 2 or len(tokens) != 2
                        or len(set(tokens)) != 2):
                    raise ValueError("outcome_identity")
                if (market.get("closed") is not True
                        or market.get("umaResolutionStatus") != "resolved"
                        or sorted(prices) != [0.0, 1.0]):
                    raise ValueError("unresolved_or_non_one_hot")
                identity.update(
                    end_ms=end, condition_id=market.get("conditionId"),
                    up_token=str(tokens[outcomes.index("Up")]),
                    down_token=str(tokens[outcomes.index("Down")]),
                    y=int(prices[outcomes.index("Up")]),
                    resolution_source=market.get("resolutionSource"),
                )
                if identity["market_id"] in found:
                    raise ValueError("duplicate_gamma_market")
                found[identity["market_id"]] = identity
            except (ValueError, KeyError, TypeError) as error:
                identity["reason"] = str(error)
                excluded.append(identity)
    duplicates = {r["market_id"] for r in excluded
                  if r["reason"] == "duplicate_gamma_market"}
    for key in duplicates:
        found.pop(key, None)
    return list(found.values()), excluded


def market_series(args, entries, anchors, kind, low, high):
    starts = {r["market_id"]: r["start_ms"] for r in entries}
    quote = kind == "orderbook"
    timestamp = "ts_ms" if quote else "timestamp"
    columns = (["ts_ms", "market_id", "token_id", "outcome", "best_bid",
                "best_ask", "best_bid_size", "best_ask_size"] if quote
               else ["market_id", "timestamp", "up_price"])
    symbol = entries[0]["symbol"]
    paths = (args.hf_root / "data" / kind /
             f"crypto={symbol}" / "timeframe=5-minute").glob("*.parquet")
    result, counts = {}, Counter()
    bounds = (low - 5000, high) if quote else ((low - 5000) // 1000, high // 1000)
    token_up = {r["market_id"]: r["up_token"] for r in entries}
    token_down = {r["market_id"]: r["down_token"] for r in entries}
    for frame, source, group in batches(paths, columns, timestamp, *bounds):
        frame = frame[frame.market_id.isin(starts)].copy()
        if frame.empty:
            continue
        frame["ts"] = frame[timestamp] * (1 if quote else 1000)
        elapsed = frame.ts - frame.market_id.map(starts)
        frame["slot"] = np.searchsorted(anchors * 1000, elapsed, side="left")
        frame = frame[frame.slot < len(anchors)].copy()
        if quote:
            expected = frame.market_id.map(token_up).where(
                frame.outcome == "Up", frame.market_id.map(token_down))
            valid_identity = frame.outcome.isin(["Up", "Down"]) & (
                frame.token_id == expected)
            counts["token_mismatch_rows"] += int((~valid_identity).sum())
            frame = frame[valid_identity]
            keys, values = ["market_id", "slot", "outcome"], ["best_bid", "best_ask"]
        else:
            keys, values = ["market_id", "slot"], ["up_price"]
        collect(frame, keys, values, result, source, group)
    return result, dict(counts)


def scalar_series(args, symbol, targets, kind, low, high):
    result = {}
    if kind == "chainlink":
        paths = (args.hf_root / "data/spot_prices").glob("*.parquet")
        stream = batches(paths, ["ts_ms", "symbol", "source", "price"],
                         "ts_ms", low - 2000, high)
        for frame, source, group in stream:
            frame = frame[(frame.source == "chainlink") &
                          (frame.symbol == symbol.lower() + "/usd")].copy()
            frame["ts"] = frame.ts_ms
            frame["slot"] = np.searchsorted(targets, frame.ts, side="left")
            collect(frame[frame.slot < len(targets)], ["slot"], ["price"],
                    result, source, group)
    else:
        for day in pd.date_range(pd.to_datetime(low, unit="ms", utc=True),
                                 pd.to_datetime(high - 1, unit="ms", utc=True),
                                 freq="D"):
            name = f"{symbol}USDT-1s-{day:%Y-%m-%d}"
            path = args.binance_dir / (symbol + "USDT") / (name + ".zip")
            if not path.exists():
                continue
            source = str(path)
            with zipfile.ZipFile(path) as archive:
                with archive.open(name + ".csv") as stream:
                    offset = 0
                    for frame in pd.read_csv(
                        stream, header=None, usecols=[4, 6],
                        chunksize=BATCH_SIZE,
                    ):
                        frame.columns = ["price", "ts"]
                        frame["row"] = np.arange(len(frame)) + offset
                        offset += len(frame)
                        frame["slot"] = np.searchsorted(targets, frame.ts,
                                                       side="left")
                        collect(frame[frame.slot < len(targets)], ["slot"],
                                ["price"], result, source, None)
    selected, previous = {}, None
    for slot, target in enumerate(targets):
        previous = result.get((slot,), previous)
        selected[int(target)] = previous
    return selected


def rows_for_symbol(args, entries, anchors, low, high):
    quotes, counts = market_series(args, entries, anchors,
                                  "orderbook", low, high)
    history, _ = market_series(args, entries, anchors,
                              "prices", low, high)
    times = np.array(sorted({r["start_ms"] + int(a) * 1000
                             for r in entries for a in anchors}), dtype=np.int64)
    openings = np.array([r["start_ms"] for r in entries], dtype=np.int64)
    cl = scalar_series(args, entries[0]["symbol"],
                       np.unique(np.concatenate([times, openings])),
                       "chainlink", low, high)
    spot = scalar_series(args, entries[0]["symbol"],
                         np.unique(np.concatenate([times * 1000,
                                                   (times - 5000) * 1000])),
                         "binance", low, high)
    rows = []
    for entry in entries:
        prior = {"Up": None, "Down": None, "history": None}
        key, start = entry["market_id"], entry["start_ms"]
        for slot, anchor in enumerate(anchors):
            at = start + int(anchor) * 1000
            row = dict(entry, anchor_s=int(anchor), at_ms=at,
                       date=pd.Timestamp(start, unit="ms", tz="UTC").strftime("%Y-%m-%d"),
                       intercept=1, horizon=1 - int(anchor) / 300)
            for leg in ("Up", "Down"):
                prior[leg] = quotes.get((key, slot, leg), prior[leg])
                row[leg.lower()] = observation(prior[leg], at, 5000, "quote")
            prior["history"] = history.get((key, slot), prior["history"])
            row["history"] = observation(prior["history"], at, 5000, "history")
            for name, series, target, age in (
                ("cl_open", cl, start, 0), ("cl_now", cl, at, 2000),
                ("spot_now", spot, at * 1000, math.inf),
                ("spot_past", spot, (at - 5000) * 1000, math.inf),
            ):
                row[name] = observation(series.get(target), target, age, "price")
            market_ok = all(row[leg]["status"] == "valid" for leg in ("up", "down"))
            row["m"] = None
            if market_ok:
                up = row["up"]["variants"][0]["values"]
                down = row["down"]["variants"][0]["values"]
                row["m"] = .5 * (.5 * (up[0] + up[1]) + 1 - .5 * (down[0] + down[1]))
            op, now = value(row["cl_open"]), value(row["cl_now"])
            past, current = value(row["spot_past"]), value(row["spot_now"])
            reference = math.log(now / op) if op is not None and now is not None else None
            momentum = current / past - 1 if past is not None and current is not None else None
            row.update(
                raw_reference_log_return=reference, raw_spot_return_5s=momentum,
                reference=reference / .001 if reference is not None else None,
                momentum=float(np.clip(momentum / .001, -1, 1)) if momentum is not None else None,
                reference_available=reference is not None, spot_available=momentum is not None,
                history_m=value(row["history"]),
                common=market_ok and reference is not None and momentum is not None,
            )
            rows.append(row)
    return rows, counts


def scores(frame, probability="m"):
    n = len(frame)
    p, y = frame[probability].to_numpy(float), frame.y.to_numpy(float)
    non_tie = p != .5
    correct = int(((p > .5) == y.astype(bool))[non_tie].sum())
    clipped = np.clip(p, 1e-6, 1 - 1e-6)
    return {
        "n": n, "markets": int(frame.market_id.nunique()),
        "dates": int(frame.date.nunique()),
        "brier": float(np.mean((p - y) ** 2)) if n else None,
        "log_loss": float(np.mean(-(y * np.log(clipped) +
                                    (1 - y) * np.log(1 - clipped)))) if n else None,
        "direction_non_tie_n": int(non_tie.sum()), "direction_correct": correct,
        "direction_accuracy_non_tie": correct / int(non_tie.sum()) if non_tie.any() else None,
        "p_half_ties": int((~non_tie).sum()),
        "mean_p": float(p.mean()) if n else None,
        "up_rate": float(y.mean()) if n else None,
        "residual": float(np.mean(y - p)) if n else None,
    }


def summarize(rows, anchors, protocol, counts):
    frame = pd.DataFrame(rows)
    market = frame[frame.m.notna()].copy()
    common = market[market.common].copy()
    common["residual"] = common.y - common.m
    common["brier"] = (common.y - common.m) ** 2
    p = common.m.clip(1e-6, 1 - 1e-6)
    common["loss"] = -(common.y * np.log(p) + (1 - common.y) * np.log(1 - p))
    means = common.groupby(["market_id", "date"])[["residual", "brier", "loss"]].mean()
    daily_means = means.groupby("date").residual.mean()
    counts.update(
        eligible_markets=int(frame.market_id.nunique()), total_anchors=len(frame),
        market_only_anchors=len(market), common_anchors=len(common),
        market_ok_reference_unavailable=int((~market.reference_available).sum()),
        market_ok_spot_unavailable=int((~market.spot_available).sum()),
        markets_any_common=int(common.market_id.nunique()),
        markets_all_five_common=int((common.groupby("market_id").size() == len(anchors)).sum()),
    )
    detailed = {leg: dict(Counter(r[leg]["status"] for r in rows))
                for leg in ("up", "down", "history", "cl_open", "cl_now",
                            "spot_now", "spot_past")}
    summary = {
        "counts": counts, "observation_status": detailed,
        "market_only_anchor_scores": {str(a): scores(market[market.anchor_s == a]) for a in anchors},
        "common_anchor_scores": {str(a): scores(common[common.anchor_s == a]) for a in anchors},
        "market_mean": means.mean().to_dict(),
        "daily_market_mean_residual": daily_means.to_dict(),
        "daily_residual_range": [daily_means.min(), daily_means.max()],
    }
    paired = market[market.history_m.notna()]
    summary["quote_history_common"] = {
        "n": len(paired), "quote": scores(paired), "history": scores(paired, "history_m"),
        "mean_absolute_probability_difference": (paired.m - paired.history_m).abs().mean(),
    }
    common["decile"] = np.minimum(np.floor(10 * common.m), 9)
    cells = []
    minimum = protocol["minimum_interpreted_cell"]
    for family, feature in (("reference", "reference"), ("momentum", "momentum")):
        for anchor in anchors:
            for decile in range(10):
                for sign in (-1, 0, 1):
                    subset = common[(common.anchor_s == anchor) &
                                    (common.decile == decile) &
                                    (np.sign(common[feature]) == sign)]
                    stats = scores(subset)
                    cells.append(dict(
                        family=family, anchor_s=int(anchor), probability_decile=decile,
                        return_sign=sign,
                        interpretable=(stats["markets"] >= minimum["distinct_markets"]
                                       and stats["dates"] >= minimum["utc_dates"]),
                        **stats,
                    ))
    summary["conditional_cells"] = len(cells)
    summary["interpretable_cells"] = sum(c["interpretable"] for c in cells)
    daily = [dict(date=date, anchor_s=int(anchor),
                  **scores(common[(common.date == date) & (common.anchor_s == anchor)]))
             for date in sorted(frame.date.unique()) for anchor in anchors]
    return {"symbol": rows[0]["symbol"], "summary": summary, "daily": daily, "cells": cells}


def recommendations(results):
    paired = {symbol: result["summary"]["quote_history_common"]
              for symbol, result in results.items()}
    supported = all(v["n"] and v["quote"]["brier"] < v["history"]["brier"]
                    for v in paired.values())
    return [{
        "status": "supported" if supported else "not_supported_in_all_symbols",
        "finding": "Paired quote/history Brier comparison.",
        "evidence": paired,
    }, {
        "status": "hypothesis_not_a_proven_live_defect",
        "recommendation": "Compare final and market calibration by stratum; check reference admission and target compatibility.",
        "evidence": "symbols/*/cells and symbols/*/summary/observation_status",
        "existing_owners": [OWNER + "snapshot_build/probability_composition.rs",
                            OWNER + "probability_evidence.rs"],
        "required_evidence": "Matched native forecasts, Chainlink identities, receipt times and current-target labels.",
    }]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("hf-root", "gamma-dir", "binance-dir", "protocol", "output-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--symbols", nargs="+", choices=["BTC", "ETH", "SOL", "XRP"],
                        default=["BTC", "ETH", "SOL", "XRP"])
    args = parser.parse_args()
    started = time.monotonic()
    if len(args.symbols) != len(set(args.symbols)):
        parser.error("Symbols must be distinct")
    protocol = json.loads(args.protocol.read_text())
    contract = {k: v for k, v in protocol.items() if k != "partitions_utc"}
    if hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest() != CONTRACT_SHA256:
        parser.error("Use the existing analysis protocol rules")
    start, end = map(ms, protocol["partitions_utc"]["development"])
    if not ms("2026-04-01T00:00:00Z") <= start < end <= ms("2026-04-15T00:00:00Z"):
        parser.error("Only April 1-14 development data are authorized")
    for name in ("hf_root", "gamma_dir", "binance_dir"):
        setattr(args, name, getattr(args, name).resolve(strict=True))
    output = args.output_dir.resolve()
    if any(output.is_relative_to(root) for root in
           (args.hf_root, args.gamma_dir, args.binance_dir)):
        parser.error("Output must be outside source directories")
    manifest = json.loads((args.hf_root / "download_manifest.json").read_text())
    if manifest["revision"] != protocol["hf_revision"]:
        parser.error("Hugging Face revision disagrees with protocol")
    output.mkdir(parents=True, exist_ok=False)
    entries, excluded = markets(args, start, end)
    anchors = np.array(protocol["anchors_elapsed_seconds"], dtype=np.int64)
    results = {}
    for symbol in args.symbols:
        selected = sorted((r for r in entries if r["symbol"] == symbol),
                          key=lambda r: (r["start_ms"], r["market_id"]))
        if not selected:
            raise ValueError(f"No eligible markets for {symbol}")
        print(f"{symbol}: extracting {len(selected)} markets", flush=True)
        rows, counts = rows_for_symbol(args, selected, anchors, start, end)
        path = output / f"{symbol}_observations.jsonl"
        with path.open("x") as stream:
            for row in rows:
                stream.write(json.dumps(clean(row), allow_nan=False) + "\n")
        results[symbol] = summarize(rows, anchors, protocol, counts)
        print(f"{symbol}: {results[symbol]['summary']['counts']}", flush=True)
    combined = {key: np.mean([r["summary"]["market_mean"][key] for r in results.values()])
                for key in ("residual", "brier", "loss")}
    report = {
        "protocol_path": str(args.protocol.resolve()), "protocol_sha256": digest(args.protocol),
        "analysis_scope": {"start_ms": start, "end_ms": end, "partition": "development"},
        "symbols": results, "excluded_markets": excluded, "combined_symbol_mean": combined,
        "recommendations": recommendations(results),
        "limitations": protocol["limitations"] + [
            "Equal-weight public quote benchmark; not native depth-weighted or final oracle output.",
            "Missing denotes bounded-scope absence; stale is excluded.",
            "Rows are zero-based within Parquet row groups or CSV members.",
            "Timestamps/ages: Binance microseconds; quotes, history and Chainlink milliseconds.",
        ],
        "source_manifests": {str(root / "download_manifest.json"):
                             digest(root / "download_manifest.json")
                             for root in (args.hf_root, args.gamma_dir, args.binance_dir)},
        "script_sha256": digest(Path(__file__)),
        "versions": {"numpy": np.__version__,
                     "pandas": pd.__version__, "pyarrow": pa.__version__},
        "elapsed_seconds": time.monotonic() - started,
    }
    write_json(output / "summary.json", report)
    print(f"Wrote {output}", flush=True)


if __name__ == "__main__":
    main()
