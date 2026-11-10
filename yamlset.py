#!/usr/bin/env python3
"""Tiny dynamic scalar editor for optimization.yml.

Default target is optimization.yml beside this script. Standard-library only.
Examples:
  ./yamlset.py max_inventory_per_token low=8 high=16
  ./yamlset.py optimizer.auto.max_generations=4 optimizer.auto.bipop=true
  ./yamlset.py --delete optimizer.auto.min_accept_cashflow_pnl
  ./yamlset.py --get max_inventory_per_token.low
  ./yamlset.py --find inventory
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

KEY_RE = re.compile(
    r"^(?P<indent>\s*)(?P<key>(?:'[^']+'|\"[^\"]+\"|[^:#\n][^:\n]*?)):(?P<rest>.*)$"
)


@dataclass
class Entry:
    path: str
    line_no: int
    indent: int
    key: str
    rest: str
    is_leaf: bool


def default_file() -> Path:
    return Path(__file__).resolve().parent / "optimization.yml"


def issue(msg: str, strict: bool = False) -> int:
    print(f"ISSUE {msg}")
    return 2 if strict else 0


def strip_quote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def parse_entries(lines: list[str]) -> list[Entry]:
    stack: list[tuple[int, str]] = []
    entries: list[Entry] = []
    for idx, line in enumerate(lines):
        match = KEY_RE.match(line)
        if not match:
            continue
        indent = len(match.group("indent"))
        key = strip_quote(match.group("key"))
        rest = match.group("rest")
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = ".".join([*(key for _, key in stack), key])
        is_leaf = bool(rest.strip())
        entries.append(Entry(path, idx, indent, key, rest, is_leaf))
        if not is_leaf:
            stack.append((indent, key))
    return entries


def scalar_to_text(raw: str) -> str:
    raw = raw.strip()
    if raw.lower() in {"true", "false"}:
        return raw.lower()
    if raw.lower() in {"null", "none"}:
        return "null"
    if re.fullmatch(r"-?\d+(?:\.\d+)?(?:e-?\d+)?", raw, re.I):
        return raw
    return raw


def replace_value(line: str, value: str) -> str:
    comment = ""
    body = line.rstrip("\n")
    if "#" in body:
        prefix, suffix = body.split("#", 1)
        body = prefix.rstrip()
        comment = " #" + suffix
    if ":" not in body:
        raise ValueError("line has no ':'")
    left = body.split(":", 1)[0]
    return f"{left}: {scalar_to_text(value)}{comment}\n"


def find_entry(entries: list[Entry], path: str) -> Entry | None:
    return next((entry for entry in entries if entry.path == path), None)


def subtree_end_line(entries: list[Entry], entry: Entry, line_count: int) -> int:
    end = line_count
    for other in entries:
        if other.line_no <= entry.line_no:
            continue
        if other.indent <= entry.indent:
            end = other.line_no
            break
    return end


def insert_missing_path(lines: list[str], entries: list[Entry], target: str, value: str) -> int:
    parts = target.split(".")
    parent_path = ""
    parent_entry: Entry | None = None
    depth = 0
    for idx, part in enumerate(parts[:-1]):
        path = child_path(parent_path, part)
        entry = find_entry(entries, path)
        if entry is None:
            break
        if entry.is_leaf:
            raise ValueError(f"{path} is a scalar; cannot create child path {target}")
        parent_path = path
        parent_entry = entry
        depth = idx + 1
    if parent_entry is None:
        insert_at = len(lines)
        indent = 0
    else:
        insert_at = subtree_end_line(entries, parent_entry, len(lines))
        indent = parent_entry.indent + 2
    new_lines: list[str] = []
    for part in parts[depth:-1]:
        new_lines.append(" " * indent + f"{part}:\n")
        indent += 2
    new_lines.append(" " * indent + f"{parts[-1]}: {scalar_to_text(value)}\n")
    lines[insert_at:insert_at] = new_lines
    return len(new_lines)


def child_path(parent: str, field: str) -> str:
    return f"{parent}.{field}" if parent else field


def load(path: Path) -> tuple[list[str], list[Entry]]:
    lines = path.read_text().splitlines(keepends=True)
    return lines, parse_entries(lines)


def set_path(path: Path, target: str, value: str) -> int:
    lines, entries = load(path)
    entry = find_entry(entries, target)
    if not entry:
        try:
            inserted = insert_missing_path(lines, entries, target, value)
        except ValueError as exc:
            return issue(str(exc), strict=True)
        path.write_text("".join(lines))
        print(f"SET {target}: <missing> -> {value}")
        return 0 if inserted > 0 else issue(f"missing {target}", strict=True)
    old = entry.rest.strip()
    lines[entry.line_no] = replace_value(lines[entry.line_no], value)
    path.write_text("".join(lines))
    print(f"SET {target}: {old} -> {value}")
    return 0


def delete_path(path: Path, target: str) -> int:
    lines, entries = load(path)
    entry = find_entry(entries, target)
    if not entry:
        return issue(f"missing {target}", strict=True)
    end = subtree_end_line(entries, entry, len(lines)) if not entry.is_leaf else entry.line_no + 1
    del lines[entry.line_no:end]
    path.write_text("".join(lines))
    print(f"DELETE {target}")
    return 0


def get_path(path: Path, target: str) -> int:
    _, entries = load(path)
    entry = find_entry(entries, target)
    if not entry:
        return issue(f"missing {target}", strict=True)
    print(entry.rest.strip())
    return 0


def find_paths(path: Path, text: str) -> int:
    _, entries = load(path)
    text = text.lower()
    found = [entry.path for entry in entries if text in entry.path.lower()]
    for item in found:
        print(item)
    return 0 if found else issue(f"no match {text}")


def apply_param_fields(path: Path, param: str, pairs: list[str]) -> int:
    code = 0
    changes = 0
    for pair in pairs:
        if "=" not in pair:
            return issue(f"bad assignment {pair}", strict=True)
        field, value = pair.split("=", 1)
        result = set_path(path, child_path(param, field), value)
        code = max(code, result)
        if result == 0:
            changes += 1
    if code == 0:
        print(f"OK optimization.yml changes={changes} issues=0")
    return code


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=str(default_file()))
    parser.add_argument("--delete", action="append", default=[])
    parser.add_argument("--get")
    parser.add_argument("--find")
    parser.add_argument("items", nargs="*")
    args = parser.parse_args(argv)
    path = Path(args.file)
    if args.delete:
        code = 0
        changes = 0
        for target in args.delete:
            result = delete_path(path, target)
            code = max(code, result)
            if result == 0:
                changes += 1
        if code == 0:
            print(f"OK optimization.yml deletes={changes} issues=0")
        return code
    if args.get:
        return get_path(path, args.get)
    if args.find:
        return find_paths(path, args.find)
    if not args.items:
        return issue("no changes requested", strict=True)
    if len(args.items) == 1 and "=" in args.items[0]:
        target, value = args.items[0].split("=", 1)
        code = set_path(path, target, value)
        if code == 0:
            print("OK optimization.yml changes=1 issues=0")
        return code
    if len(args.items) >= 2 and "=" not in args.items[0]:
        return apply_param_fields(path, args.items[0], args.items[1:])
    code = 0
    changes = 0
    for item in args.items:
        if "=" not in item:
            return issue(f"bad assignment {item}", strict=True)
        target, value = item.split("=", 1)
        result = set_path(path, target, value)
        code = max(code, result)
        if result == 0:
            changes += 1
    if code == 0:
        print(f"OK optimization.yml changes={changes} issues=0")
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
