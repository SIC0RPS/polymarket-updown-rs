#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/tools/production_rust_line_limits.txt"

if [[ ! -f "$manifest" ]]; then
  echo "error: missing baseline manifest: $manifest" >&2
  exit 1
fi

declare -A baseline_limits=()
while IFS=$'\t' read -r path limit; do
  [[ -z "${path:-}" ]] && continue
  [[ "${path:0:1}" == "#" ]] && continue
  baseline_limits["$path"]="$limit"
done < "$manifest"

status=0
warn_count=0

while read -r lines path; do
  [[ "$path" == "total" ]] && continue

  if (( lines > 500 )); then
    echo "warn: production Rust file exceeds 500 lines: $path ($lines)"
    ((warn_count += 1))
  fi

  if (( lines <= 1000 )); then
    continue
  fi

  baseline="${baseline_limits[$path]:-}"
  if [[ -n "$baseline" ]]; then
    if (( lines > baseline )); then
      echo "error: oversized baseline file grew: $path ($lines > $baseline)" >&2
      status=1
    fi
    continue
  fi

  echo "error: new production Rust file exceeds 1000 lines: $path ($lines)" >&2
  status=1
done < <(
  cd "$repo_root"
  find src \
    -type f \
    -name '*.rs' \
    ! -path '*/tests/*' \
    ! -path '*/generated/*' \
    ! -name '*_tests.rs' \
    ! -name '*.generated.rs' \
    -print0 \
    | xargs -0 wc -l \
    | awk '$2 != "total" { print $1, $2 }'
)

if (( warn_count > 0 )); then
  echo "info: warned on $warn_count production Rust file(s) over 500 lines"
fi

exit "$status"
