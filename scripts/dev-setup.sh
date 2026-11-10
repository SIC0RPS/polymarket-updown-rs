#!/usr/bin/env bash
set -euo pipefail

if [ ! -d .git ]; then
  echo "error: git repository not initialized" >&2
  exit 1
fi

rm -f .git/hooks/pre-commit .git/hooks/commit-msg

echo "removed .git/hooks/pre-commit"
echo "removed .git/hooks/commit-msg"
