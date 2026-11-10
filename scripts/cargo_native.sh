#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"

if [[ -n "${CONDA_PREFIX:-}" ]]; then
  clean_path=""
  IFS=':' read -r -a path_parts <<<"${PATH:-}"
  for part in "${path_parts[@]}"; do
    case "$part" in
      "$CONDA_PREFIX"|"$CONDA_PREFIX"/*) continue ;;
    esac
    if [[ -z "$clean_path" ]]; then
      clean_path="$part"
    else
      clean_path="$clean_path:$part"
    fi
  done
  export PATH="$clean_path"
fi

unset CONDA_DEFAULT_ENV
unset CONDA_EXE
unset CONDA_PREFIX
unset CONDA_PROMPT_MODIFIER
unset CONDA_PYTHON_EXE
unset CONDA_SHLVL

unset CC
unset CXX
unset CC_FOR_BUILD
unset CXX_FOR_BUILD
unset CFLAGS
unset CXXFLAGS
unset CPPFLAGS
unset LDFLAGS
unset LIBRARY_PATH
unset LD_LIBRARY_PATH
unset CPATH
unset C_INCLUDE_PATH
unset CPLUS_INCLUDE_PATH
unset PKG_CONFIG_PATH
unset PKG_CONFIG_LIBDIR
unset PKG_CONFIG_SYSROOT_DIR
unset OPENSSL_DIR
unset OPENSSL_LIB_DIR
unset OPENSSL_INCLUDE_DIR

exec cargo "$@"
