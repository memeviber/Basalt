#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT="$ROOT/.tmp/diagnostic-contract"
STRICT=(-std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
mkdir -p "$OUT"

source "$ROOT/scripts/bootstrap_stage.sh"
BOOT_BIN=$(bootstrap_stage "$ROOT" "$OUT/bootstrap-stage" "${STRICT[@]}")

expect_diagnostic() {
  local source=$1
  local name=$2
  shift 2
  local log="$OUT/$name.log"
  local generated="$OUT/$name.c"

  if "$BOOT_BIN" "$source" "$generated" >"$log" 2>&1; then
    printf 'FAIL %s: expected Bootstrap rejection\n' "$name" >&2
    return 1
  fi

  local field
  for field in "$@"; do
    if ! grep -Fqx "$field" "$log"; then
      printf 'FAIL %s: missing diagnostic field: %s\n' "$name" "$field" >&2
      cat "$log" >&2
      return 1
    fi
  done
  test ! -e "$generated"
  printf 'PASS %s\n' "$name"
}

expect_diagnostic \
  "$ROOT/tests/spec/invalid/initializer_type_mismatch_invalid.basalt" \
  initializer_type_mismatch \
  'diagnostic.code=20' \
  'diagnostic.expected=int' \
  'diagnostic.found=string' \
  'diagnostic.hint=make the initializer expression match the declared type' \
  'diagnostic.excerpt=  let value: int = "wrong";'

expect_diagnostic \
  "$ROOT/tests/spec/invalid/return_type_mismatch_invalid.basalt" \
  return_type_mismatch \
  'diagnostic.code=23' \
  'diagnostic.expected=int' \
  'diagnostic.found=string' \
  'diagnostic.hint=return a value matching the function return type' \
  'diagnostic.excerpt=  return "wrong";'

expect_diagnostic \
  "$ROOT/tests/regression/tagged_union_type_invalid.basalt" \
  tagged_union_type \
  'diagnostic.code=12' \
  'diagnostic.expected=int' \
  'diagnostic.found=string'

expect_diagnostic \
  "$ROOT/tests/spec/invalid/unary_not_type_invalid.basalt" \
  unary_not_type \
  'diagnostic.code=15' \
  'diagnostic.excerpt=  if !"invalid" then return 1;'

printf '%s\n' 'Diagnostic contract checks completed successfully.'
