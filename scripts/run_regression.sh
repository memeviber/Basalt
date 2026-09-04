#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
BOOT_SOURCE="$ROOT/src/bootstrap/basaltc.basalt"
BOOT_BIN="$ROOT/.tmp/bootstrap.bin"
OUT="$ROOT/.tmp/regression"
source "$ROOT/scripts/bootstrap_stage.sh"
STRICT_FLAGS=(-std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
mkdir -p "$OUT"

GENERATED_SOURCES=()
track_source() { GENERATED_SOURCES+=("$1"); }
cleanup_generated() {
  for source in "${GENERATED_SOURCES[@]}"; do
    rm -f "${source}.c" "${source%.basalt}"
  done
}
trap cleanup_generated EXIT

# The stored C compiler builds the current Bootstrap compiler; tests use the
# resulting current-generation binary, never the frozen Host compiler.
BOOT_BIN=$(bootstrap_stage "$ROOT" "$ROOT/.tmp/bootstrap-stage" "${STRICT_FLAGS[@]}")
cp "$BOOT_BIN" "$ROOT/.tmp/bootstrap.bin"
BOOT_BIN="$ROOT/.tmp/bootstrap.bin"

assert_single_runtime_prologue() {
  local c_file=$1 label=$2
  local prefix needle count
  prefix=$(awk '/^#line /{exit} {print}' "$c_file")
  for needle in '#include <stdio.h>' '#include <stdlib.h>' '#include <string.h>' '_POSIX_C_SOURCE 200809L' '_XOPEN_SOURCE 700'; do
    count=$(printf '%s\n' "$prefix" | grep -F -c "$needle" || true)
    if [[ "$count" -ne 1 ]]; then
      echo "FAIL $label: expected exactly one '$needle' in generated C prologue, found $count" >&2
      return 1
    fi
  done
  if printf '%s\n' "$prefix" | awk '
    previous == "#if !defined(_WIN32)" && $0 == "#define _POSIX_C_SOURCE 200809L" { found = 1 }
    { previous = $0 }
    END { exit(found ? 0 : 1) }
  '; then
    echo "FAIL $label: obsolete duplicate feature prelude remains in generated C" >&2
    return 1
  fi
}

assert_source_mapping() {
  local c_file=$1 source_file=$2 source_line=$3 label=$4
  local directive="#line ${source_line} \"${source_file}\""
  if ! grep -Fqx -- "$directive" "$c_file"; then
    echo "FAIL $label: missing source mapping '$directive'" >&2
    return 1
  fi
}

compile_run() {
  local source=$1 label=$2
  track_source "$source"
  local boot_c="$OUT/${label}.boot.c"
  local boot_bin="$OUT/${label}.boot.bin"
  rm -f "$boot_c" "$boot_bin" "$ROOT"/$(basename "$source").c
  "$BOOT_BIN" "$source" "$boot_c" >/dev/null
  if [[ "$label" == "include_test_main" ]]; then
    assert_single_runtime_prologue "$boot_c" "$label (Bootstrap)"
    assert_source_mapping "$boot_c" "$ROOT/tests/regression/include_test_nested.basalt" 2 "$label (nested include)"
    assert_source_mapping "$boot_c" "$ROOT/tests/regression/include_test_lib.basalt" 3 "$label (include library)"
    assert_source_mapping "$boot_c" "$ROOT/tests/regression/include_test_main.basalt" 8 "$label (main source)"
  fi
  if [[ "$label" == "source_mapping_test" ]]; then
    assert_source_mapping "$boot_c" "$ROOT/tests/regression/source_mapping_test.basalt" 3 "$label"
  fi
  gcc -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror "$boot_c" -o "$boot_bin"
  "$boot_bin"
  printf 'PASS %s\n' "$label"
}

compile_run_with_output() {
  local source=$1 label=$2 expected=$3
  track_source "$source"
  local boot_c="$OUT/${label}.boot.c"
  local boot_bin="$OUT/${label}.boot.bin"
  local stdout_file="$OUT/${label}.stdout"
  rm -f "$boot_c" "$boot_bin" "$stdout_file" "$ROOT"/$(basename "$source").c
  "$BOOT_BIN" "$source" "$boot_c" >/dev/null
  gcc -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror "$boot_c" -o "$boot_bin"
  "$boot_bin" >"$stdout_file"
  if ! diff -u <(printf '%s' "$expected") "$stdout_file"; then
    echo "FAIL $label: stdout mismatch" >&2
    return 1
  fi
  printf 'PASS %s (stdout)\n' "$label"
}

build_process_probe() {
  local probe="$ROOT/tests/regression/sys_process_probe.c"
  local binary="$OUT/sys_process_probe"
  cc -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror "$probe" -o "$binary"
}

check_windows_process_compile() {
  local source="$OUT/sys_process_portable_test.boot.c"
  local object="$OUT/sys_process_portable_test.windows.o"
  if ! command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
    printf 'SKIP Windows process object compile (MinGW-w64 unavailable)\n'
    return 0
  fi
  x86_64-w64-mingw32-gcc -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror -c "$source" -o "$object"
  printf 'PASS Windows process object compile\n'
}

check_cli_modes() {
  local source="$ROOT/tests/regression/source_mapping_test.basalt"
  local line_c="$OUT/source_mapping_cli_line.c"
  local no_line_c="$OUT/source_mapping_cli_no_line.c"
  "$BOOT_BIN" --line "$source" "$line_c" >/dev/null
  assert_source_mapping "$line_c" "$source" 3 "source mapping CLI --line"
  "$BOOT_BIN" --no-line "$source" "$no_line_c" >/dev/null
  if grep -q '^#line ' "$no_line_c"; then
    echo "FAIL source mapping CLI --no-line: generated C still contains #line" >&2
    return 1
  fi
  printf 'PASS source mapping CLI modes\n'
}

auto_compile_cli() {
  local source="$OUT/auto_compile_input.basalt"
  local binary="$OUT/auto_compile_input.bin"
  local generated="${source}.c"
  cp "$ROOT/tests/super/print_stream_valid.basalt" "$source"
  "$BOOT_BIN" --compile "$source" -o "$binary" --cc gcc -- -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror >/dev/null
  if [[ ! -x "$binary" ]]; then
    echo "FAIL auto-compile: compiler did not produce executable" >&2
    return 1
  fi
  if ! diff -u <(printf '%s' $'Basalt-2026\nline-two\n42\n') <("$binary"); then
    echo "FAIL auto-compile: executable output mismatch" >&2
    return 1
  fi
  if "$BOOT_BIN" --compile "$source" -o "$OUT/auto_compile_should_fail.bin" --cc false -- >/dev/null 2>&1; then
    echo "FAIL auto-compile: nonzero compiler status was accepted" >&2
    return 1
  fi
  test -f "$generated"
  if ! grep -q '^#line ' "$generated"; then
    echo "FAIL auto-compile: #line is not enabled by default" >&2
    return 1
  fi
  local line_source="$OUT/auto_compile_line_input.basalt"
  local line_binary="$OUT/auto_compile_line_input.bin"
  local line_generated="${line_source}.c"
  cp "$ROOT/tests/super/print_stream_valid.basalt" "$line_source"
  "$BOOT_BIN" --compile --no-line "$line_source" -o "$line_binary" --cc gcc -- -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror >/dev/null
  if grep -q '^#line ' "$line_generated"; then
    echo "FAIL auto-compile --no-line: source mapping was not disabled" >&2
    return 1
  fi
  "$BOOT_BIN" --compile --line "$line_source" -o "$line_binary" --cc gcc -- -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror >/dev/null
  if ! grep -q '^#line ' "$line_generated"; then
    echo "FAIL auto-compile --line: source mapping was not enabled" >&2
    return 1
  fi
  printf 'PASS auto-compile CLI\n'
}

compile_run_with_input() {
  local source=$1 label=$2 input=$3 expected=$4
  track_source "$source"
  local boot_c="$OUT/${label}.boot.c"
  local boot_bin="$OUT/${label}.boot.bin"
  local stdout_file="$OUT/${label}.stdout"
  rm -f "$boot_c" "$boot_bin" "$stdout_file" "$ROOT"/$(basename "$source").c
  "$BOOT_BIN" "$source" "$boot_c" >/dev/null
  gcc -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror "$boot_c" -o "$boot_bin"
  printf '%s' "$input" | "$boot_bin" >"$stdout_file"
  diff -u <(printf '%s' "$expected") "$stdout_file"
  printf 'PASS %s (stdin/stdout)\n' "$label"
}

expect_runtime_failure_with_input() {
  local source=$1 label=$2 input=$3 expected_code=$4
  track_source "$source"
  local boot_c="$OUT/${label}.boot.c"
  local boot_bin="$OUT/${label}.boot.bin"
  rm -f "$boot_c" "$boot_bin" "$ROOT"/$(basename "$source").c
  "$BOOT_BIN" "$source" "$boot_c" >/dev/null
  gcc -std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror "$boot_c" -o "$boot_bin"
  set +e
  printf '%s' "$input" | "$boot_bin" >"$OUT/${label}.stdout" 2>"$OUT/${label}.stderr"
  local actual_code=$?
  set -e
  if [[ "$actual_code" -ne "$expected_code" ]]; then
    echo "FAIL $label: expected runtime exit $expected_code, got $actual_code" >&2
    return 1
  fi
  printf 'PASS %s (runtime rejected)\n' "$label"
}

expect_reject() {
  local source=$1 label=$2
  track_source "$source"
  local boot_log="$OUT/${label}.boot.log"
  rm -f "${source}.c" "$OUT/${label}.boot.c"
  if "$BOOT_BIN" "$source" "$OUT/${label}.boot.c" >"$boot_log" 2>&1; then
    echo "FAIL $label: Bootstrap accepted invalid source" >&2
    return 1
  fi
  test ! -e "${source}.c" && test ! -e "$OUT/${label}.boot.c"
  printf 'PASS %s (rejected)\n' "$label"
}

expect_import_reject() {
  local source=$1 label=$2 code=$3 diagnostic_file=$4 target=$5
  expect_reject "$source" "$label"
  local boot_log="$OUT/${label}.boot.log"
  grep -Fq "diagnostic.code=${code}" "$boot_log"
  grep -Fq "diagnostic.file=${diagnostic_file}" "$boot_log"
  grep -Fq "diagnostic.target=${target}" "$boot_log"
  printf 'PASS %s import diagnostic contract\n' "$label"
}

expect_collision_reject() {
  local source=$1 label=$2
  expect_reject "$source" "$label"
  grep -Fq 'distinct functions collide after C mangling' "$OUT/${label}.boot.log"
}

prefix_import_checks() {
  local prefix_root="$ROOT/.tmp/prefix-regression"
  local vendor_source="$prefix_root/.basalt/vendor/demo/0.1.0/src/prefix_installed_lib.basalt"
  local stdlib_c="$OUT/import_prefix_stdlib.boot.c"
  local stdlib_bin="$OUT/import_prefix_stdlib.boot.bin"
  local lib_c="$OUT/import_prefix_lib.boot.c"
  local lib_bin="$OUT/import_prefix_lib.boot.bin"
  rm -rf "$prefix_root"
  mkdir -p "$(dirname "$vendor_source")"
  cp "$ROOT/tests/regression/prefix_installed_lib.basalt" "$vendor_source"
  (cd "$ROOT" && "$BOOT_BIN" --no-line "$ROOT/tests/regression/import_prefix_stdlib_valid.basalt" "$stdlib_c" >/dev/null)
  gcc "${STRICT_FLAGS[@]}" "$stdlib_c" -o "$stdlib_bin"
  "$stdlib_bin"
  (cd "$prefix_root" && "$BOOT_BIN" --no-line "$ROOT/tests/regression/import_prefix_lib_valid.basalt" "$lib_c" >/dev/null)
  gcc "${STRICT_FLAGS[@]}" "$lib_c" -o "$lib_bin"
  "$lib_bin"
  printf 'PASS import prefix resolution (@stdlib and @lib)\n'
}

compile_run "$ROOT/tests/stress/modulo_stress.basalt" modulo_stress
compile_run "$ROOT/tests/regression/stdlib_growth_test.basalt" stdlib_growth_test
compile_run "$ROOT/tests/regression/stdlib_containers_test.basalt" stdlib_containers_test
compile_run_with_output "$ROOT/tests/regression/arena_cycle_test.basalt" arena_cycle_test $'21\n'
compile_run_with_output "$ROOT/tests/regression/arena_nested_parent_child_test.basalt" arena_nested_parent_child_test $'47\n'
compile_run_with_output "$ROOT/tests/regression/arena_cycle_stress_test.basalt" arena_cycle_stress_test $'1\n'
expect_runtime_failure_with_input "$ROOT/tests/regression/arena_use_after_free_runtime_invalid.basalt" arena_use_after_free_runtime_invalid '' 2
compile_run "$ROOT/tests/regression/option_test.basalt" option_test
compile_run "$ROOT/tests/regression/option_result_combinators_test.basalt" option_result_combinators_test
compile_run "$ROOT/tests/regression/stdlib_stabilization_test.basalt" stdlib_stabilization_test
compile_run "$ROOT/tests/regression/stdlib_stabilization_edge_test.basalt" stdlib_stabilization_edge_test
compile_run "$ROOT/tests/regression/result_error_convention_test.basalt" result_error_convention_test
expect_reject "$ROOT/tests/regression/deprecated_result_option_invalid.basalt" deprecated_result_option_invalid
compile_run "$ROOT/tests/super/stdlib_matrix_valid.basalt" stdlib_matrix_valid
compile_run "$ROOT/tests/super/integer_pointer_boundary_valid.basalt" integer_pointer_boundary_valid
compile_run "$ROOT/tests/super/closure_generic_nested_valid.basalt" closure_generic_nested_valid
compile_run_with_output "$ROOT/tests/super/print_stream_valid.basalt" print_stream_valid $'Basalt-2026\nline-two\n42\n'
compile_run "$ROOT/tests/regression/string_builder_iter_test.basalt" string_builder_iter_test
compile_run "$ROOT/tests/regression/stdlib_filesystem_path_string_test.basalt" stdlib_filesystem_path_string_test
compile_run "$ROOT/tests/regression/stdlib_utf8_test.basalt" stdlib_utf8_test
compile_run "$ROOT/tests/regression/stdlib_time_process_format_random_test.basalt" stdlib_time_process_format_random_test
compile_run "$ROOT/tests/regression/numeric_compound_test.basalt" numeric_compound_test
compile_run "$ROOT/tests/regression/f32_f64_test.basalt" f32_f64_test
compile_run "$ROOT/tests/regression/block_comment_numeric_test.basalt" block_comment_numeric_test
compile_run "$ROOT/tests/regression/generic_float_bound_nested_test.basalt" generic_float_bound_nested_test
compile_run "$ROOT/tests/regression/generic_callback_borrow_lifecycle_test.basalt" generic_callback_borrow_lifecycle_test
compile_run "$ROOT/tests/regression/compound_assignment_side_effect_test.basalt" compound_assignment_side_effect_test
compile_run "$ROOT/tests/regression/source_mapping_test.basalt" source_mapping_test
check_cli_modes
compile_run_with_output "$ROOT/tests/regression/sys_process_test.basalt" sys_process_test $'0\n1\nhello world|quote"value\n\n0\n0\n'
build_process_probe
compile_run "$ROOT/tests/regression/sys_process_portable_test.basalt" sys_process_portable_test
check_windows_process_compile
auto_compile_cli
compile_run "$ROOT/tests/regression/tagged_union_test.basalt" tagged_union_test
compile_run "$ROOT/tests/regression/defer_test.basalt" defer_test
compile_run "$ROOT/tests/regression/defer_loop_control_test.basalt" defer_loop_control_test
compile_run "$ROOT/tests/regression/defer_return_if_test.basalt" defer_return_if_test
compile_run "$ROOT/tests/regression/defer_return_expression_order_test.basalt" defer_return_expression_order_test
expect_reject "$ROOT/tests/regression/defer_single_statement_invalid.basalt" defer_single_statement_invalid
grep -Fq 'diagnostic.code=77' "$OUT/defer_single_statement_invalid.boot.log"
compile_run "$ROOT/tests/regression/match_test.basalt" match_test
compile_run "$ROOT/tests/regression/match_default_test.basalt" match_default_test
compile_run "$ROOT/tests/regression/match_default_chain_test.basalt" match_default_chain_test
compile_run "$ROOT/tests/regression/match_arm_defer_test.basalt" match_arm_defer_test
compile_run "$ROOT/tests/regression/match_constructor_subject_test.basalt" match_constructor_subject_test
compile_run "$ROOT/tests/regression/match_nested_constructor_defer_test.basalt" match_nested_constructor_defer_test
compile_run "$ROOT/tests/regression/match_nested_loop_defer_test.basalt" match_nested_loop_defer_test
compile_run "$ROOT/tests/regression/match_branch_move_test.basalt" match_branch_move_test
compile_run "$ROOT/tests/regression/if_match_and_tuple_branch_test.basalt" if_match_and_tuple_branch_test
expect_reject "$ROOT/tests/regression/if_declaration_branch_invalid.basalt" if_declaration_branch_invalid
grep -Fq 'diagnostic.code=78' "$OUT/if_declaration_branch_invalid.boot.log"
expect_reject "$ROOT/tests/regression/if_tuple_branch_invalid.basalt" if_tuple_branch_invalid
grep -Fq 'diagnostic.code=78' "$OUT/if_tuple_branch_invalid.boot.log"
compile_run "$ROOT/tests/regression/for_continue_step_test.basalt" for_continue_step_test
compile_run "$ROOT/tests/regression/tuple_test.basalt" tuple_test
compile_run "$ROOT/tests/regression/const_array_dimension_test.basalt" const_array_dimension_test
compile_run "$ROOT/tests/regression/concurrency_test.basalt" concurrency_test
compile_run "$ROOT/tests/regression/stdlib_concurrency_extended_test.basalt" stdlib_concurrency_extended_test
compile_run "$ROOT/tests/regression/aligned_alloc_test.basalt" aligned_alloc_test
compile_run "$ROOT/tests/regression/fixed_width_integer_test.basalt" fixed_width_integer_test
compile_run "$ROOT/tests/regression/integer_literal_boundary_test.basalt" integer_literal_boundary_test
compile_run "$ROOT/tests/regression/integer_literal_usize_boundary_test.basalt" integer_literal_usize_boundary_test
expect_reject "$ROOT/tests/regression/integer_literal_overflow_invalid.basalt" integer_literal_overflow_invalid
expect_reject "$ROOT/tests/regression/integer_literal_u64_overflow_invalid.basalt" integer_literal_u64_overflow_invalid
expect_reject "$ROOT/tests/regression/integer_literal_u8_overflow_invalid.basalt" integer_literal_u8_overflow_invalid
expect_reject "$ROOT/tests/regression/integer_literal_global_invalid.basalt" integer_literal_global_invalid
expect_reject "$ROOT/tests/regression/integer_literal_indirect_invalid.basalt" integer_literal_indirect_invalid
expect_reject "$ROOT/tests/regression/f32_f64_mismatch_invalid.basalt" f32_f64_mismatch_invalid
expect_reject "$ROOT/tests/regression/fixed_width_invalid_generic.basalt" fixed_width_invalid_generic
expect_reject "$ROOT/tests/regression/aligned_invalid_power.basalt" aligned_invalid_power
expect_reject "$ROOT/tests/regression/concurrency_invalid_atomic.basalt" concurrency_invalid_atomic
expect_reject "$ROOT/tests/regression/concurrency_invalid_callback.basalt" concurrency_invalid_callback
expect_reject "$ROOT/tests/regression/tagged_union_arity_invalid.basalt" tagged_union_arity_invalid
expect_reject "$ROOT/tests/regression/match_non_exhaustive.basalt" match_non_exhaustive
expect_reject "$ROOT/tests/regression/match_payload_arity_invalid.basalt" match_payload_arity_invalid
expect_reject "$ROOT/tests/regression/tuple_binding_count_invalid.basalt" tuple_binding_count_invalid
expect_reject "$ROOT/tests/regression/tagged_union_type_invalid.basalt" tagged_union_type_invalid
expect_reject "$ROOT/tests/super/generic_element_mismatch_invalid.basalt" generic_element_mismatch_invalid
expect_reject "$ROOT/tests/super/generic_callback_mismatch_invalid.basalt" generic_callback_mismatch_invalid
compile_run_with_input "$ROOT/tests/regression/io_safe_test.basalt" io_safe_test $'42\nbad-number\nBasalt-OVERFLOW\nok\n' $'safe-io\n42\nBasalt-\n'
compile_run_with_input "$ROOT/tests/regression/io_safe_edge_test.basalt" io_safe_edge_test $'-17\n999999999999999999999999999999999999999999999\n' ''
expect_runtime_failure_with_input "$ROOT/tests/regression/io_invalid_limit.basalt" io_invalid_limit '' 2
compile_run "$ROOT/tests/regression/builtin_join_test.basalt" builtin_join_test
compile_run "$ROOT/tests/regression/stdlib_slice_only_test.basalt" stdlib_slice_only_test
compile_run "$ROOT/tests/regression/stdlib_map_only_test.basalt" stdlib_map_only_test
compile_run "$ROOT/tests/regression/stdlib_hashing_test.basalt" stdlib_hashing_test
compile_run "$ROOT/tests/regression/map_bucket_mask_edge.basalt" map_bucket_mask_edge
compile_run "$ROOT/tests/stress/map_bucket_mask_stress.basalt" map_bucket_mask_stress
compile_run "$ROOT/tests/regression/stress_containers_loop.basalt" stress_containers_loop
compile_run "$ROOT/tests/regression/generic_map_probe.basalt" generic_map_probe
compile_run "$ROOT/tests/regression/generic_explicit_args_test.basalt" generic_explicit_args_test
compile_run "$ROOT/tests/regression/include_test_main.basalt" include_test_main
compile_run "$ROOT/tests/regression/long_include_line_test.basalt" long_include_line_test
expect_import_reject "$ROOT/tests/regression/include_cycle_a.basalt" include_cycle 62 "$ROOT/tests/regression/include_cycle_b.basalt" "$ROOT/tests/regression/include_cycle_a.basalt"
expect_import_reject "$ROOT/tests/regression/include_alias_cycle_a.basalt" include_alias_cycle 62 "$ROOT/tests/regression/include_alias_cycle_b.basalt" "$ROOT/tests/regression/include_alias_cycle_a.basalt"
expect_import_reject "$ROOT/tests/regression/include_missing_module.basalt" include_missing_module 63 "$ROOT/tests/regression/include_missing_module.basalt" "$ROOT/tests/regression/include_missing_target.basalt"
expect_reject "$ROOT/tests/regression/import_prefix_traversal_invalid.basalt" import_prefix_traversal_invalid
grep -Fq 'diagnostic.code=64' "$OUT/import_prefix_traversal_invalid.boot.log"
prefix_import_checks
compile_run "$ROOT/tests/regression/extern_ffi_test.basalt" extern_ffi_test
compile_run "$ROOT/tests/regression/stress_ffi_loop.basalt" stress_ffi_loop
compile_run "$ROOT/tests/spec/valid/controlled_ffi_valid.basalt" controlled_ffi_valid
compile_run "$ROOT/tests/spec/valid/closure_valid.basalt" closure_valid
compile_run "$ROOT/tests/regression/ffi_c_abi_matrix_valid.basalt" ffi_c_abi_matrix_valid
compile_run "$ROOT/tests/regression/ffi_named_struct_valid.basalt" ffi_named_struct_valid
if [[ "$(grep -F -c '#include "stdlib.h"' "$OUT/controlled_ffi_valid.boot.c")" -ne 1 ]]; then
  echo 'FAIL controlled_ffi_valid: duplicate controlled stdlib.h header' >&2
  exit 1
fi
if [[ "$(grep -F -c '#include "stdint.h"' "$OUT/controlled_ffi_valid.boot.c")" -ne 1 ]]; then
  echo 'FAIL controlled_ffi_valid: missing or duplicate controlled stdint.h header' >&2
  exit 1
fi
printf 'PASS controlled_ffi_valid header deduplication guard\n'
expect_reject "$ROOT/tests/spec/invalid/ffi_unsafe_type_invalid.basalt" ffi_unsafe_type_invalid
expect_reject "$ROOT/tests/spec/invalid/ffi_unsafe_return_invalid.basalt" ffi_unsafe_return_invalid
expect_reject "$ROOT/tests/spec/invalid/ffi_bad_header_invalid.basalt" ffi_bad_header_invalid
expect_reject "$ROOT/tests/spec/invalid/ffi_nested_named_invalid.basalt" ffi_nested_named_invalid
grep -Fq 'diagnostic.code=55' "$OUT/ffi_nested_named_invalid.boot.log"
expect_reject "$ROOT/tests/spec/invalid/ffi_nested_array_invalid.basalt" ffi_nested_array_invalid
grep -Fq 'diagnostic.code=55' "$OUT/ffi_nested_array_invalid.boot.log"
expect_reject "$ROOT/tests/spec/invalid/ffi_closure_tuple_invalid.basalt" ffi_closure_tuple_invalid
grep -Fq 'diagnostic.code=55' "$OUT/ffi_closure_tuple_invalid.boot.log"
expect_reject "$ROOT/tests/spec/invalid/ffi_move_invalid.basalt" ffi_move_invalid
grep -Fq 'diagnostic.code=65' "$OUT/ffi_move_invalid.boot.log"
expect_reject "$ROOT/tests/spec/invalid/ffi_borrow_scalar_invalid.basalt" ffi_borrow_scalar_invalid
grep -Fq 'diagnostic.code=66' "$OUT/ffi_borrow_scalar_invalid.boot.log"
expect_reject "$ROOT/tests/spec/invalid/ffi_borrowed_free_invalid.basalt" ffi_borrowed_free_invalid
grep -Fq 'diagnostic.code=67' "$OUT/ffi_borrowed_free_invalid.boot.log"
expect_reject "$ROOT/tests/spec/invalid/closure_escape_invalid.basalt" closure_escape_invalid
expect_reject "$ROOT/tests/spec/invalid/closure_move_after_capture_invalid.basalt" closure_move_after_capture_invalid
compile_run "$ROOT/tests/regression/print_pointer_test.basalt" print_pointer_test
grep -Fq '%p' "$OUT/print_pointer_test.boot.c"
grep -Fq '(void*)' "$OUT/print_pointer_test.boot.c"
printf 'PASS print_pointer_test format guard\n'
compile_run "$ROOT/tests/regression/namespace_collision.basalt" namespace_collision
compile_run "$ROOT/tests/regression/nested_namespace_valid.basalt" nested_namespace_valid
compile_run "$ROOT/tests/regression/namespace_lexical_parent_valid.basalt" namespace_lexical_parent_valid
expect_reject "$ROOT/tests/regression/namespace_ambiguous_root_invalid.basalt" namespace_ambiguous_root_invalid
grep -Fq 'diagnostic.code=41' "$OUT/namespace_ambiguous_root_invalid.boot.log"
compile_run "$ROOT/tests/regression/namespace_global.basalt" namespace_global
compile_run "$ROOT/tests/regression/stdout_stderr_identifier_test.basalt" stdout_stderr_identifier_test
compile_run "$ROOT/tests/regression/pointer_struct_field.basalt" pointer_struct_field
compile_run "$ROOT/tests/regression/pointer_generic_struct_test.basalt" pointer_generic_struct_test
compile_run "$ROOT/tests/regression/fixed_array_valid.basalt" fixed_array_valid
expect_reject "$ROOT/tests/regression/fixed_array_oob_literal.basalt" fixed_array_oob_literal
expect_reject "$ROOT/tests/regression/fixed_array_oob_negative.basalt" fixed_array_oob_negative
compile_run "$ROOT/tests/regression/enum_typechecker_valid.basalt" enum_typechecker_valid
compile_run "$ROOT/tests/regression/plain_enum_match_test.basalt" plain_enum_match_test
expect_reject "$ROOT/tests/regression/enum_typechecker_invalid.basalt" enum_typechecker_invalid
expect_reject "$ROOT/tests/regression/fixed_array_oob_struct_field.basalt" fixed_array_oob_struct_field
expect_collision_reject "$ROOT/tests/regression/mangle_collision.basalt" mangle_collision
expect_collision_reject "$ROOT/tests/regression/nested_namespace_flat_collision.basalt" nested_namespace_flat_collision
expect_collision_reject "$ROOT/tests/regression/nested_namespace_segment_collision.basalt" nested_namespace_segment_collision
expect_reject "$ROOT/tests/stress/modulo_invalid_string.basalt" modulo_invalid_string
expect_reject "$ROOT/tests/regression/undefined_function_call.basalt" undefined_function_call
expect_reject "$ROOT/tests/regression/non_function_value_call.basalt" non_function_value_call
expect_reject "$ROOT/tests/regression/generic_explicit_arity_invalid.basalt" generic_explicit_arity_invalid
grep -Fq 'diagnostic.code=76' "$OUT/generic_explicit_arity_invalid.boot.log"
expect_reject "$ROOT/tests/regression/explicit_non_generic_invalid.basalt" explicit_non_generic_invalid
grep -Fq 'diagnostic.code=76' "$OUT/explicit_non_generic_invalid.boot.log"
expect_reject "$ROOT/tests/regression/unknown_variable_use.basalt" unknown_variable_use
expect_reject "$ROOT/tests/regression/unknown_field_access.basalt" unknown_field_access
expect_reject "$ROOT/tests/regression/deref_non_pointer.basalt" deref_non_pointer
expect_reject "$ROOT/tests/regression/index_non_container.basalt" index_non_container
expect_reject "$ROOT/tests/regression/indirect_call_non_function.basalt" indirect_call_non_function
expect_reject "$ROOT/tests/regression/reserved_runtime_function.basalt" reserved_runtime_function
# The package-manager implementation and its Python suite were removed by the
# upstream repository update; the remaining compiler regression gates stay active.
printf 'Bootstrap-only regression checks completed successfully.\n'
