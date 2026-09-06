# CLAUDE.md — Basalt Bootstrap Compiler

## Purpose and authority

This file defines the working contract for maintaining the self-hosting Bootstrap Basalt compiler. It is repository guidance for compiler changes, regression work, audits, generated-output verification, and documentation. When this file conflicts with the active source or an explicit project decision, the current repository source and the explicit project decision take precedence; any resulting policy change must be documented in the same change.

The active compiler source is [`src/bootstrap/basaltc.basalt`](src/bootstrap/basaltc.basalt). The frozen generated seed is [`src/bootstrap/basaltc.seed.c`](src/bootstrap/basaltc.seed.c). The recorded seed digest is [`src/bootstrap/fixed_point_production.sha256`](src/bootstrap/fixed_point_production.sha256).

> **Bootstrap-only rule:** during Bootstrap audits and compiler feature work, modify and validate `src/bootstrap/basaltc.basalt` and its Bootstrap fixtures only. Do not edit, build, or use `src/compiler/` as an implementation or validation shortcut.

## Working model

Every task should be handled as a reproducible engineering change rather than as an informal source edit. The same standard applies to parser work, type checking, ownership, emitter behavior, include expansion, runtime safety, and documentation.

| Area | Source of truth | Required evidence | Result that must be reported |
|---|---|---|---|
| Compiler implementation | `src/bootstrap/basaltc.basalt` | Focused reproducer and fresh Bootstrap build | Source change, semantic reason, and observed behavior. |
| Frozen compiler state | `src/bootstrap/basaltc.seed.c` and its SHA-256 file | Checksum comparison and fixed-point verification | Whether the seed is unchanged, promoted, or intentionally pending. |
| Language behavior | Positive and negative fixtures under `tests/` | Bootstrap compile result, diagnostic code, generated C, and runtime result where applicable | Whether behavior is valid, rejected, conservative, unsupported, or a confirmed bug. |
| Language and diagnostics | `docs/spec/` | Specification review and diagnostic-contract harness | Which language rules, diagnostic fields, and compatibility codes are covered. |
| Generated C | Bootstrap output | Strict GCC/Clang compilation and sanitizer execution when relevant | Whether the generated translation unit is portable and memory-safe for the tested case. |
| Documentation and diagrams | `CLAUDE.md` and `docs/architecture/` | Source-anchor or repository-state check | Which implementation facts are documented and which boundaries remain selected or omitted. |

## Repository scope and pipeline

The Bootstrap source contains the lexer, parser and AST construction, type checker, ownership and borrow analysis, generic specialization, C emitter, include expansion, runtime emission, and command-line driver. The generated runtime is written as C text by the emitter and is separate from the user-program token buffer.

The normal self-hosting pipeline is:

```text
src/bootstrap/basaltc.seed.c
  → trusted seed executable
  → Bootstrap compiler stage
  → stage2 C and executable
  → current Bootstrap compiler
  → generated C11 translation unit
  → GCC or Clang executable
```

The production fixed-point procedure extends this to successive generated compiler stages and must verify that the stable output repeats. In the current repository, the required stability condition is `n3.c == n4.c`, followed by a checksum comparison between the promoted seed and `fixed_point_production.sha256`.

| Pipeline boundary | Responsibility | Important invariant |
|---|---|---|
| Frozen seed → stage | Bootstraps the current compiler without using `src/compiler/`. | The seed executable must be verified before trusting its output. |
| Bootstrap source → generated C | Parses, checks, collects, and emits the user program. | The generated C must reflect checked Basalt semantics rather than an unvalidated AST. |
| Generated C → host executable | Uses a strict C compiler and, when needed, sanitizer instrumentation. | GCC, Clang, and portability targets must not report diagnostics under the declared gate. |
| Candidate stages → fixed point | Confirms that compiler generation has stabilized. | `n3.c` and `n4.c` must be byte-identical before promotion. |

## Standard verification commands

Run commands from the repository root. The canonical gates are:

```bash
bash scripts/run_regression.sh
bash scripts/run_ownership_stress.sh
bash scripts/run_ffi_portability.sh
bash scripts/fixed_point.sh
```

The default strict C compilation flags are:

```text
-std=c11 -Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror
```

A normal validation sequence is staged from narrow to broad. First build a fresh Bootstrap compiler and run the focused reproducer. Then compile the generated C with strict GCC and Clang. Add ASan and UBSan when the change involves ownership, pointers, strings, containers, FFI, process execution, control-flow cleanup, or generated allocation logic. Finally run the full regression, ownership-stress, portability, and fixed-point gates.

| Test kind | Required expectation | Additional evidence |
|---|---|---|
| Positive fixture | Bootstrap accepts, generated C compiles, and the executable produces the expected result. | Capture the output and relevant generated-C fragment. |
| Negative fixture | Bootstrap rejects the input with the intended diagnostic code and does not silently generate a valid-looking program. | Record the diagnostic label, message, source location, and code. |
| Ownership or memory fixture | Compile and run under strict mode and sanitizers. | Distinguish a controlled runtime panic from an allocator or use-after-free failure. |
| Portability fixture | Check GCC, Clang, and configured cross-platform object targets. | Preserve the exact compiler flags and target result. |
| Fixed-point check | Successive compiler outputs stabilize and the seed digest is correct. | Record stage names, byte comparisons, and checksum. |

## Source-change protocol

A compiler change must begin with a minimal reproduction or a clearly stated feature requirement. The implementation must remain in the Bootstrap source. If generated seed content changes, rebuild the relevant stages, run focused and full gates, verify fixed-point stability, and promote the seed only after the candidate has passed those checks.

Before committing any change, run:

```bash
git diff --check
git status --short
```

Only intentional source, test, harness, or documentation files should be staged. Do not commit generated executables, temporary generated C, benchmark output, compiler logs, audit notes, or ignored `.tmp/` artifacts. Do not force-push. Fetch the remote before publishing and push the named audit branch explicitly when working outside the default branch.

## Bug-audit protocol

A suspicious output is not automatically a compiler bug. A disciplined audit must separate source validity, language policy, missing features, runtime safety behavior, and proven miscompilation. The compatibility baseline is [`docs/spec/basalt-language-spec.md`](docs/spec/basalt-language-spec.md), and the structured diagnostic contract is [`docs/spec/diagnostics.md`](docs/spec/diagnostics.md).

| Classification | Evidence required | Correct response |
|---|---|---|
| Confirmed compiler bug | Valid Basalt source produces incorrect semantics, incorrect generated C, or an incorrect diagnostic compared with the language contract. | Preserve a minimal regression fixture, patch only Bootstrap source, and rerun the complete relevant gates. |
| Invalid syntax or invalid program | The source violates the current grammar or semantic contract. | Keep the rejection; improve the diagnostic only if that is the requested behavior. |
| Conservative policy | The checker rejects a construct intentionally to preserve safety even though unrestricted C could execute it. | Document the policy; do not weaken it merely to make a probe pass. |
| Feature gap | The behavior is not defined or implemented by the current language. | Record the gap separately from a bug and add a design decision before implementing it. |
| Runtime safety abort | Compilation succeeds but the generated runtime intentionally stops on a contract violation, such as releasing an untracked pointer. | Verify the exit code and sanitizer result; do not label the behavior a compiler miscompile without a contrary language requirement. |

The minimum reproduction record should contain the input fixture, compiler command, diagnostic or runtime result, generated-C fragment, host compiler result, sanitizer result when relevant, and the classification rationale. A source-looking output is not sufficient evidence by itself. Structured diagnostic changes must also update `scripts/run_diagnostic_contract.sh` and the diagnostic specification.

## Parser and AST contracts

The parser returns identifiers into compiler-managed AST and type-node arrays. Parser failure sentinels are part of the internal contract: expression and statement parsing use a negative failure result, while type parsing uses `0` for malformed or incomplete type syntax. Callers must preserve this distinction.

Expression parsing uses precedence climbing for binary, comparison, logical, bitwise, shift, and compound-assignment operators. The parser must construct a complete right-hand expression before emission. Compound assignment must remain a single-evaluation operation in generated C; it must not be desugared into a form that evaluates a side-effecting left-hand place twice.

The parser requires block bodies for `while` and `for`. Direct `defer`, direct local declaration, and direct tuple binding are not accepted as bare `if` branch bodies. The established invalid-branch diagnostics are code 77 for direct `defer` and code 78 for direct declaration or tuple-binding cases. A valid block may contain those constructs where the grammar permits them.

## Type, ownership, borrow, and lifetime contracts

The type checker annotates expressions and maintains lexical scopes, ownership state, borrow state, generic bindings, closure capture state, field-target state, and control-flow snapshots. Checker functions return `void`; semantic failure is reported through fatal diagnostics rather than an error value.

| Semantic area | Current rule | Audit focus |
|---|---|---|
| Lexical scope | `tc_enter_scope` records a variable boundary and `tc_leave_scope` unwinds local bindings and borrow relationships. | Verify shadowing, scope exit, and borrow cleanup against nested blocks and closures. |
| Ownership move | A consuming call marks an owned variable moved and clears its owned state. | Check repeated moves, non-owned values, active borrows, and branch joins. |
| Borrow state | Borrow counts, mutability, source, parent, parameter, and FFI-borrow metadata are tracked conservatively. | Check escape, conflict, mutation, and scope-exit behavior. |
| Branch flow | `if`, loops, and `match` use baseline snapshots, branch snapshots, restoration, conservative merge, and frame end. | Confirm that one arm’s move or borrow does not contaminate an exclusive sibling arm. |
| Raw boundaries | Raw pointers, pointer arithmetic, `extern`, `includec`, and generated C are low-level boundaries. | Require strict compilation and sanitizer coverage; do not infer safety from type checking alone. |

The current defer ownership edge case is documented rather than silently reclassified: `defer str::free(value);` evaluates at cleanup time, and a second release of the same tracked allocation can trigger the runtime registry’s controlled panic code 2. This is a runtime safety behavior unless the language contract explicitly requires compile-time rejection.

## Runtime allocation and cyclic graph lifetimes

The generated runtime maintains an allocation registry for tracked pointers. The registry uses an open-addressed hash index with tombstones and a compact live-pointer array, so lookup is expected `O(1)` rather than a linear scan. Releasing an allocation uses swap-delete in the live array and updates the moved entry’s hash slot. A successful `realloc` must rebind the registry entry through the saved index and hash slot; it must not reuse the old pointer as if it were still a live allocation.

The registry is an allocation-safety boundary, not a graph collector. It does not inspect pointer fields, infer object edges, count references, or detect arbitrary cycles. Replacing the index structure therefore improves `track`, `find`, `release`, and resize behavior but does not by itself reclaim unreachable cyclic graphs.

For cyclic object graphs, [`src/stdlib/arena.basalt`](src/stdlib/arena.basalt) provides an explicit arena lifetime. `arena::alloc<T>` allocates typed storage and records each block under the arena; `arena::free` releases every block and then the arena handle as a group. Cyclic pointers inside the arena are safe because cleanup follows the allocation list rather than recursively traversing object fields. This is deterministic group-lifetime management, not tracing garbage collection or general cycle detection.

`arena::child(parent)` creates a child arena linked into a parent-child ownership tree. Freeing a child detaches and closes only that child; freeing a parent closes all still-attached descendants before releasing the parent’s own blocks. The runtime rejects invalid or already-freed handles through controlled panic behavior and does not permit a child to remain linked to a released parent. Nested cleanup is measured by the number of descendants and owned blocks, not by user-graph traversal.

The fixture [`arena_leak_probe_test.basalt`](tests/regression/arena_leak_probe_test.basalt) intentionally omits both `arena::free` calls after constructing a cyclic node/edge graph. It is a leak-oriented probe, not a normal leak-free assertion: the current process-level registry cleanup reclaims tracked allocations at exit, so a clean exit does not prove that a long-running service released its arena promptly. Use the probe to inspect generated behavior and distinguish an omitted explicit lifetime boundary from invalid access or compiler-generated cleanup errors.

Arena-owned objects must not be individually passed to `memory_free`, and pointers into a freed arena must not be used afterward. Use ordinary `memory_alloc` when each allocation needs an independent lifetime. If a future ownership design adds `Owned`, `Borrow`, or `Weak` references, it must preserve this distinction and specify whether cycles are rejected statically, managed by a collector, or placed in an explicit arena.

The arena API and registry are currently process-local runtime facilities. The registry is not a replacement for synchronization around concurrent allocation or release; any multithreaded extension must define the required locking or atomicity contract before changing these routines.

## Control flow, match, and defer

`for` is emitted as a C `for`; its step belongs in the C increment clause. Do not clone or emit the step manually before `continue`, because C executes the increment clause after `continue` reaches the loop boundary. `while` and `for` bodies remain block-based in the parser.

`match` emits a temporary subject and a tag-based `if`/`else if`/`else` chain. Each arm has an independent emitter cleanup baseline and C block. A wildcard/default arm must appear at most once and at the end; without a default, the checker requires exhaustive coverage where the language contract demands it. Nested enum constructors must preserve the active enum context while checking nested arguments.

`defer` is stored in an emitter LIFO stack. A block or match arm records a stack baseline, cleanup is emitted in reverse registration order, and control-flow exits flush defers belonging to scopes being exited. A return expression with active cleanup must be materialized before defer emission so cleanup cannot change the returned value. Deferred expressions themselves are evaluated at cleanup time under the current implementation.

The emitter uses termination analysis to avoid cleanup that is statically unreachable after `return`, `break`, `continue`, or a terminal match arm. Generated C that contains ordinary unreachable statements is not automatically a compiler bug; classify it only after strict compilation, runtime execution, and sanitizer behavior have been checked against the intended semantics.

## Include, FFI, portability, and generated C

Recursive `include` expansion resolves a canonical target, records the dependency edge, checks for a cycle, and recursively processes Basalt source. `includec` follows a separate raw-C path and copies the target bytes into the C-source buffer. Include handles are borrowed by the expansion routine; the owner of the root handle remains responsible for closing it.

FFI and raw C are controlled boundaries rather than ordinary type-safe Basalt code. Any change involving `extern`, header registration, `includec`, ABI mapping, platform process execution, allocation, alignment, or thread/runtime shims must be checked with strict GCC and Clang and with the configured portability harness. Do not claim Windows, POSIX, MinGW, or sanitizer compatibility unless that target was actually tested.

Generated C has two output channels. The emitter first writes the runtime prelude and registered FFI headers through `emit_c_file`; it then writes raw C source and the typed token buffer. `gen_program` orchestrates token generation but does not directly serialize the runtime. `--line` enables source mapping directives for generated C, while `--no-line` disables them; preserve the intended mode for the specific test or seed workflow.

## Regression and stress-test policy

Every confirmed bug must receive a focused regression fixture. Complex control-flow, match, defer, ownership, pointer, generic, closure, string, FFI, and recursive-include changes should also receive adversarial or stress coverage where the failure mode could be state-dependent.

A fixture should state whether it is positive or negative, the expected output or diagnostic code, and any required compiler mode. Test additions must be runnable in a clean clone and must not depend on ignored local files. If a fixture depends on an intermediate directory or marker file, commit the minimal marker needed to preserve that directory in a clean checkout.

Performance comparisons are diagnostic evidence, not semantic proof. Report source-to-C generation time, host C compilation time, generated-C size, and runtime measurements separately. Never substitute a performance result for strict compilation, sanitizer execution, or fixed-point verification.

## Documentation and architecture graphs

The selected architecture and function-level artifacts are stored in [`docs/architecture/`](docs/architecture/). The function reference is [`bootstrap-function-reference.md`](docs/architecture/bootstrap-function-reference.md), with its Mermaid source and rendered PNG beside it. The diagrams describe selected functions and grouped external boundaries; they are not a claim that every helper in the 379-function Bootstrap source has been shown individually.

Documentation changes must remain source-grounded. Function anchors should point to current definition lines, caller/callee descriptions should distinguish selected relationships from external boundaries, and generated diagrams should be rendered and visually inspected before publication. Temporary generator scripts and evidence logs may remain under `.tmp/` when they are intentionally ignored, but they must not be presented as tracked deliverables.

## Completion checklist

Before declaring a Bootstrap compiler task complete, verify all applicable items:

| Check | Required question |
|---|---|
| Scope | Did the implementation stay in `src/bootstrap/` and avoid `src/compiler/`? |
| Reproduction | Is there a minimal fixture or an explicit feature contract? |
| Semantics | Were parser, type, ownership, borrow, control-flow, defer, and include effects considered where relevant? |
| Generated C | Was the output inspected and compiled with the declared strict flags? |
| Safety | Were ASan/UBSan and portability checks run when the change could affect them? |
| Regression | Were focused, full, negative, stress, and fixed-point tests run as applicable? |
| Cyclic lifetimes | If pointer graphs can cycle, was the explicit arena/ownership policy tested without recursive cleanup? |
| Seed | If generated compiler source changed, was fixed-point stability and the recorded checksum verified before promotion? |
| Documentation | Were source anchors, graphs, and repository guidance updated consistently? |
| Publishing | Was `git diff --check` run, only intentional files staged, and the non-force push verified remotely? |

## References

[1]: src/bootstrap/basaltc.basalt "Bootstrap Basalt compiler source"
[2]: src/bootstrap/basaltc.seed.c "Frozen generated Bootstrap seed"
[3]: src/bootstrap/fixed_point_production.sha256 "Recorded production seed checksum"
[4]: docs/architecture/bootstrap-function-reference.md "Selected Bootstrap function reference"
[5]: docs/architecture/bootstrap-function-graph.mmd "Selected Bootstrap function graph"
[6]: src/stdlib/arena.basalt "Explicit arena lifetime for cyclic pointer graphs"
[7]: docs/spec/basalt-language-spec.md "Basalt language compatibility specification"
[8]: docs/spec/diagnostics.md "Basalt structured diagnostic contract"
