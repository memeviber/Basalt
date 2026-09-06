# Basalt Diagnostic Contract

## Purpose

Diagnostics are part of the Basalt compatibility surface. A diagnostic is not only human-readable text: its code, source location, structured fields, and rejection behavior are consumed by regression scripts, editor tooling, and downstream automation.

The Bootstrap compiler emits the first fatal diagnostic and exits with a non-zero status. A rejected source file must not be treated as a valid generated program.

## Machine-readable fields

A compiler diagnostic uses one field per line. The current structured fields are:

| Field | Meaning |
|---|---|
| `diagnostic.code` | Stable numeric diagnostic code. `0` is reserved for parser-level fallback diagnostics. |
| `diagnostic.file` | Logical source file containing the reported position, or `<unknown>`. |
| `diagnostic.line` | One-based source line. |
| `diagnostic.column` | One-based source column. |
| `diagnostic.hint` | Action-oriented guidance when a hint is defined; otherwise a generic inspection hint. |
| `diagnostic.expected` | Expected type description for type-aware diagnostics. |
| `diagnostic.found` | Actual type description for type-aware diagnostics. |
| `diagnostic.target` | Include/import target for source-import failures. |
| `diagnostic.excerpt` | Source-line excerpt associated with the reported position. |

Not every diagnostic has every optional field. Type mismatch diagnostics include `expected` and `found` when the checker has both values. Include/import diagnostics include `target` when a target is available.

## Human-readable prefix

The human-readable first line identifies the broad category and message, for example:

```text
type error: invalid function arguments
```

The structured fields follow on subsequent lines. The label and message remain on one line so terminal output and editor integrations can identify the primary failure without parsing multiple lines.

## Position rules

Positions are derived from the source byte offset stored by the parser and checker:

- Lines and columns are one-based.
- A newline increments the line and resets the column to one.
- File identity is preserved through recursive include expansion.
- A missing or invalid position uses the active source file or `<unknown>`.
- The excerpt contains the source line containing the reported position without adding unrelated lines.

## Diagnostic lifecycle

The type checker uses a first-error-wins policy. `tc_fail` records the first error code and source position while preserving the original failure when later checks encounter cascading errors. `tc_fail_types` additionally records expected and found type kinds before entering the same first-error path.

This policy prevents a single malformed expression from producing a misleading list of secondary failures. A future multi-diagnostic mode must be designed separately and must not silently change the current single-diagnostic contract.

## Current diagnostic categories

The current Bootstrap implementation uses the following established categories. The numeric code is stable test/API data; the message and hint should remain semantically aligned with the category.

| Codes | Category |
|---|---|
| 3, 5 | Duplicate declaration and unknown name. |
| 12, 13, 41, 42, 43 | Function calls, argument count, unknown functions, non-function calls, and reserved runtime names. |
| 14, 17, 18 | String, built-in argument, and arithmetic operand errors. |
| 20, 21, 23, 54 | Initializer, assignment, return, and integer-literal range mismatches. |
| 28, 31 | Recursive struct definition and assignment to `const`. |
| 33, 34, 35, 37, 38, 40 | Ownership move, release, borrow conflict, escape, and implicit owned-copy errors. |
| 36, 45 | Array element mismatch and bounds errors. |
| 46, 47, 48, 49, 50, 51 | `defer` result, match subject, variant, duplicate-arm, payload-arity, and exhaustiveness errors. |
| 52, 53 | Tuple type and binding-count errors. |
| 55, 56, 57 | FFI parameter, return-type, and header-path restrictions. |
| 58, 59, 60, 61 | Explicit move, borrow source, closure lifetime, and moved-capture errors. |
| 64 | Import-prefix traversal or source-resolution violation. |
| 65, 66, 67 | FFI ownership, borrow-mode, and release restrictions. |
| 68, 69, 70 | Borrow-place, return-borrow lifetime, and mutable-borrow errors. |
| 72, 73 | Reference return escape and unknown function-pointer ownership signature. |
| 74, 75, 76 | Match default ordering, loop-carried move, and explicit generic-argument errors. |
| 77, 78 | Invalid direct `if` branch forms for `defer`, declarations, and tuple bindings. |

Import failures use a separate source-import contract. The implementation distinguishes dependency cycles, missing targets, invalid targets, and include-line/path failures through the `source_import_fail` kind and its emitted target/excerpt fields.

## Error-code policy

A diagnostic code may be changed only when the language contract intentionally changes. Rewording a message without changing the semantic category should preserve the code. Adding a new diagnostic requires:

1. A source-grounded definition in this document.
2. A positive or negative fixture that proves the intended behavior.
3. A stable expected code assertion in the test harness where appropriate.
4. A hint or explicit explanation of why the generic hint is insufficient.
5. Validation through the Bootstrap compiler, strict C compilation, and applicable sanitizers.

Diagnostic tests must assert the code and relevant structured fields rather than relying only on the human-readable sentence. This keeps tests resilient to editorial improvements while preserving the machine-readable contract.

## Runtime safety diagnostics

Compile-time rejection and runtime safety aborts are distinct. A generated program may intentionally terminate with a controlled runtime code when it releases an untracked pointer, uses a freed arena, or violates another runtime ownership boundary. Such behavior must be tested as a runtime contract and must not be relabeled as a compiler diagnostic unless the source should have been rejected earlier.

Sanitizers, Valgrind Memcheck, and runtime exit-code assertions complement compile-time diagnostics. They answer different questions:

| Evidence | Question |
|---|---|
| Diagnostic code | Did the compiler reject the source for the intended reason? |
| Generated C | Did emission preserve the checked semantics? |
| Strict host compile | Is the generated translation unit portable under the declared flags? |
| Sanitizer | Did execution trigger memory or undefined-behavior instrumentation? |
| Memcheck | Did execution report invalid accesses or definite/indirect leaks? |
| Runtime exit code | Did a deliberate safety boundary terminate as specified? |

## Tooling compatibility

Editors and CI should consume the structured `diagnostic.*` fields. They should not infer line or column from the human-readable message or from the excerpt. The format is intentionally line-oriented and stable for shell-based regression tests, while the human-readable first line remains suitable for interactive terminal output.
