# Basalt Language Specification

## Status and scope

This document specifies the behavior implemented by the Bootstrap Basalt compiler in `src/bootstrap/basaltc.basalt`. It is a compatibility baseline for the current language, not a proposal for features that are not yet implemented. A compiler change that intentionally changes one of these rules must update this specification and add or revise conformance fixtures in the same change.

The specification is organized around observable source behavior: lexical rules, expressions, statements, types, ownership, control flow, modules, FFI boundaries, generated C, and diagnostics. The frozen generated compiler seed is an implementation artifact and is not itself the language specification.

## Compilation model

Basalt is a statically checked language that emits C11. Compilation has the following conceptual stages:

```text
source text
  → lexical analysis and parsing
  → AST construction
  → include/import expansion
  → name and type checking
  → ownership, borrow, and lifetime checking
  → generic specialization
  → C11 emission
  → host C compilation
```

A valid Basalt program must pass all compiler checks before generated C is considered an executable translation. The Bootstrap compiler reports the first fatal semantic diagnostic and does not produce a valid output program for a rejected source file.

## Lexical and source rules

Source files are UTF-8-compatible byte sequences for purposes of file loading, but the current string implementation is byte-oriented. Character literals represent one character value, while strings are mutable or immutable runtime-managed string values according to the standard-library API in use. Escape processing is performed by the lexer for the supported quote, slash, newline, carriage-return, tab, and related escape forms.

Source locations are one-based. A diagnostic line starts at one, and a diagnostic column resets to one after each newline. The compiler preserves source-file identity through include expansion so diagnostics can identify the originating file.

## Types

The current type universe includes primitive scalar types, pointers, arrays, dynamic generic arrays, named structs, tagged enums, tuples, function values, closures, generic parameters, and type-level generic forms.

| Category | Current types |
|---|---|
| Integer | `int`, `long`, `long long`, `u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`, `usize` |
| Floating point | `f32`, `f64` |
| Other primitives | `bool`, `char`, `string`, `void` |
| Composite | pointers, fixed arrays, dynamic arrays, structs, tuples, tagged enums |
| Callable | functions, function pointers, closures |
| Generic | type parameters and generic container/function forms |

Assignment, initialization, return, function arguments, array elements, and field writes are checked using the complete type shape. In particular, element types and nested pointer depth are not erased when comparing values.

Numeric literal conversion is permitted only when the literal fits the target type and the conversion is part of the implemented type contract. An integer literal that exceeds the target representation is rejected at compile time.

## Expressions and evaluation order

Expressions include literals, names, field access, indexing, pointer operations, calls, constructors, tuples, unary operators, binary operators, comparisons, logical operators, bitwise operators, shifts, and compound assignments.

Operator precedence is represented in the AST before C emission. A compound assignment is a single-evaluation operation: the left-hand place must not be expanded into a duplicated expression that evaluates a function call, index expression, or other side effect twice. The generated C may use the native compound-assignment operator when the target place and operator are valid.

Function arguments are checked against their declared parameters. Generic arguments must match the declared generic parameter count and constraints. A call to a non-function value, an unknown function, or a function with an incompatible argument list is rejected.

## Statements and control flow

Basalt supports blocks, declarations, assignments, expression statements, returns, conditionals, `while`, `for`, `break`, `continue`, `defer`, printing, and pattern matching.

`while` and `for` require block bodies. A `for` statement contains an optional initializer, a condition, a block body, and an optional step. The step is emitted in the C `for` increment clause so `continue` preserves C loop semantics.

A bare `if` branch may not be a direct `defer`, local declaration, or tuple binding. These forms are valid inside a block-scoped branch when the grammar permits them. `break` and `continue` are valid only inside a loop.

`match` operates on tagged enum values. Arms may bind payloads, must respect payload arity, may contain nested constructors, and must satisfy the implemented exhaustiveness and duplicate-arm rules. A default arm is unique and must be last.

## Functions, structs, enums, and generics

Functions declare parameter types and a return type. Non-void functions must return a value compatible with the declared return type on all required paths. Struct fields are named and checked at declaration, construction, access, and assignment sites.

Tagged enum constructors carry a tag and zero or more payload fields. Nested constructor expressions preserve the active enum context while their arguments are checked. Plain enum matching and payload-bearing matching follow the same subject and arm-flow rules.

Generic functions and containers are specialized from checked type arguments. Specialization must preserve element types, pointer depth, ownership metadata, and callback signatures. A generic operation is rejected when an explicit type argument count or element type does not match its declaration.

## Ownership, borrowing, and lifetimes

Owned values cannot be copied implicitly. An explicit move transfers ownership and invalidates the source binding for subsequent use. Releasing a value requires an owned value and is rejected for a borrowed or already-moved value.

Borrowed values do not transfer ownership. A borrow must remain within the source lifetime and must obey mutable/shared aliasing rules. Mutable access is rejected while an incompatible borrow is active. Branches, loops, and match arms use conservative flow snapshots and merges so an ownership or borrow state from one exclusive path does not incorrectly contaminate another path.

Closures record capture mode and lifetime relationships. A moved capture cannot be reused after move, and a borrowed capture may not escape the source lifetime. Return-borrow analysis requires a consistent source lifetime.

## `defer` and cleanup

`defer` registers a cleanup expression in the current emitter scope. Cleanup executes in last-in-first-out order when the scope exits, including returns and terminal control-flow paths. A return expression is materialized before active cleanup is emitted; therefore cleanup cannot mutate the value already selected for return.

Deferred expressions are evaluated at cleanup time under the current implementation. This is an explicit semantic rule and must not be silently changed to argument capture without a specification and compatibility update.

## Memory and arenas

The generated runtime tracks allocations in an indexed registry. The registry is an allocation-safety boundary; it is not a tracing garbage collector and does not infer pointer-field graphs.

The standard-library arena module provides deterministic group lifetime for cyclic pointer graphs. `arena::child(parent)` creates a nested arena. Freeing a child detaches and releases that child group; freeing a parent releases still-attached descendants before the parent group. Pointers into a freed arena are invalid and must not be used.

## Modules, include, and imports

`include` expands Basalt source recursively and tracks canonical dependency edges. Dependency cycles and missing or invalid targets are rejected. `includec` copies controlled raw C content into the generated translation unit and is a low-level boundary rather than ordinary type-safe Basalt code.

The standard-library and installed-library prefixes resolve through the configured import roots. Path traversal outside those roots is rejected. A clean checkout must contain all marker files and fixtures required for deterministic include resolution.

## FFI and generated C

FFI declarations are restricted to ABI-representable scalar, pointer, fixed-array, and approved named-struct types. Header paths are validated. Borrow, move, and release behavior at the FFI boundary is checked separately from ordinary Basalt ownership.

The emitter produces C11 and must pass the repository’s strict GCC and Clang flags. MinGW object checks, AddressSanitizer, UndefinedBehaviorSanitizer, Valgrind Memcheck, and fixed-point checks are additional evidence for the applicable change; they do not replace semantic conformance tests.

## Conformance categories

The repository conformance corpus is organized into positive and negative fixtures under `tests/spec/`, with additional regression and stress fixtures under `tests/`.

| Category | Expected result |
|---|---|
| Valid source | Bootstrap accepts, C compiles, and runtime output matches the fixture contract. |
| Invalid source | Bootstrap rejects with the documented diagnostic category/code. |
| Ownership safety | Invalid moves, escapes, aliasing, or releases are rejected or controlled by the documented runtime contract. |
| Generated-C safety | Strict host compilation and applicable sanitizer/Memcheck gates pass. |
| Fixed point | Repeated compiler generation stabilizes with `n3.c == n4.c`. |

## Compatibility policy

Behavior not specified here is not a stable language guarantee. A change to parser acceptance, type compatibility, ownership flow, diagnostic code, generated-C evaluation order, runtime cleanup, or module resolution must include a specification update and a regression or conformance fixture. The Bootstrap source remains the implementation authority during the current self-hosting phase.
