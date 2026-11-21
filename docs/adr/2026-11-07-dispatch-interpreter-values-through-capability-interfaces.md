# Dispatch interpreter values through capability interfaces, and evaluate missing values to false

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `b6cf357` (#221, issue #143). The interpreter's
object model already used interfaces: `ContainerObject` has been there since the
extraction (`1a89b93`), and `AppendableObject` and `DetachableObject` came with
the ADD and DELETE actions (`f8b4caa`, `1047b72`). #221 made the rule explicit.
It added `PrefixObject` so that `begins_with` dispatches through an interface,
and it fixed how conditions treat missing data.

The problem in #221: condition, filter and projection expressions failed the
whole request when a function or comparison met a NULL, a missing attribute or
a value of another type (`contains is not supported for path=NULL`,
`invalid type NULL`). DynamoDB evaluates those to false. "DynamoDB only rejects
an expression for what it can see without reading the item."

Maintainer review of #221 asked why the code cast to `*string` instead of
following the interface design.

Facts on this branch:

- `grep -c 'Object interface' interpreter/language/object.go` prints `6`: the
  base `Object` plus the five capability interfaces `ContainerObject`,
  `PrefixObject`, `SizableObject`, `AppendableObject` and `DetachableObject`.
- `git grep -n 'PrefixObject' -- '*.go' ':!*_test.go' | wc -l` prints `3`:
  the interface (with its comment) and the one dispatch, in `begins_with`
  (`interpreter/language/functions.go`). `String` and `Binary` implement it.

## Decision

- Values are `language.Object`s. What an object can do is an interface it
  implements, in `interpreter/language/object.go`. A new built-in or operator
  dispatches through such an interface, or adds one when none fits. The rule is
  no concrete casts or type switches where an interface fits (maintainer
  review).
- A path that does not resolve evaluates to `UNDEFINED`. In conditions,
  functions and comparisons return `false` (or no size) for a missing value or a
  value of the wrong type read from the item. They return an error only when the
  type is known from the expression alone, such as a `:value` of the wrong type.
- Evaluation returns `*language.Error` objects, never panics and never uses Go
  errors.
- A concrete assertion is fine where that type is the only one possible, such
  as arithmetic on `*Number`.

The alternatives were considered and rejected:

- **Cast to the concrete type at each call site.** Maintainer review of #221
  rejected the `*string` cast against the interface design. The reason stated in
  `docs/architecture.md` is that an interface keeps each type's behavior next to
  the type, and adding a type stays a local change.
- **Skip the interface for speed.** `docs/architecture.md` records that the
  interface call costs nothing measurable next to the rest of an evaluation. No
  benchmark is cited.
- **Fail the request on mismatched data.** This was the behavior before #221 and
  it broke `contains`, `begins_with`, `size`, comparisons, `BETWEEN`, `IN` and
  projections on ordinary data.

## Consequences

**Positive.** Adding a type or an operator is a local change. Ordinary data with
NULLs or missing attributes no longer breaks requests.

**Negative.** #221 changed behavior callers could see (release note):
expressions that errored on NULL or missing data now return false or omit the
path, `SET x = <missing path>` now fails instead of storing NULL, and `REMOVE`
under a missing or wrong-type parent now fails. The boundary between "static
error" and "false" needs care: it depends on whether the type is knowable from
the expression alone.

**Neutral.** Some switches over concrete types remain where only one type is
possible.

## References

- `interpreter/language/object.go`, `interpreter/language/evaluator.go`
- `docs/architecture.md` (D5)
- #143, #221
