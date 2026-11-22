# Reuse existing code, put new code in the file that owns its concept, and measure before optimizing

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `657bd71` (#223). That commit wrote D10 in
`docs/architecture.md`, and the text has not changed since
(`git log -L '/### D10/,/### D11/:docs/architecture.md' -s --format=%h origin/main`
prints only that commit). No earlier commit states the rule. It records practice from the
change history that preceded it, for example `9f6e337` (#136), where the write
billing logic "was copied into server/ and aws-v2/client/, so a billing fix
could land in one client only", and the benchmark suite of #160, which made
"measure first" possible.

Review of later pull requests applied the rule and shaped how it is read:

- #245 (`GetItem` projection before key): the maintainer rejected a flag
  parameter on `GetItem` that switched the order of checks. A second design
  review, with probes against Local, returned `ReadItem{Stored, Projected}` and
  two entry points, each with a fixed order and no mode flag. `Table.BatchGetItem`
  checks every key first and the projection once per table.
- #244 and #252: the maintainer asked why `ValidateNoneOrAllOldReturnValues`
  called `validateNoneOrAllOld`, which took the error message as a callback.
  #252 replaced the callback with plain predicates, `isNoneOrAllOld` and
  `isUpdateReturnValue`, and each validator builds its own error. #252: "Each
  validator read across two functions".

Facts on this branch:

- `ls core/*.go | grep -v _test | tr '\n' ' '` prints
  `core/doc.go core/index.go core/key_encoding.go core/key_schema.go
  core/list_tables.go core/select.go core/table.go core/transaction.go
  core/types.go`: nine files, each named after a concept, none after a fix.
- `git grep -n 'validateNoneOrAllOld' -- '*.go' | wc -l` prints `0`: the callback
  helper is gone.

## Decision

- Before writing a helper, look for an existing one (`ExtractPath`,
  `types.WithDetail`, `types.ValidateItemMap`, `keySchema.validatePrimaryKeyMap`,
  `index.ProjectItem`, the `capacity` functions).
- Do not duplicate a loop or a validator with only its message changed. Share
  the logic, and let each caller build its own message. Prefer plain predicates
  to callbacks, and separate entry points to flag parameters (maintainer
  review of #244, #245 and #252; `docs/architecture.md` D10 states both).
- Put code in the file that owns its concept, and never create a non-test file
  named after an issue or a fix. Key validation belongs in `core/key_schema.go`,
  table and index checks in `core/table.go` or `core/index.go`, and expression
  behavior in the interpreter file it extends. A new file is for a new concept
  that stands on its own, such as `core/select.go` or `core/key_encoding.go`.
- A performance argument needs a benchmark in `benchmarks/` with ns/op, B/op and
  allocs/op, taken before and after on an idle machine.

The alternatives were considered and rejected:

- **A flag parameter that changes the order of checks.** Rejected in #245 (see
  above): each order is its own entry point.
- **A validator that receives its error text as a callback.** Rejected in #252.
- No alternative was recorded at the time for the file-placement rule or for the
  benchmark requirement.

## Consequences

**Positive.** A check added to one path reaches the others (#237 shares the
`Put`, `Delete`, `ValidatePutItem` and `ValidateDeleteKey` helpers). The code has
a predictable home, and speed claims can be checked.

**Negative.** Finding the owner of a concept takes a search, and sharing a helper
means the first author carries the cost of generalizing it. A review has to ask
"does this exist already?" for every helper. A benchmark takes time to write.

**Neutral.** Test files can still be grouped by behavior.

## References

- `core/` file layout, `benchmarks/README.md`
- `docs/architecture.md` (D10)
- #136, #160, #223, #237, #244, #245, #252
