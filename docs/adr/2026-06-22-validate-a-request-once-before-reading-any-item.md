# Validate a request once, before reading any item, in DynamoDB's order

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `ccc2db6` (#135, issue #67). DynamoDB rejects an
expression that names an undeclared `#name`, whatever the table holds.
minidyn found the problem only while evaluating an item, so the same bad
request passed on an empty table, or when no item reached the check, and
failed on a full one. #135 moved the `KeyCondition`, `Filter` and
`Projection` checks into `SearchData`, "so empty or unmatched tables still
error like DynamoDB (the per-item interpreter checks would otherwise never
run)".

The same idea was then applied one rule at a time, each time with a probe of
DynamoDB Local: function operand counts (`91bd4d1`, #196, issue #189: DynamoDB "rejects
both before reading any item, even on an empty table"), undefined `:values`
(#207), key conditions (#205), `Select` (#204), empty key values (#220), an
unknown `IndexName` (#249) and others. The order between checks is pinned
against Local.

Facts on this branch:

- `grep -c '^func Test' e2e/parity_validation_order_test.go` prints `5` and
  `grep -c '^func Test' core/validation_order_test.go` prints `7`: the order is
  pinned by a parity test and by a core unit test.
- `git grep -n 'ValidateSearchIndex' -- server aws-v2 ':!*_test.go'` prints four
  lines. The front ends call it ahead of their own pre-check of unused names and
  values until #243 moves that pre-check into `core` (Gap #243).

## Decision

Whatever DynamoDB can reject without reading an item is rejected before the
first item is read. These checks fail even on an empty table or when a
condition would never be evaluated. They run once per request, not once per
item. Per-item evaluation produces only errors that depend on the item, such as
an update operand of the wrong type read from the item. When a request has
several problems, the checks run in the order DynamoDB Local reports them.

The alternatives were considered and rejected:

- **Check inside the per-item interpreter only.** This was the state before
  #135. The error then depends on the data (#135).
- **Copy Local's order everywhere.** Where Local's order is not stable, minidyn
  chooses its own and says so. For a missing table next to an invalid
  `BatchWriteItem` request, Local's answer depended on the wire order and the
  key schema, so the parity test compares only that the batch is rejected
  (#237).

## Consequences

**Positive.** A bad request fails the same way on any table, and the error does
not depend on the data. Parity tests can pin the order.

**Negative.** Every new rule needs a probe of Local to find where it goes in the
order, plus a test that fails on an empty table. Some checks run in the front
ends, before `core`, so the order is not fully DynamoDB's: Gap (#243).

**Neutral.** The order is a list in `docs/architecture.md` (D3) that changes
whenever a check is added.

## References

- `core/table.go` (`validateSearch*`, `validateWriteExpressions`,
  `ValidateSearchIndex`), `core/select.go`
- `e2e/parity_validation_order_test.go`, `core/validation_order_test.go`
- `docs/architecture.md` (D3)
- #67, #135, #189, #196, #204, #205, #207, #220, #237, #243, #249
