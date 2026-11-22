# Validate everything before changing anything, and restore state when a write fails midway

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `9b6f272` (#220, issue #163). Before it, a write
could leave the base table and its indexes out of step. #220 rejected empty S
and B values in key and index key attributes, and to do that for an `Update` it
had to handle an index rejecting an item after the item was changed: "the
rollback also covers interpreter errors, which previously could leave a
partially modified item" (#220). It made `Put` and `Update` compute every index
key (`indexKeysOf`) before storing anything, and made `Update` restore the item
in place.

The principle already existed for transactions. `7af557e` (#114, 2026-03-30)
implemented `TransactWriteItems` "with atomic all-or-nothing semantics": "Without
snapshot/restore, a mid-transaction failure would leave tables in an
inconsistent state, violating the all-or-nothing guarantee DynamoDB provides."
#220 extended the same rule to single-item writes. #237 (issue #216) then
applied it to `BatchWriteItem`: "a batch with an invalid request wrote the valid
requests that came before it. DynamoDB validates the whole batch first".

Facts on this branch:

- `git grep -n 'table.Snapshot()' -- '*.go' ':!*_test.go' | wc -l` prints `2`:
  one caller per front end, both in `prepareTransact`
  (`server/client.go`, `aws-v2/client/client.go`).
- `git grep -n 'core.ValidateBatchWrite(' -- server aws-v2 ':!*_test.go' | wc -l`
  prints `2`: the same one call per front end.

## Decision

A request applies completely or not at all. Everything is validated before
anything is changed. Where a failure can only be found mid-write, the previous
state is restored, so the table and every index match their state before the
request.

The rule covers item writes, table definitions, transactions and batches. Each
operation's mechanism is listed in `docs/architecture.md` (D8). A batch is
validated whole under one lock acquisition, then applied one request at a time,
which DynamoDB does not make atomic either.

The alternatives were considered and rejected:

- **Record only the items a transaction writes, instead of snapshotting whole
  tables.** Issue #158 (P2) proposed it: "Recording the previous version of only
  the items the transaction writes would make it O(items in the transaction)."
  The item is still open and no reason against it was recorded; table-level
  snapshots stay until someone does it.
- **Write items one by one and stop at the first error (for batches).** This was
  the behavior before #237 and caused issue #216.
- No other alternative was recorded at the time.

## Consequences

**Positive.** A failure never leaves the base table and its indexes out of step,
which would otherwise show up later as wrong query results.

**Negative.** A transaction snapshot costs time in proportion to the table size
(`benchmarks/README.md`), so the number of callers is kept at two. Isolation is
table-level, not item-level, so a concurrent environment can see a whole-table
rollback where DynamoDB would roll back specific items (`docs/operations.md`).
For a batch with both a missing table and an invalid request, minidyn's order
is its own choice (D1 level 3), because Local's answer is not stable (#237).

**Neutral.** Every error path that follows a mutation needs a restore and a test
that asserts the table and its indexes after the failure.

## References

- `core/table.go` (`indexKeysOf`, `Put`, `Update`, `ValidateBatchWrite`,
  `Snapshot`), `server/client.go`, `aws-v2/client/client.go` (`prepareTransact`)
- `docs/architecture.md` (D8), `docs/operations.md`, `benchmarks/README.md`
- #114, #158, #163, #208, #216, #220, #237
