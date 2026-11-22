# Hold one lock per client for the whole of each public operation

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `1dd2eb1` (#251, issue #217). Each front end's
`Client` has a `sync.Mutex` (the aws-v2 client has had one since `22d6f92`).
Item operations took it, but other operations did not, or took it in pieces:

- `CreateTable`, `UpdateTable`, `DeleteTable` and `DescribeTable` took no lock,
  so an `UpdateTable` that built a GSI raced a `PutItem` that iterated the
  indexes ("a data race under `go test -race`", #217).
- Batches and `TransactGetItems` locked once per sub-request by calling the
  public item methods, so another call could run inside a batch.
  `BatchWriteItem` validated and applied under separate acquisitions, so its
  "validate everything before applying any" guarantee held only
  single-threaded.
- Several test knobs and `ClearTable` read or wrote client state without the
  lock.

#251 reports that before the fix the race test found 20 data races in `server`
and 8 in aws-v2.

Facts on this branch:

- `git grep -c 'mu.Lock()' -- server/client.go aws-v2/client/client.go` prints
  `23` for `server/client.go` and `25` for `aws-v2/client/client.go`.
- `git grep -n 'sync\.' -- core interpreter capacity types ':!*_test.go'`
  prints four lines, all in `interpreter/language_cache.go` (the expression
  caches). `core`, `capacity` and `types` use no `sync` primitive.

## Decision

Each front end's `Client` has one `sync.Mutex`. A public operation takes it once
at the top, holds it for the whole call and calls `core` directly. It does not
call another locked public method. Batches and transactions call unexported
helpers that assume the lock is held (`putItemLocked`, `deleteItemLocked`,
`getItemLocked`, `batchEmulationLocked`, ...), named with a `Locked` suffix or
documented as needing the lock. `core.Table` and the interpreter do no locking,
except for the expression caches.

A consequence the decision accepts: a batch or transaction is not interleaved
with any other call, and user callbacks run under the lock. That covers the
`EmulateUnprocessedItems` predicates, native-interpreter matchers and updaters
(`AddMatcher`, `AddUpdater`) and a custom interpreter from `SetInterpreter`. They
must not call the client, or they deadlock.

The alternatives were considered and rejected:

- **A read lock for `DescribeTable`.** Issue #217 floated "the client lock (or a
  read lock for `DescribeTable`)". #251 uses the single mutex. The reason that
  `docs/architecture.md` gives for the single lock is that it is simple to
  reason about and that minidyn's goal is correctness, not throughput.
- **Lock per sub-request inside batches.** This was the state before #251 and
  caused the interleaving above.

## Consequences

**Positive.** Table operations, batches, transactions and knobs no longer race or
interleave with other calls. The "validate everything first" guarantee of
`BatchWriteItem` also holds against concurrent calls. #251 reports no
measurable difference (±1-5%) in GetItem, PutItem, BatchGetItem and
TransactWriteItems benchmarks.

**Negative.** Concurrent operations on one client are serialized. A callback
that calls the client deadlocks (the release note of #251): an
`EmulateUnprocessedItems` predicate used to run with the lock released between
requests. 

**Neutral.** Every new public method must take the lock first and call helpers,
not other public methods.

## References

- `server/client.go`, `aws-v2/client/client.go`
- `docs/architecture.md` (D7, D8)
- #217, #250, #251
