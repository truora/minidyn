# Benchmarks

Per-operation cost of minidyn as tables grow, through the in-process `aws-v2`
client. Each benchmark runs on tables of 100, 1,000 and 10,000 items with 10
items per partition and per GSI group, so point operations and one-partition
queries should cost about the same at every size.

```bash
go test ./benchmarks -run '^$' -bench . -benchmem                 # everything
go test ./benchmarks -run '^$' -bench 'Ops/Query' -benchmem       # a subset
MINIDYN_BENCH_SCALING=1 go test ./benchmarks -run TestScaling -v  # scaling check
```

## Comparing a change

Use [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) on 10 runs
before and after:

```bash
go test ./benchmarks -run '^$' -bench . -benchmem -count 10 > /tmp/old.txt
# ...apply the change...
go test ./benchmarks -run '^$' -bench . -benchmem -count 10 > /tmp/new.txt
go run golang.org/x/perf/cmd/benchstat@latest /tmp/old.txt /tmp/new.txt
```

Profile one case with
`go test ./benchmarks -run '^$' -bench 'Ops/QueryPartition/items=10000' -cpuprofile cpu.out`
and `go tool pprof -top -cum benchmarks.test cpu.out`.

## Scaling check

`TestScaling` measures every operation on 1,000 and 10,000 items and fails if
an operation that should not depend on the table size gets more than 3x slower
(full-table operations are compared per item). It is opt-in because it measures
wall time. Operations with a known issue below are expected to stay slow: the
check skips them while they are, and **fails once they grow less than 1.5x**, so the
issue gets closed and the expectation tightened (remove the `knownIssue` in
`scaling_test.go` and the section here). `TestScalingExpectationsDocumented`
runs in the normal test suite and keeps this file and the expectations in sync.

## Known performance issues

Measured on an Apple M-series laptop (`-benchtime 300ms`); times are per
operation. `TestAllocationsDoNotGrow` (normal test suite) also fails when a
point operation or one-partition query starts allocating more on a larger
table; allocation counts are deterministic, so it is safe to run in CI.

### P2 — Transactions copy every table they touch

| Benchmark | 100 items | 1,000 | 10,000 |
|-----------|-----------|-------|--------|
| `TransactWritePutUpdate` (2 items) | 0.11 ms | 1.2 ms | 15 ms |

`aws-v2/client` `prepareTransact` takes a `core.(*Table).Snapshot` of each
table in the transaction for rollback, which deep-copies every item and index
(`core/table.go:623`, `core/index.go:51`). Recording the previous version of
only the items the transaction writes would make it O(items in the
transaction).

### P3 — Inserting a new key re-sorts all keys

| Benchmark | 100 items | 1,000 | 10,000 |
|-----------|-----------|-------|--------|
| `PutItemNewThenDelete` | 4.7 µs | 11 µs | 63 µs |
| `UpdateItemIndexKey` (moves an item to another GSI key) | 6.7 µs | 8.7 µs | 57 µs |
| `QueryGSIAfterWrite` (one write, then a GSI query) | 37 µs | 0.19 ms | 1.9 ms |

GSI queries binary-search a sorted copy of the index entries (see P1 below), but
any write discards that copy and the next GSI query re-sorts every entry. A
read-heavy test gets the fast path; one that alternates writes and GSI queries
pays a full sort per query. Keeping the copy sorted on each write (the same
change as the insert fix) would remove both costs.

Adding a key appends it to `sortedKeys` and calls `sort.Strings` on the whole
slice (table and every index, e.g. `core/index.go` `putData` and `updateData`, which
also re-sorts when an item's index key changes), O(n log n) per insert, so loading n items is O(n² log n). Inserting at the
`sort.SearchStrings` position is O(n) (a copy) and much cheaper in practice; a
B-tree or skip list would make it O(log n). Note that this sorting is also the
source of parity issues #147-#149 (keys compared as joined strings).

## Fixed

### P1 — `Query` scanned the whole table (#158)

`Query` walked every key of the table or index and evaluated the
`KeyConditionExpression` on each one, re-parsing the expression for every
item. Now the partition key equality is taken from the (cached) parsed key
condition and the search binary-searches to that partition; GSI queries
binary-search a sorted copy of the index entries that is rebuilt only after a
write. Condition expressions are parsed once per distinct text, which also
makes `Scan` and filters ~45% cheaper per item.

| Benchmark at 10,000 items | Before | After |
|---------------------------|--------|-------|
| `QueryPartition` | 26 ms, 580k allocs | 23 µs, 396 allocs |
| `QueryRangeLimit` | 18 ms | 16 µs |
| `QueryWithFilter` | 33 ms | 46 µs |
| `QueryGSI` | 28 ms | 26 µs |
| `ScanFilterFullTable` | 35 ms | 21 ms |
