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
check skips them while they are, and **fails once they scale well**, so the
issue gets closed and the expectation tightened (remove the `knownIssue` in
`scaling_test.go` and the section here). `TestScalingExpectationsDocumented`
runs in the normal test suite and keeps this file and the expectations in sync.

## Known performance issues

Measured on an Apple M-series laptop (`-benchtime 300ms`); times are per
operation.

### P1 — `Query` scans the whole table

| Benchmark | 100 items | 1,000 | 10,000 |
|-----------|-----------|-------|--------|
| `QueryPartition` (10 items returned) | 0.23 ms | 2.6 ms | 26 ms |
| `QueryRangeLimit` (`sk > :s`, `Limit: 3`) | 0.17 ms | 1.8 ms | 18 ms |
| `QueryWithFilter` | 0.32 ms | 3.3 ms | 33 ms |
| `QueryGSI` (10 items returned) | 0.26 ms | 2.9 ms | 28 ms |

A query of one 10-item partition is as expensive as a full `Scan`
(~58 allocations per item in the table). `core/table.go` `SearchData` walks
every key in `sortedKeys` (table or index) and evaluates the
`KeyConditionExpression` on each one. Keys are sorted by partition, so the
search could binary-search to the first key of the partition (and of the sort
key range) and stop at the first key past it, making a query
O(log n + items read). With `Limit` the walk stops early, but still starts at
the first key of the table, so the cost depends on where the partition is.

Per item, about 70% of `interpreter.(*Language).Match` is spent creating the
lexer and parser, parsing and re-validating attribute names (`go tool pprof` on
`ScanFilterFullTable`): the same expression is parsed **once per item**.
Parsing each expression once per request (or caching the parsed AST by
expression string) would cut the per-item cost of every Query, Scan and filter
by about two thirds, including full scans.

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

Adding a key appends it to `sortedKeys` and calls `sort.Strings` on the whole
slice (table and every index, e.g. `core/index.go` `putData` and `updateData`, which
also re-sorts when an item's index key changes), O(n log n) per insert, so loading n items is O(n² log n). Inserting at the
`sort.SearchStrings` position is O(n) (a copy) and much cheaper in practice; a
B-tree or skip list would make it O(log n). Note that this sorting is also the
source of parity issues #147-#149 (keys compared as joined strings).
