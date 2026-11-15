# Parity tests

Tests in this package run the same AWS SDK v2 calls against minidyn (in-process
HTTP server) and DynamoDB Local (one Docker container per test run) and compare
the results. They need Docker.

```bash
go run ./tools/run-e2e                            # every parity test
go run ./tools/run-e2e TestE2E_Item/PutWithGSI    # one test
```

`run-e2e` fails when Docker isn't running, rather than skipping every comparison
and reporting `OK`. Set `MINIDYN_E2E_REQUIRE_DOCKER=0` to skip instead, for example
to check that the suite builds on a machine without Docker. A plain `go test ./...`
still skips the parity tests without Docker. `run-e2e` also gives `go test` a 45m
timeout, since the full suite takes longer than the default 10m. Pass `-timeout` to
override it.

## Known divergences

A divergence that isn't fixed yet is tracked as a
[GitHub issue labeled `bug`](https://github.com/truora/minidyn/issues?q=is%3Aissue+label%3Abug+Parity),
and the tests that hit it set `knownIssue: "#<number>"` and run through
`RunE2EKnown` (`known_issue_test.go`) instead of `RunE2E`:

- **still failing**: the test is reported as skipped with a link to the issue,
  so CI stays green while the bug stays visible;
- **now passing**: the test **fails** and tells you to remove `knownIssue` and
  close the issue, so a fixed bug can't keep a stale skip.

To work on one, run its tests as normal tests:

```bash
MINIDYN_E2E_RUN_KNOWN=1 go run ./tools/run-e2e <TestName> -count=1
```

Fix minidyn (test first), then remove the `knownIssue` field from every test
that references the issue, and any fuzzer restriction or `parity-surface`
allowlist entry that names it.

### Intentional divergences

DynamoDB Local is the parity reference, but where it disagrees with the
documented behavior of DynamoDB, minidyn follows DynamoDB. These tests keep
their `knownIssue` marker even though the issue is closed, so the difference
stays visible and is checked on every run:

- **#153, `LastEvaluatedKey` at the end of the sort key range**
  (`TestE2E_FuzzFindings/QueryLimitEndsAtRangeEnd`). When a `Query` page fills
  at `Limit`, DynamoDB Local leaves out `LastEvaluatedKey` if the last item
  read sits exactly on the inclusive end of the sort key range. minidyn always
  returns it, as the [Query API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)
  describes: hitting `Limit` stops the operation "and returns ... a key in
  `LastEvaluatedKey`", an empty `LastEvaluatedKey` means "there is no more
  data to be retrieved", and a present one "does not necessarily mean that
  there is more data". Copying Local would also break that contract: on a
  global secondary index whose range key is the table's sort key, Local
  omitted the key while items sharing that sort key were still unread, so a
  caller paging until `LastEvaluatedKey` is empty would miss them. The cost is one extra, empty page when a range ends
  exactly at a page boundary. The default fuzz mode avoids the case (see
  `fuzz_gen_test.go`).
- **`ReturnConsumedCapacity` outside `INDEXES`, `TOTAL` and `NONE`**
  (`TestE2E_ConsumedCapacityInvalidMode`). DynamoDB Local accepts any value
  and answers as if it were `NONE`. minidyn rejects it with a
  `ValidationException` ("Member must satisfy enum value set: [INDEXES, TOTAL,
  NONE]") before the operation runs, as the
  [Query reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)
  requires for `ReturnConsumedCapacity` ("Valid Values: `INDEXES | TOTAL |
  NONE`"; the value is case sensitive, so `total` is
  invalid). The test asserts the error on minidyn and the silent success on
  Local.
- **`ReturnValuesOnConditionCheckFailure` outside `ALL_OLD` and `NONE` inside
  `TransactWriteItems`**
  (`TestE2E_TransactReturnValuesOnConditionCheckFailureInvalidValue`).
  DynamoDB Local does not validate the value on the transactional `Put`,
  `Update`, `Delete` and `ConditionCheck`: a failed condition cancels the
  transaction with `ConditionalCheckFailed`. minidyn rejects the whole
  request, before any write, with a `ValidationException` ("1 validation error
  detected: Value 'BOGUS' at
  'transactItems.1.member.put.returnValuesOnConditionCheckFailure' failed to
  satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"),
  as the [TransactWriteItems reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)
  and its nested shapes
  ([Put](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Put.html),
  [Update](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Update.html),
  [Delete](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Delete.html),
  [ConditionCheck](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ConditionCheck.html))
  list "Valid Values: `ALL_OLD | NONE`". The standalone `PutItem`,
  `UpdateItem` and `DeleteItem` already reject it on both engines. The test
  asserts the error on minidyn and the cancellation on Local. The exact
  message text of the nested path is taken from the documented shape, and
  verification against real DynamoDB is tracked in #224.
- **Transactional `ConditionCheck` billing**
  (`TestE2E_ConsumedCapacityConditionCheck`). DynamoDB Local bills 2 x the
  read units of the checked item (20 KB item: 10). The
  [transactions guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-capacity-handling)
  only says that "DynamoDB performs two underlying reads or writes of every
  item in the transaction" and does not say which a `ConditionCheck` is, so
  minidyn keeps its approximation: 2 x the write units of the key (2). The
  test asserts both numbers.

The harness treats these markers like any other: a skip links to the (closed)
issue. If one of these tests ever reports that the issue "appears FIXED", minidyn
has stopped following DynamoDB's documented behavior: fix minidyn, don't remove
the marker.

## Finding new gaps

### Full-response tracing

Most parity tests only compare the values they return. To compare **every**
response field and the final table and index contents as well:

```bash
MINIDYN_E2E_TRACE=report MINIDYN_E2E_TRACE_OUT=/tmp/trace.jsonl go run ./tools/run-e2e -count=1
```

`report` logs differences (and writes one JSON line per diverging test to the
`_OUT` file) and keeps tests green; `strict` fails them. Benign differences
(ARNs, timestamps, set and Scan order, explicit `null`s, number formatting,
index `CREATING` vs `ACTIVE`) are normalized in `trace_test.go`.

### Ignored API surface

```bash
go run ./tools/parity-surface          # operations, request fields and response fields minidyn ignores
go run ./tools/parity-surface -check   # compare with tools/parity-surface/allowlist.txt
```

`-check` (also run by `go test ./tools/...`) fails on a gap missing from the
allowlist, such as a new SDK field after a dependency bump, and on an
allowlisted gap that is now covered, so the list can't go stale.

### Differential fuzzing

`TestE2E_Fuzz` (`fuzz_test.go`, generator in `fuzz_gen_test.go`) runs random
sequences of `PutItem`, `UpdateItem`, `DeleteItem`, `GetItem`, `Query` and
`Scan` (with conditions, filters, projections, a GSI and `ReturnValues`) against
both engines in lockstep. It fails at the first response or final table state
that differs, printing the seed, the requests so far and a reproduce command.

```bash
go run ./tools/run-e2e TestE2E_Fuzz                       # default: 8 seeds x 40 ops
MINIDYN_E2E_FUZZ_SEEDS=200 MINIDYN_E2E_FUZZ_OPS=120 go run ./tools/run-e2e TestE2E_Fuzz
MINIDYN_E2E_FUZZ_SEED=42 go run ./tools/run-e2e TestE2E_Fuzz   # reproduce one seed
MINIDYN_E2E_FUZZ_KNOWN=1 go run ./tools/run-e2e TestE2E_Fuzz   # explore known areas too
```

By default the generator stays away from open issues (each restriction in
`fuzz_gen_test.go` names its issue), so the run is green and any failure is a
**new** gap: file an issue and add a minimal case to
`parity_fuzz_findings_test.go`. When an issue is fixed, remove its restriction
to widen the fuzzer. `MINIDYN_E2E_FUZZ_KNOWN=1` lifts the restrictions and also
compares error messages, so it fails quickly on known bugs.
