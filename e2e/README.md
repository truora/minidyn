# Parity tests

Tests in this package run the same AWS SDK v2 calls against minidyn (in-process
HTTP server) and DynamoDB Local (one Docker container per test run) and compare
the results. They need Docker.

```bash
go run ./tools/run-e2e                            # every parity test
go run ./tools/run-e2e TestE2E_Item/PutWithGSI    # one test
```

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
- **Transactional `ConditionCheck` billing**
  (`TestE2E_ConsumedCapacityConditionCheck`). DynamoDB Local bills 2 x the
  read units of the checked item (20 KB item: 10). The
  [transactions guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-capacity-handling)
  only says that "DynamoDB performs two underlying reads or writes of every
  item in the transaction" and does not say which a `ConditionCheck` is, so
  minidyn keeps its approximation: 2 x the write units of the key (2). The
  test asserts both numbers.

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
