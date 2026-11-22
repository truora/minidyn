# Keep test knobs in the front-end clients, with the same meaning in both

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `c784e54` (#134, issue #133). minidyn offers knobs
that exist only to make tests more realistic or more hostile: failure emulation
(`EmulateFailure`, which predates these changes, and the table-scoped
`EmulateFailureForTable` from #132), unprocessed-items emulation,
the GSI activation delay (#126) and the native interpreter.

#134 fixed the contract of the failure knob. v0.8.0 had dropped the global
forced-failure guard in `BatchWriteItem`, so "an emulated `InternalServerError`
was routed into `UnprocessedItems` and the batch returned success instead of
erroring. This silently broke the `EmulateFailure` contract consumers ... rely
on" (#134). #132 had made batch sub-requests on a failing table land in
unprocessed results. #134 reverted that: a global or table-scoped failure fails
the whole batch, "matching DynamoDB returning a 500". Partial failure is its own
knob, `EmulateUnprocessedItems`, which selects individual sub-requests with a
predicate. The PR made both front ends behave the same. A review comment on #134
asked that an invalid batch be validated before the emulated failure,
"DynamoDB validates (400) before any 500", and `06fe32a` (also #134, 2026-06-19)
moved `validateBatchWriteItemInput` ahead of the emulation check in aws-v2. The
server already validated first.

Facts on this branch:

- `git grep -n 'EmulateFailure\|EmulateUnprocessed' -- core ':!*_test.go' | wc -l`
  prints `0`: failure emulation is not in `core`.
- `git grep -n 'UseNativeInterpreter\|IndexActivationDelay' -- core ':!*_test.go' | wc -l`
  prints `7`: the native-interpreter flag and the activation delay are fields of
  `core.Table`, set by the front ends (see Consequences).

## Decision

The knobs live in each front-end `Client`, with the same meaning in both:
failure emulation (global, table- and index-scoped, and unprocessed items), the
index activation delay, and the native interpreter. In single-item operations an
emulated failure is checked after `ReturnConsumedCapacity` is parsed and before
expression validation and the table lookup. A table-scoped failure fails a whole
batch, and unprocessed-items emulation is the only partial path. In batches, the
input shape is validated first, then the emulated failure, then each request.
GSIs are written synchronously and are strongly consistent. The only emulated
index delay is `Table.IndexActivationDelay` (#126).

The alternatives were considered and rejected:

- **Route the sub-requests of a failing table into `UnprocessedItems`.** #132
  did so, and #134 reversed it because it broke the `EmulateFailure` contract
  that consumers rely on.
- **Check the emulated failure before validating the batch input.** aws-v2 did
  so in `c784e54`. The #134 review asked for validation first ("DynamoDB
  validates (400) before any 500"), and `06fe32a` (also #134) moved
  `validateBatchWriteItemInput` ahead of the emulation check, with a test. The
  per-request checks that #237 added run after the emulated failure.
- No other alternative was recorded at the time.

## Consequences

**Positive.** Tests can make one table fail while another works, and can
simulate partial batch failure and a slow index. `core` carries no
failure-injection logic.

**Negative.** `core` carries no failure-injection logic, but two knobs are
stored on `core.Table` because the table needs them: `UseNativeInterpreter`
(matching and updating) and `IndexActivationDelay` (`Description`). The front
ends set both, and `core` never reads client state. Gap (#275): the transaction
operations check failures at different points in each front end, and a new knob
should follow the single-item order. Callbacks run under the
client lock (see the ADR on the lock), so a predicate must not call the client.

**Neutral.** A new knob has to be added to both front ends, at the same point in
the request.

## References

- `server/client.go`, `aws-v2/client/client.go`, `core/table.go`
- `docs/architecture.md` (D13)
- #126, #132, #133, #134, #237, #251, #275
