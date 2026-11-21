# Compute ReturnValues in core, and consumed capacity in the front ends

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `c8bc51b` (#136, issue #49). minidyn decoded
`ReturnConsumedCapacity` on every request but "never populated ConsumedCapacity
on any response, so tests that assert on consumed capacity could not run against
the fake" (#136). #136 added the `capacity` package, which sizes items and
derives read and write units, and filled `ConsumedCapacity` in both front ends.

`ReturnValues` already lived in `core`: `97e68e1` (#111, 2026-03-30) built the
update response from a snapshot of the pre-update item in `core`, because
minidyn "always returned the full post-update item (ALL_NEW)". The two halves
together make the split.

Capacity could not live in `core` as such. Billing needs the item sizes before
and after the write, and each front end reads and writes with its own request
shape. `9f6e337` (#136) shows the cost of leaving it per front end: "The
SDK-agnostic write billing ... was copied into server/ and aws-v2/client/, so a
billing fix could land in one client only". It moved the shared part into
`capacity`.

Facts on this branch:

- `git grep -c 'minidyn/capacity"' -- core | wc -l` prints `0`: `core` does not
  import `capacity`.
- `git grep -n 'capacity.ParseMode' -- server aws-v2 ':!*_test.go' | wc -l`
  prints `20`: the mode is parsed in the front ends, once per operation.

## Decision

- `core` validates the `ReturnValues` enums and computes the attributes to
  return: `ALL_OLD` for `Put` and `Delete`, `*_OLD` and `*_NEW` for `Update`, and
  the item of a failed condition.
- Consumed capacity is a front-end concern. Every operation parses the mode
  (`capacity.ParseMode`) before it calls `core`, measures item sizes before and
  after the write through `capacity` (`server/capacity.go`,
  `aws-v2/client/capacity.go`), and never changes what `core` returns.

The alternatives were considered and rejected:

- **Keep billing in each front end.** This was the state of #136 until
  `9f6e337` removed it because a fix could land in one client only.
- No other alternative was recorded at the time. In particular, nothing records
  why capacity was kept out of `core` rather than inside it.

## Consequences

**Positive.** The response contents are decided once, and capacity math is shared
and SDK-free. `core` stays unaware of billing.

**Negative.** The front ends read the stored item themselves to measure sizes,
which is why `core` returns `ReadItem{Stored, Projected}` for reads. A review
comment on #136 noted that write accounting billed only the base table and
omitted the index part; `capacity.IndexWrites` bills affected indexes now, as
`docs/operations.md` describes. The `ReturnConsumedCapacity` parse also runs in
the front ends before `core`, which forces an order that DynamoDB Local does not
use: Gap (#243).

**Neutral.** A `ReturnValues` rule in a front end, or capacity logic in `core`,
is a review finding.

## References

- `core/table.go`, `capacity/`, `server/capacity.go`, `aws-v2/client/capacity.go`
- `docs/architecture.md` (D12), `docs/operations.md`
- #49, #111, #136, #243
