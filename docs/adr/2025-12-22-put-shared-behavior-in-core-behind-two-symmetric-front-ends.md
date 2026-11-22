# Put shared behavior in core and keep the two front ends thin and symmetric

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `fb211ca` (#97). That commit added the `server`
front end beside `aws-v2/client`, both on top of `core`. Its motivation, from
the commit message and issue #80, was to let teams use minidyn through
`httptest` and "remove the duplicated code for each aws-sdk version".

The decision became a written rule in `docs/architecture.md` (#223). That
document says the two front ends "have drifted apart before". No commit, pull
request or issue records a specific incident, so the claim is the document's
own. What the history does show is repeated cleanup of copies:

- `9f6e337` (#136): the write billing logic "was copied into server/ and
  aws-v2/client/, so a billing fix could land in one client only".
- `242ce19` (#249): the transactional key checks of both front ends repeated by
  hand a sequence that `core` already ran, "so the two copies could drift from
  it and from each other".

Facts on this branch:

- `git grep -l 'minidyn/core"' -- server aws-v2/client ':!*_test.go'` lists five
  files: both `client.go`, both `capacity.go` and `aws-v2/client/mapper.go`.
- `git grep -c 'minidyn/capacity"' -- core | wc -l` prints `0`: `core` does not
  import the capacity package.
- `git grep -n 'ValidateSearchIndex' -- server aws-v2 ':!*_test.go'` prints four
  lines, two per front end. A pre-check still lives in both front ends (see
  Consequences).

## Decision

Anything both front ends need goes in `core` (or `capacity`). A front-end-only
change is allowed only for SDK or JSON mapping, including the
`validateExpressionAttributes` pre-check. A change in one front end needs the
same change, and the same test, in the other. A new request-level check belongs
in `core` unless it is about the shape of the SDK or JSON request.

The alternatives were considered and rejected:

- **One front end per SDK, each with its own logic.** This was the state before
  `server`. Issue #80 asks to remove it ("remove the deplicated [sic] code for each
  aws-sdk version").
- No other alternative was recorded at the time.

## Consequences

**Positive.** A behavior fix lands once and reaches both front ends. The tests
for the behavior live in `core`.

**Negative.** Each front end still needs its own mapping and tests, so a change
to a front end is two changes. The rule is not fully met: Gap (#243). The front
ends run the unused-names pre-check, the `ReturnValuesOnConditionCheckFailure`
check and the `ReturnConsumedCapacity` parse before `core`. That forces an order
that DynamoDB Local does not use and duplicates steps in both front ends.

**Neutral.** `capacity` is a second shared package, because capacity math needs
no SDK (see the ADR on `ReturnValues` and consumed capacity).

## References

- `docs/architecture.md` (D2, section 1)
- `server/client.go`, `aws-v2/client/client.go`
- #80, #97, #136, #223, #243, #249
