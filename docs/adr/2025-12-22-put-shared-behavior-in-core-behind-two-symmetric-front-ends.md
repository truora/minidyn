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
- `8714c88` (#194, issue #180): "Use the item core returns in both front ends
  instead of re-checking ALL_OLD." Before it, `server/client.go` line 398 had
  `if input.ReturnValues == ddbtypes.ReturnValueAllOld`, and the aws-v2 client
  had the string form of the same test at line 439 (`git show
  8714c88^:server/client.go`, `8714c88^:aws-v2/client/client.go`). The commit's
  reason is validation, not duplication: "DeleteItem accepted any ReturnValues
  and deleted the item."
- `eb31ccb` (#245, issue #228): both front ends dropped
  `getItemAttributesForOutput`. "Both front ends also called
  LangInterpreter.Project themselves and mapped its error, which D2 and D4 of
  docs/architecture.md call a gap." DynamoDB checks the projection before the
  key, and minidyn checked the key first everywhere.

`0881ed0` (#237, issue #216) applies the rule to new code. Issue #216 says both
front ends applied a `BatchWriteItem` request by request through `PutItem` and
`DeleteItem`. The fix is `core.ValidateBatchWrite`, and the PR body says the
front ends "only map shapes into `core.WriteRequest` and call
`core.ValidateBatchWrite` (D2)".

Facts on this branch:

- `git grep -l 'minidyn/core"' -- server aws-v2/client ':!*_test.go'` lists five
  files: both `client.go`, both `capacity.go` and `aws-v2/client/mapper.go`.
- `git grep -c 'minidyn/capacity"' -- core | wc -l` prints `0`: `core` does not
  import the capacity package.
- `git grep -n 'ValidateSearchIndex' -- server aws-v2 ':!*_test.go'` prints four
  lines, two per front end. A pre-check still lives in both front ends (see
  Consequences).
- `grep -rn 'func validateExpressionAttributes' --include='*.go' .` finds two
  functions with different signatures: `server/expression_attributes.go:31` takes
  `exprValueKeys []string`, and `aws-v2/client/client.go:1510` takes
  `exprValues map[string]types.AttributeValue`.

## Decision

Anything both front ends need goes in `core` (or `capacity`). A front-end-only
change is allowed only for SDK or JSON mapping, including the
`validateExpressionAttributes` pre-check. A change in one front end needs the
same change, and the same test, in the other. A new request-level check belongs
in `core` unless it is about the shape of the SDK or JSON request.

The alternatives were considered and rejected:

- **Recorded: one front end per SDK, each with its own logic.** This was the
  state before `server`. Issue #80 asks to remove it ("remove the deplicated
  [sic] code for each aws-sdk version").
- **Replaced: each front end keeps its own copy of a rule.** Removed in steps
  (`8714c88`, `eb31ccb`, `242ce19`, `0881ed0`; details in Context). Only
  `242ce19` gives drift as the reason; the others removed a copy while fixing
  a behavior.
- No other alternative was recorded at the time (searched: #223, #237, #245,
  #249, #194, #136, issues #80, #180, #216, #228).

## Consequences

**Positive.** A behavior fix lands once and reaches both front ends. The tests
for the behavior live in `core`. `c292abf` (#173) fixed `PutItem`
`ReturnValues` without a change to either front end: "Both front ends already
pass that value straight through, so they get the fix without code changes."

**Negative.** Each front end still needs its own mapping and tests, so a change
to a front end is two changes. The pre-check `validateExpressionAttributes`
exists twice with different signatures (see the facts above), and the front
ends have to keep the two in step. The rule is not fully met: Gap (#243). The front
ends run the unused-names pre-check, the `ReturnValuesOnConditionCheckFailure`
check and the `ReturnConsumedCapacity` parse before `core`. That forces an order
that DynamoDB Local does not use and duplicates steps in both front ends. Gap (#275): the transaction operations
check emulated failures at different points in each front end (D13), so a
change to one front end without the other is still possible, and only review
and the D2 check catch it.

**Neutral.** `capacity` is a second shared package, because capacity math needs
no SDK (see the ADR on `ReturnValues` and consumed capacity). D12 and D13 place
consumed capacity and the test knobs in the front ends; this ADR defers to
them. Where the `ReturnConsumedCapacity` parse runs relative to `core` is the
order question tracked in #243.

## References

- `docs/architecture.md` (D2, section 1)
- `server/client.go`, `aws-v2/client/client.go`
- #80, #97, #136, #194, #216, #223, #228, #237, #243, #245, #249, #275
- `8714c88` (#194), `eb31ccb` (#245), `0881ed0` (#237), `c292abf` (#173)
