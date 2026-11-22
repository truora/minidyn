# Follow the AWS documentation first, DynamoDB Local second, and minidyn's own choice last

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `a840e3a` (#136). That commit states the rule:
"Where DynamoDB Local and the documented behavior of DynamoDB differ, minidyn
follows DynamoDB and records the divergence." The e2e suite had compared minidyn
with DynamoDB Local since `ae9e58d` (#108, 2026-03-27), so Local was already the
reference. What was missing was the answer to "what if Local is wrong?".

Users test code that will run against real DynamoDB, and Local has quirks that
real DynamoDB does not have. #153 is the case that settled the rule: on a
`Query` page that fills at `Limit`, Local omits `LastEvaluatedKey` when the last
item read sits on the inclusive end of the sort key range. On a GSI whose range
key is the table's sort key, Local omitted it while items with the same sort key
were still unread, so a caller paging until the key is empty would miss data
(#153, #210).

Facts on this branch:

- `sed -n '/^### Intentional divergences/,/^## Finding new gaps/p' e2e/README.md | grep -c '^- \*\*'`
  prints `5`: five divergences from Local are recorded today.
- `git grep -n 'knownIssue: "#153"' -- 'e2e/*_test.go'` prints one line
  (`e2e/parity_fuzz_findings_test.go`). It is the only divergence that still
  carries a `knownIssue` marker.

## Decision

minidyn copies DynamoDB, in this order of authority:

1. The AWS documentation (API reference and developer guide).
2. DynamoDB Local, where the documentation is silent.
3. minidyn's own choice, only when neither source decides. The choice is
   commented at the site and covered by a unit test.

Maintainer review confirmed the order. A divergence from Local goes under
"Intentional divergences" in `e2e/README.md`, cites the documentation, and has a
parity test that asserts each engine's behavior. Examples: #153; trailing zeros
in numbers (#259, #264); and, proposed in #271 (open), skipping wrong-typed items
when a GSI is created.

The alternatives were considered and rejected:

- **Copy Local's behavior.** For #153 it breaks the documented contract of
  `LastEvaluatedKey` (an empty key means "there is no more data to be
  retrieved") and can silently lose items when paging a GSI. Keeping minidyn's
  behavior costs one extra, empty page when a range ends exactly at a page
  boundary (the #153 comment, #210).
- **Treat the divergence as a bug and track it as a known issue.** #153 was
  tracked that way first. The decision recorded on the issue is that minidyn
  keeps its behavior, so this is "an intentional divergence ... not a bug".

## Consequences

**Positive.** minidyn matches what users will see in production, and each
difference from Local is written down with its source.

**Negative.** Each divergence needs documentation, a parity test that asserts
both engines, and a citation. Some documentation is vague, so choosing between
"docs" and "Local" takes judgment. None of the five divergences is
verified against real DynamoDB yet (#224).

**Neutral.** #153 kept its `knownIssue` marker (#210). The later entries, for
example `TestE2E_NumberPrecision/TrailingZerosAreTrimmed`, assert each engine
without one.

## References

- `docs/architecture.md` (D1)
- `e2e/README.md` ("Intentional divergences")
- `e2e/parity_fuzz_findings_test.go` (`QueryLimitEndsAtRangeEnd`)
- #108, #136, #153, #210, #224, #259, #264, #271
