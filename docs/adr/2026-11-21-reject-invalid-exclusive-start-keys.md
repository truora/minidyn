# Reject an ExclusiveStartKey DynamoDB rejects, before reading any item

**Status:** Proposed

## Context

A Query or Scan resumes after an `ExclusiveStartKey`. Until now minidyn accepted
almost any value there. `Table.checkSeek` documented the choice: a start key
outside the queried partition, or without its key attributes, "is not rejected,
unlike DynamoDB: consumers' tests rely on that leniency, so validating it is a
separate, release-noted change" (`git grep -n "consumers' tests rely" origin/main -- core`).

What minidyn did with such a key depended on which check it failed:

- a key of another partition, or outside the range condition, gave an empty page;
- a key missing its attributes, or with one of the wrong type, restarted the
  walk from the first item, so a client paging until `LastEvaluatedKey` is
  empty could loop forever;
- on an index, a key without the index key resumed after the item the table key
  named;
- extra attributes and empty S or B values were taken as an ordinary key.

DynamoDB Local rejects every one of these with a `ValidationException`, before
reading an item (#171, with its update from #227). The cases and their texts were
probed against DynamoDB Local, including the order in which they are reported
when several apply; `e2e/parity_start_key_validation_test.go` pins them.

D1 makes DynamoDB's behavior the reference and D3 asks that whatever DynamoDB can
reject without reading an item is rejected up front, in its order. The leniency
was a deliberate, documented exception to both (it is not an entry of
`docs/architecture.md`, which never recorded it), kept because consumers depend
on it. #171 reports the cost of removing it: running 207 minidyn-backed test
packages of a consumer (scrap-services) against a version that rejected the
case, 6 packages broke, 4 passing placeholder start keys and 2 reusing a
`LastEvaluatedKey` for another partition.

## Decision

`Table.SearchData` validates the `ExclusiveStartKey` of a Query and of a Scan,
on the table and on an index, after the key condition and before the filter's
projected attributes, `Select` and the index projection (`Table.validateStartKey`).
The checks run in the order DynamoDB Local reports them:

1. a number DynamoDB cannot store, in any attribute;
2. the size: for the table and a local index, the number of key attributes
   (`Exclusive Start Key must have same size as table's key schema`); for a
   global index, `The provided starting key is invalid`;
3. each key attribute in turn, the table's first and then the index's: missing
   is `The provided starting key is invalid`, a type other than the declared one
   is `Type mismatch for attribute to update`;
4. an empty S or B value in the same order, as `The provided starting key is
   invalid: ` followed by the empty-value text of the key (with `The query can
   return at most one row and cannot be restarted. ` in front for a hash key);
5. for a Query, the key condition, which the start key must satisfy
   (`The provided starting key does not match the range key predicate`). A start
   key equal to the inclusive bound the walk ends at is accepted (see the
   rejected alternatives).

The texts are Local's; minidyn's detail (`index`, `attribute`, `reason`, ...)
follows ` | ` (D4). The start key is matched against the key condition with the
interpreter, so numbers compare by value and the parse is the cached one (D6).
An empty `ExclusiveStartKey` map is still taken as none: `core/table.go` collapses
`len == 0`, and `aws-v2/client/mapper.go` maps an empty map to nil (`server/mapper.go`
keeps it), so Local's rejection of `{}` is left out (see Consequences, #277).

A start key need not name an existing item (#258): that is unchanged.

The alternatives were considered and rejected:

- **Keep the leniency, behind an option.** The issue's compatibility numbers are
  an argument for a heads-up, not for a permanent divergence: the lenient pages
  (empty, or restarted from the first item) are exactly what hides a client bug,
  and a flag parameter splits every test of the feature in two.
- **Reject only the partition case.** That is the case 2 of the 6 packages hit,
  but the placeholder start keys that restart the walk are the more dangerous
  half (4 of the 6), and the cases share one check.
- **Reject, on the table, a start key equal to the inclusive bound the walk ends
  at** (`sk <= :v` going forward, `sk >= :v` going backward, the matching bound
  of BETWEEN), as Local does. minidyn returns a `LastEvaluatedKey` when a page
  ends exactly there (the intentional divergence of #153), so the next call of a
  client paging until it is empty would fail with the key minidyn handed out.
  Local is self-consistent because it never returns that key. Omitting the key
  was not chosen here (#153). minidyn accepts the start key and returns an empty
  page; `TableRangePredicateAtBound` records the difference with `knownIssue: "#153"`.
- **Compare the partition by encoded key and the range with new analysis code.**
  Evaluating the key condition on the start key reuses what already decides
  whether an item is in the query, and keeps number spellings equal.

## Consequences

**Positive.** minidyn rejects the same start keys as DynamoDB, with its text, so
a test that passes against minidyn no longer hides a client that would fail
against DynamoDB; a paging loop cannot restart silently. The fuzzer can now send
a start key together with a range condition.

**Negative.** This is a breaking change for consumers that relied on the
leniency: a `LastEvaluatedKey` reused for another partition, a placeholder start
key, a start key of the table used on a global index, or a start key of the wrong
type, now fail with a `ValidationException`. It needs a release note listing
them (see the pull request).

**Neutral.** An empty `ExclusiveStartKey` map is accepted, where Local answers
`The provided starting key is invalid` (#277). A start key at the inclusive
bound the walk ends at is accepted, where Local rejects it (#153). `Limit` below 1, a syntax error in the
filter and `ConsistentRead` on a global index are still checked at different
points than Local checks them, whether or not the request has a start key.

## References

- `core/table.go` — `Table.validateStartKey` and `Table.SearchData`
- `e2e/parity_start_key_validation_test.go` — the parity cases against DynamoDB Local
- `docs/architecture.md` — D1, D3, D4, D6
- #153, #171, #227, #258, #277
