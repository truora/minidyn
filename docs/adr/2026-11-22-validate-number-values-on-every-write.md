# Reject an N value DynamoDB cannot store, wherever a request carries it

**Status:** Proposed

## Context

Before this change minidyn parsed the text of an N value in two places only: the key
encoding (`core/key_encoding.go`), for the attributes that make up a key, and the
interpreter, when an item is read into an expression. (`git grep -n 'ParseDecimal('
origin/main -- '*.go' ':!*_test.go' ':!types/decimal.go'` lists exactly those two; the
third use in `types/decimal.go` is `NormalizeNumber`, which the key encoding calls.) `types.ValidateItemMap` checked sets
for duplicates and nothing else. So `{"x": {"N": "abc"}}` and `{"x": {"N": "1e200"}}` were
stored on a non-key attribute, and a key attribute of 39 digits was accepted, where
DynamoDB rejects all of them (#226, #273). A stored invalid N then behaved differently
from DynamoDB in expressions and when an index was created on its attribute.

The behavior to match was probed on DynamoDB Local, with the same request on minidyn
beside it (`e2e/parity_number_validation_test.go`):

- Every N is checked, whichever attribute holds it: top level, inside L and M, every
  member of an NS, a key attribute, and a value of `ExpressionAttributeValues`.
- The messages are `A value provided cannot be converted into a number` (not a number, or
  an exponent whose text does not fit in 32 bits), the overflow and underflow texts, and
  `DynamoDB only supports precision up to 38 digits`. Trailing zeros are not significant
  digits, so `1.000...0` with 40 zeros is accepted. Precision is reported before range.
- An expression value is worded `ExpressionAttributeValues contains invalid value: <text>
  for key :v`. An item or key value is the bare text.
- Local walks lists, maps and sets in order and reports the first problem, so an invalid
  number before a duplicate set member wins, and the other way round.
- The numbers of an item or a key come before an empty key, a missing key, a key of the
  wrong type, undeclared names, undefined values and syntax errors. For UpdateItem and
  DeleteItem the shape and type of the key come before the numbers of the expression
  values. A transaction checks every operation, the values of an expression before the
  item or key, before anything else, and a batch write checks every request.

## Decision

`types.ParseNumber` (`ParseDecimal`, the 38-digit limit, `CheckRange`) and
`types.ValidateNumber` are the one place that decides whether an N is storable. The key
encoding calls `ParseNumber`, so a key gets the precision check too, and the interpreter's
precision text is `types.ErrNumberPrecision`.

`types.ValidateItemMap` and `types.ValidateItemAttributeValue` check every N and NS member
as they check sets, in the order Local does, and name the path of the offending value as
minidyn detail (D4). `types.ValidateExpressionAttributeValues` does the same for the
values of an expression with Local's wording. `core` calls them on every write, search and
key operation, in Local's order (D3); `core.ValidateWriteValues` does it once for a
`TransactWriteItems` request and `core.ValidateBatchWrite` first checks the numbers of
every request. Both front ends map their request into `core.ValidateWriteValues`; neither
has a number check of its own (D2).

The alternatives were considered and rejected:

- **Validate in the front ends, where the SDK or JSON value is decoded.** The two front
  ends have drifted before, and the check needs the order of D3.
- **Validate only when an item is read or an expression is evaluated.** That is where the
  problem came from: the same request would pass on an empty table and fail on a full
  one (D3).
- **A second number parser in the validator.** `ParseDecimal` already defines which texts
  are numbers; two parsers would disagree at the edges.
- **Normalize the stored text at the same time** (the second half of #226). It changes
  what a `GetItem` returns, and needs its own decision about lists and map copies, where
  Local keeps the raw text. It stays with #283.

## Consequences

**Positive.** An item minidyn stores is one DynamoDB would store, so an expression, an
index backfill or a key condition never meets an N that cannot be read. The messages and
the order of the checks match DynamoDB Local, pinned by parity tests.

**Negative.** This breaks callers: a test that stored a value like `{"N": "abc"}`, a
number past 38 digits, or one outside 1E-130 to 9.99...E+125 now fails with a
`ValidationException`. So does an N under a key attribute of another type: the number
error comes before the type mismatch, as in DynamoDB. A fixture that relied on any of
them must be fixed. Every N of every write is now parsed once more; a plain number of at
most 38 characters skips the parser.

**Neutral.** PutItem no longer pre-checks `ReturnValuesOnConditionCheckFailure` in the
front ends, so `core.Put` reports the numbers of the item first, as Local does; a missing
table and an unused value still come before it, also as in Local (parity cases
`PutItemNumberBeforeReturnValuesOnConditionCheckFailure`,
`PutMissingTableBeforeReturnValuesOnConditionCheckFailure` and
`PutUnusedValueBeforeReturnValuesOnConditionCheckFailure`). UpdateItem and DeleteItem keep
that pre-check ahead of the unused names and values and of the numbers (#243), so for them
the order of those combinations is unchanged and the parity tests do not pin it. A duplicate NS member is still compared by its text, not its value (`1` and
`1.0` are different members here and the same in Local): part of the normalization that
stays open.

## References

- `types/decimal.go` — `ParseNumber`, `ValidateNumber`, the number errors
- `types/validate_item.go` — the walk over items, sets and expression values
- `core/table.go` — the order of the checks in Put, Update, Delete, `SearchData`,
  `ValidateBatchWrite` and `ValidateWriteValues`
- `e2e/parity_number_validation_test.go` — the parity cases for each class and operation
- `docs/architecture.md` — D3 (validation order), D4 (error text)
- #226, #273, #219, #259
