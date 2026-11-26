# Reject a placeholder list index like DynamoDB

**Status:** Accepted

## Context

minidyn accepted a value placeholder, or a plain name, between the brackets of a list
path: `SET l[:i] = :v`, `l[:i] = :x` in a condition, `l[:i]` in a projection. The
evaluator resolved the placeholder to a number when it evaluated the path. The error
text `access index with [] only support N as index` has been in the repository since
the list index was added (`git log -S'only support N as index' --format='%h %ad'
--date=short | tail -1` gives `f24efd4 2021-08-24`).

DynamoDB does not have this syntax. DynamoDB Local takes only digits between the
brackets; anything else is a syntax error that names the token. The probe was run on
DynamoDB Local with minidyn beside it (`e2e/parity_list_index_syntax_test.go`, 74
named cases in `TestE2E_ListIndexSyntax`, from `awk '/^func TestE2E_ListIndexSyntax/{f=1}
/^func TestE2E_ListIndexLiteral/{f=0} f' e2e/parity_list_index_syntax_test.go | grep -c
'^\s*"[A-Za-z0-9]*": *{kind:'`; the literal rules are in `TestE2E_ListIndexLiteral`):

- **The text.** `Invalid UpdateExpression: Syntax error; token: ":i", near: "[:i]"`,
  and the same with `ConditionExpression`, `FilterExpression`,
  `KeyConditionExpression` and `ProjectionExpression`. A name placeholder gives
  `token: "#n", near: "[#n]"`, and a plain name `token: "abc", near: "[abc]"`. A keyword
  is a word like any other, whatever its case, and the token keeps the case it was written
  in: `l[OR]` gives `token: "OR", near: "[OR]"`, `l[and]` gives `token: "and", near:
  "[and]"`, and `set`, `in`, `between` and `not` are the same, in an update, a condition, a
  filter and a projection. A token that is not a word is the same error, with `near` from
  the bracket to the end of the token after it: `l[-1]` gives `token: "-", near: "[-1"`,
  `l[]` gives `token: "]", near: "[] ="` (or `"[]"` at the end of the text) and `l[(]`
  gives `token: "(", near: "[(]"`.
- **Where.** In every position: SET (left and right side, inside `list_append`,
  `if_not_exists` and arithmetic), REMOVE, ADD, DELETE, a condition on `UpdateItem`,
  `PutItem`, `DeleteItem` and `TransactWriteItems`, a filter on `Query` and `Scan`, a
  key condition on the hash or the sort key, a projection on `GetItem`, `BatchGetItem`
  and `Query`; nested (`l[:i].m`, `m.l[:i]`) and mixed with a literal (`l[0][:i]`,
  `l[:i][0]`).
- **When.** While parsing, before Local compares the placeholders with the declared
  ones. An undefined `:i`, no values at all, an unused value, an unused name and an
  undeclared `#name` all give the syntax error, not their own message. An update
  expression is reported before a condition.

The literals follow Local too, which a second probe of the same cases showed (the
text, position and order are in the Decision):

- **Range.** An index above 2147483647 is `Invalid <X>Expression: List index is not
  within the allowable range; index: [N]` in every expression kind, with no token or
  position, whatever its size (`2147483648`, `9223372036854775807`, `9223372036854775808`
  and 26 digits all give the same text). `2147483647` is accepted. minidyn accepted
  2^31 to 2^63-1 and gave its own text beyond int64.
- **Leading zeros.** `l[007]`, `l[00]`, `l[01]` and `l[00000000002147483648]` are
  syntax errors, `l[0]` is fine. Local's lexer reads a zero as a number of its own and
  any other digits as one token, and reports the token after the first zero with its
  neighbours: `l[01]` gives `token: "1", near: "01]"`, `l[0123]` gives `token: "123",
  near: "0123]"`, `l[00]` gives `token: "0", near: "00]"`, `l[007]` gives `token: "0",
  near: "007"` and `l[0007]` gives `token: "0", near: "000"`.

The extension also cost three fixes: #262 (a negative placeholder index panicked), #259
(the conversion of the number) and #267 (the conversion used `int64(float)` or an
int-sized parse, so an out-of-range value behaved differently on arm64 and amd64). Each
of them handled values that only a placeholder can produce: neither a fraction nor a
negative number is a valid literal index. #164 lists the extension among the inputs
minidyn accepts and Local rejects.

The code that existed on `main` only for it (`git show origin/main:<file>`):
`(*Number).int64Value` (`interpreter/language/object.go`), `evalListIndexValue`'s
`evalIdentifier` fallback and `newNegativeListIndexError`, used by `indexAccessor.Set`
(`interpreter/language/evaluator.go`), `extractPathListIndex`'s `Eval` fallback
(`interpreter/language/project.go`), and the `values` argument of `UpdateTargetPaths`
(`interpreter/language.go`, `interpreter/language/update_targets.go`), which loaded the
expression values to resolve `l[:i]`.

## Decision

A list index is an integer literal, as in DynamoDB. The parser rejects any other word
between `[` and `]` (a name, a placeholder, a number that is not a valid literal, or a
keyword such as `OR` or `SET`; `isWordToken`), or any other token such as the `-` of
`l[-1]` or the `]` of `l[]`, with Local's text, as the first error of
the parse, so the cached parse (D6) carries it: `Parser.parseIndexExpression` adds
`Syntax error; token: "<token>", near: "[<token>]"` and keeps its detail in
`Parser.IndexSyntaxError` when it is the first error, so no caller recovers the kind of
error from the text; `interpreter.syntaxErrorAtToken` gives it the label of the
expression (`Invalid UpdateExpression: ...`).

Local reports it before the placeholder checks (D3), so:

- `interpreter.ValidateExpressionAttributeNamesDeclared` and
  `ValidateExpressionAttributeValuesDeclared` report it first, from the cached lexer
  scan (`language.ScanExpression`, which finds it in the same pass as the
  placeholders), with the label of the first expression that has one;
- both front ends' `validateExpressionAttributes` skip the unused-name and
  unused-value errors when `interpreter.HasInvalidListIndex` is true, and the core
  reports the syntax error.

The code that only a placeholder index needed is removed: the conversion of a number to
an index, the negative-index error of #262, the reasons for fractions, and the
`values` argument of `UpdateTargetPaths`.

The literals follow Local (D1, level 1) instead of minidyn's own int64 rule:

- **The rule.** A literal index is 0 to 2147483647 with no leading zero.
  `classifyListIndex` is the one place that knows: it gives the syntax detail of a token
  that is not digits or has a leading zero (`token: "0", near: "007"`), and the range
  message of a literal above the range. The parser, the lexer scan and `listIndexLiteral`
  (all in `interpreter/language/list_index.go`) use it, so they cannot disagree.
- **Leading zeros** are a located syntax error in the same parse pass as the placeholder
  check (D6), with the same `Invalid <Expr>: Syntax error; token: ..., near: ...` text.
  The token after the literal is taken to be the closing bracket, so whitespace between a
  literal and its bracket is not part of `near`; the parity harness compares only up to
  `Syntax error` (#188).
- **The range error** is found by the same pass but kept apart: `Parser.IndexRangeError`
  holds the first literal above the range, and it is not one of `Parser.Errors`. Local
  reports it only when the text has no syntax error, so a syntax error anywhere in the
  text (a placeholder, a leading zero, `= x`) comes first, before or after the literal.
  The first literal in the text is the one reported. `Language.Match`, `Update` and
  `CheckProjection` return it after the parse errors and before the placeholder checks,
  with the label of the expression (`Invalid UpdateExpression: List index is not within
  the allowable range; index: [2147483648]`). It unwraps to `ErrSyntaxError`, so the
  front ends map it to a `ValidationException` like the other expression errors.
- **Order against the placeholders.** `interpreter.ValidateExpressionAttributeNamesDeclared`
  and `ValidateExpressionAttributeValuesDeclared` report a syntax error of any expression
  first, then a range error, both before the undeclared names and values.
  `interpreter.HasInvalidListIndex` is true for a leading zero and a range error too, so
  the front ends skip the unused-name and unused-value errors. When the same text has a
  syntax error besides the range error, the validators return the error of the parse of
  that text, since they only lex it: it is the error DynamoDB reports, whether or not
  a value is undefined and whether or not an item is evaluated.
- **Evaluation.** `listIndexLiteral` converts exactly and enforces 0 to 2147483647. Its
  "must be an integer literal" reason is removed: the parser rejects an index that is not
  digits, so a tree built without it gets the range message, the one reason an index has
  for being refused. The evaluator and `ExtractPath` report it as before (the field, the
  path and the reason on an update; `invalid list index in a document path` on a read).

The alternatives were considered and rejected:

- **Keep the extension and harden it (PR #287 as it was).** One exact conversion and
  clear errors fix #267, but the extension stays: a test that passes on minidyn with
  `l[:i]` fails on DynamoDB, which is what minidyn exists to prevent (D1, level 2:
  Local rejects it, and the documentation has no such syntax). The maintainer's
  decision on this change was to follow DynamoDB.
- **Keep it behind an option.** It would keep a second grammar, a second set of error
  texts and the code of #259, #262 and #267 alive for a syntax no DynamoDB
  deployment accepts. Nobody asked for it.
- **Reject it only at evaluation time.** The error would depend on whether an item is
  read (D3): it would pass on an empty table, and it would come after the checks of
  the undefined values, unlike Local.

## Consequences

**Positive.** Every expression minidyn accepts has a list index that DynamoDB accepts.
#267 is gone for placeholders, since there is nothing to convert, and the dead code of
#259, #262 and #287 is removed. The error is reported once, at parse time, on the
cached parse, and in Local's order.

**Negative.** This breaks callers. A test or fixture that used `l[:i]`, `l[#n]` or
`l[name]` in any expression now fails with a `ValidationException` (`Invalid
UpdateExpression: Syntax error; token: ":i", near: "[:i]"`). The fix is to write the
index as a literal, as DynamoDB requires. minidyn's own code never builds such an
expression: `git grep -nE '\[[:#][A-Za-z0-9_]*\]' -- '*.go' ':!*_test.go'` matches only Go
slice expressions such as `checks[:2]`.

**Neutral.** The text after `Syntax error` is not compared by the parity harness (#188),
so the whole message is pinned by unit tests (`TestPlaceholderListIndexIsASyntaxError`,
`TestTablePlaceholderListIndexIsASyntaxError`, `TestLiteralListIndexLikeDynamoDBLocal`,
`TestTableLiteralListIndexLikeDynamoDBLocal`). Generic syntax errors, such as
`SET n = =`, still come after the unused-value check and without the `Invalid <X>:`
prefix; only this error was moved. Two gaps remain, tracked in #296 (the order of the
update expression against the condition expression) and #188 (the `near` text and the
generic syntax errors).

## References

- `interpreter/language/parser.go` — `parseIndexExpression`, `checkListIndex`,
  `IndexSyntaxError`, `IndexRangeError`
- `interpreter/language/expression_placeholders.go` — `ScanExpression`
- `interpreter/interpreter.go` — `listIndexError`, `syntaxErrorAtToken`,
  `listIndexRangeError`, `HasInvalidListIndex`
- `interpreter/language/list_index.go` — `classifyListIndex`, `isWordToken`,
  `listIndexLiteral`, the range and leading zero texts
- `e2e/parity_list_index_syntax_test.go` — the parity cases for each position, the
  keywords, the range boundary and the leading zeros
- `docs/architecture.md` — D1 (reference), D3 (order), D4 (text), D6 (parse once)
- `docs/interpreter.md` — the grammar of paths
- #164, #188, #259, #262, #267, #296
