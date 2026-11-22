# Interpret DynamoDB expressions with a hand-written lexer, parser and evaluator

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `1a89b93` (issue #1; the commit has no pull
request).

minidyn has to run DynamoDB expressions itself: `ConditionExpression`,
`FilterExpression`, `KeyConditionExpression`, `ProjectionExpression` and
`UpdateExpression` (`docs/interpreter.md`). It needs the value of a condition
on an item, the attributes a projection keeps, and the item an update
produces. The results have to match DynamoDB, so the evaluator needs its own
object model for DynamoDB's types (N, S, B, BOOL, NULL, L, M, SS, NS, BS) and
for DynamoDB's own errors.

The first commit of this repository, `1a89b93` (2021-08-02, "Extract minidyn
code from truora's monorepo"), already holds the whole pipeline in
`interpreter/language`: `lexer.go`, `token.go`, `parser.go`, `ast.go`,
`evaluator.go`, `object.go`, `environment.go`, `functions.go` and `mapper.go`,
with a test file for each except `token.go` and `mapper.go`. Its message says why the commit exists: "At truora
we decided to share this library with the OSS community. ... It copies the
code from our monorepo." So the decision was taken in truora's monorepo, and
that history is not in this repository.

The date of this ADR is the author date of `1a89b93`
(`git log --diff-filter=A --format='%h %ad' --date=short -- interpreter/language/lexer.go interpreter/language/parser.go interpreter/language/evaluator.go interpreter/language/ast.go`
prints `1a89b93 2021-08-02`, the only commit).

Facts that hold on the branch `docs-adrs-layers` at `eecd863`:

- `grep -n -i -E 'antlr|yacc|participle|pigeon|peg|lex|pars' go.mod` prints
  nothing (exit status 1). No parser generator or parser library is a
  dependency. At `1a89b93` the same pattern on `go.mod` also matches nothing
  (`git show 1a89b93:go.mod | grep -c -i -E '<same pattern>'` prints 0); its
  only direct requirements were `github.com/aws/aws-sdk-go` and
  `github.com/stretchr/testify`.
- The non-test files of `interpreter/language` import only `bytes`, `fmt`,
  `reflect`, `slices`, `sort`, `strconv`, `strings` and
  `github.com/truora/minidyn/types`
  (`for f in $(ls interpreter/language/*.go | grep -v _test); do grep -h '^<tab>"[^"]*"$' $f; done | sort -u`).

## Decision

DynamoDB expressions are lexed, parsed and evaluated by code in this
repository, in `interpreter/language`, with no parser generator and no
third-party parsing or expression library:

- the lexer (`lexer.go`, `token.go`) turns the text into tokens, including the
  `#name` and `:value` placeholders;
- the parser (`parser.go`) builds the AST (`ast.go`);
- the evaluator (`evaluator.go`) walks the AST over an `Environment` and an
  object model (`object.go`) that mirrors DynamoDB's types, and returns
  `*language.Error` objects instead of Go errors.

The grammar is the one in `docs/interpreter.md`. Later changes stayed inside
the interpreter: `cff5d55` (2021-08-21, issue #3) added
`SET` to update expressions, and `571f4c7` (2021-08-21, issue #3) removed the
`DynamoExpression` AST node because "Dynamodb languages do not have more than
one expression so we can start the AST from the Expression node".

No alternatives were recorded when this was decided (searched: `1a89b93`,
issue #1, issue #3, `cff5d55`, `571f4c7`, `docs/interpreter.md`, and
`git log -i --grep='antlr\|goyacc\|pigeon\|participle\|parser generator'`,
which finds no commit). The decision was taken before the code was extracted
from truora's monorepo, and that history is not available.

## Consequences

**Positive.** The module has no parsing dependency to track or to upgrade
(`go.mod` names none). Later features were built on the same interpreter: the
parse cache (`docs/architecture.md`, section 1 and D6).

**Negative.** The grammar, operator precedence and every built-in function
are maintained by hand. A syntax DynamoDB accepts and the parser does not is a
bug in minidyn, and `docs/interpreter.md` has to say what is supported.
A new expression feature can touch the lexer, the parser and the evaluator,
and each needs its own tests.

**Neutral.** Users can still replace the evaluation of an expression the
interpreter does not support with the native interpreter
(`interpreter/native.go`), which was also in `1a89b93`.

## References

- `interpreter/language/` — lexer, parser, AST, evaluator, object model
- `docs/interpreter.md` — the supported grammar
- `docs/architecture.md` — section 1 (layers) and D6 (parse cache)
- `1a89b93` — extraction from truora's monorepo (#1); `cff5d55`, `571f4c7`
  (#3)
