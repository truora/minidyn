# Parse and scan each expression text once, in a bounded process-wide cache

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `50f48a3` (#160, issue #158). Query, Scan and
filters lexed and parsed the same expression for every item they visited. The
commit message puts that at "about 70% of the per-item matching cost". Issue
#158 measured it with the new `benchmarks/` suite: a one-partition Query cost
as much as a full Scan, and per-item allocations were high.

Issue #158 listed the options: "Parsing each expression once per request (or
caching the parsed AST by expression string) would cut the per-item cost of
every Query, Scan and filter by about two thirds, including full scans."

The decision grew with the code that followed. #207 extended the cache to
placeholder lists (`exprPlaceholders`). Maintainer review of #207 asked whether
parsing the function twice was really required, and the parsing was
deduplicated. `parseUpdate` (#165, #218) and `parseProjection` (#245) joined the
other caches.

Facts on this branch:

- `git grep -n 'NewLexer(' -- '*.go' ':!*_test.go' ':!interpreter/language/lexer.go'`
  prints four lines: `expression_placeholders.go` (the placeholder scan) and
  the three cache loaders in `interpreter/language_cache.go`. No other
  non-test code builds a lexer.
- `git grep -n 'maxParsedExpressions' -- '*.go' ':!*_test.go'` shows the single
  bound (`1024`) shared by the caches.

## Decision

Anything derived only from an expression's text lives in a bounded,
process-wide cache keyed by that text: the parse, the placeholder lists,
operand-count errors and key condition analysis. It does not belong in a fresh
lexer pass per request or per item. The AST is never changed by evaluation, so
cached entries are shared. An expression is not lexed twice in one request, and
an inner layer does not repeat a check an outer layer already made.

The alternatives were considered and rejected:

- **Parse once per item.** This was the state before #160, and the measured
  cost above.
- **Parse once per request, without a cache.** Issue #158 named it next to the
  cache. #160 chose the process-wide cache; the stated gain is that tests repeat
  the same expressions constantly (D6 in `docs/architecture.md`). No further
  reason for the choice over per-request parsing was recorded.

## Consequences

**Positive.** Per the commit message, Query and Scan time dropped by about 45%
and per-item allocations from 58 to 23. `TestAllocationsDoNotGrow`
(`benchmarks/`) guards the scaling in the normal test suite.

**Negative.** The caches are shared state. Each has its own mutex, and a bound
of 1024 entries means that a process with more distinct expressions pays for
re-parsing. A cached AST must never be mutated, and new request code that calls
`NewLexer` or `NewParser` on a text the cache could hold is a review finding.

**Neutral.** `core` and the rest of the interpreter stay lock-free; the cache
mutexes are the only locks outside the client (see the ADR on the client lock).

## References

- `interpreter/language_cache.go`, `interpreter/language/expression_placeholders.go`
- `benchmarks/README.md`
- `docs/architecture.md` (D6)
- #158, #160, #165, #207, #218, #245
