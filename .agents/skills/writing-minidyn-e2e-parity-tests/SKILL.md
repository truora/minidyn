---
name: writing-minidyn-e2e-parity-tests
description: Writes DynamoDB Local vs minidyn parity E2E tests with the repository's comparison patterns, normalization rules, and validation workflow. Use when adding or updating parity tests in e2e/, investigating parity mismatches, or when the user asks to write E2E parity coverage.
---

# Writing Minidyn E2E Parity Tests

## Purpose

Use this skill to add or update parity E2E tests that compare `minidyn` and DynamoDB Local using the project pattern in `e2e/`.

This skill is focused on parity behavior only (not unit tests, not AWS client package tests).

## Workflow Checklist

Copy this checklist into your response and keep it updated:

```markdown
Parity E2E Progress:
- [ ] Step 1: Define parity use case and expected AWS behavior
- [ ] Step 2: Choose the right parity test file and test name
- [ ] Step 3: Implement test using `RunE2E`
- [ ] Step 4: Normalize error text only when needed
- [ ] Step 5: Execute focused parity tests
- [ ] Step 6: Report parity result and risks
```

## Step 1: Define parity use case

Before writing code, state:
- DynamoDB operation under test (for example `PutItem`, `Query`, `UpdateTable`)
- Input shape that may diverge
- Expected parity output (response fields, sorted collections, or normalized error string)

If behavior is uncertain, define the test to discover DynamoDB Local behavior first and keep minidyn aligned to that.

## Step 2: Pick file and naming

Use existing `e2e/*` parity files by operation domain:
- Item and basic item flows: `e2e/parity_item_test.go`
- Query/batch specific behavior: `e2e/parity_query_batch_test.go`
- Shared setup/utilities: `e2e/parity_helpers_test.go`
- Common E2E harness and normalization: `e2e/e2e_test.go`

Naming rules:
- Top-level test: `TestE2E_<Domain>`
- Subtest names: short behavior statements, for example `PutWithGSI`, `GetItemWithUnusedAttributes`

## Step 3: Implement using RunE2E pattern

Always follow this structure:
1. Build a `tests := []struct{name, fn}` table.
2. In each `fn`, call `t.Helper()` and `ctx := context.Background()`.
3. Set up table/index state with parity helpers.
4. Execute operation(s) against the injected `*dynamodb.Client`.
5. Return a stable value for comparison across both engines.

Use stable comparison outputs:
- Primitive slices with deterministic order
- Helper-sorted representations for unordered sets
- Normalized error strings for known message formatting differences

## Step 4: Error normalization rules

When comparing errors, normalize SDK strings through `normalizeSDKErrorString`.

Current normalization includes:
- RequestID stripping
- Known DynamoDB Local text differences
- Trimming extra minidyn diagnostic suffixes after `;`

Guideline:
- Keep parity checks strict on semantic error meaning.
- Normalize only known formatting differences that are not behavior differences.

## Step 5: Execute focused tests

From the repository root, use the helper command (runs `go test -json` and prints only failed tests, or `OK` if none failed):

```bash
go run ./tools/run-e2e TestE2E_Item/PutWithGSI
```

Optional extra arguments are forwarded to `go test` (after `./e2e/...`, `-json`, and `-run`):

```bash
go run ./tools/run-e2e TestE2E_Item/PutWithGSI -count=1
```

Or run the full parity package:

```bash
go run ./tools/run-e2e
```

### Known divergences

If a new test exposes a minidyn bug you are not fixing now, do not leave the suite red and
do not weaken the test. File a GitHub issue (repro, expected DynamoDB Local behavior,
actual minidyn behavior, where to look) and set `knownIssue: "#<number>"` on the case,
which must run through `RunE2EKnown` instead of `RunE2E`. The case is then skipped while it
still fails and **fails** once it passes, so the marker and the issue can't go stale.
`MINIDYN_E2E_RUN_KNOWN=1` runs known-issue cases as normal tests while fixing one.

### Finding gaps proactively

- `MINIDYN_E2E_TRACE=report go run ./tools/run-e2e -count=1` also diffs every raw response
  and the final table/index contents of each test (`strict` fails on differences;
  `MINIDYN_E2E_TRACE_OUT=<file>` writes one JSON line per diverging test). Benign
  differences are normalized in `e2e/trace_test.go`.
- `go run ./tools/parity-surface` lists unimplemented operations, request fields that are
  decoded but never read and response fields that are never set. Its allowlist
  (`tools/parity-surface/allowlist.txt`) is enforced by `go test ./tools/...`: remove the
  entry when you close a gap, and triage any new one.
- `go run ./tools/run-e2e TestE2E_Fuzz` runs random operation sequences against both
  engines (`MINIDYN_E2E_FUZZ_SEEDS`, `_OPS`, `_SEED`, `_KNOWN`; see `e2e/README.md`). Turn a fuzz failure into a minimal case in
  `e2e/parity_fuzz_findings_test.go`; when fixing an issue, remove the generator
  restriction that names it.

## Step 6: Report result

Report with this format:

```markdown
Parity E2E Report
- Use case: <one-line behavior>
- Test file: <path>
- Added/updated tests: <list>
- Commands run: <list>
- Result: <pass/fail>
- If failed: <first mismatch observed>
- Risks/gaps: <bullets or "none identified">
```

## Guardrails

- Do not modify tests in `e2e/` just to force green results when parity is genuinely different; fix minidyn behavior instead.
- Keep assertions deterministic (sort or normalize when response order is undefined).
- Prefer extending existing parity helpers over duplicating setup logic in each subtest.
