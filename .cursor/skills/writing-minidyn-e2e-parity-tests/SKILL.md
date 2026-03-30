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
