---
name: defining-minidyn-tdd
description: Apply a rigorous Test-Driven Development (TDD) workflow for the Minidyn project. Use when adding features, fixing bugs, changing behavior, refactoring, or when the user mentions TDD, use cases, or "test first".
---

# Defining Minidyn TDD

## Purpose

Use this skill to apply a consistent TDD workflow in this repository:
1. Clarify use cases first.
2. Define test cases from those use cases.
3. Write tests before implementing.
4. Implement the minimum change to pass.
5. Verify behavior and report coverage/risks.

If there is tension between speed and order, maintain the TDD order: **tests first, then code**.

## Mandatory Workflow

Copy this checklist into your response and keep it updated:

```markdown
TDD Progress:
- [ ] Step 1: Clarify use cases
- [ ] Step 2: Convert use cases to test cases
- [ ] Step 3: Write failing tests first (RED)
- [ ] Step 4: Implement minimum code (GREEN)
- [ ] Step 5: Safe refactor (REFACTOR)
- [ ] Step 6: Verify and report
```

### Step 1: Clarify Use Cases
Before editing production code, define:
- Actor/context
- Input/trigger
- Expected output/effect
- Edge cases
- Error paths

Write these in short bullets. If there is ambiguity, ask clarifying questions before coding.

### Step 2: Convert Use Cases to Test Cases
For each use case, define at least:
- A happy path test
- An edge case test
- A failure/error test (when applicable)

Map each use case to test names with a 1:1 relationship when possible. Use this template:

```markdown
Use Case: <name>
- Given: <initial state>
- When: <action>
- Then: <expected result>
- Test File: <path>
- Test Name: <it/describe title>
```

### Step 3: Write Failing Tests First (RED)
- Create or update tests **before** implementing.
- Run specific tests and confirm they fail for the expected reason.
- If it's a bugfix: first reproduce the bug with a failing test.

**Evidence to capture in your response:**
- Test command executed
- Names of failing tests
- Short reason for failure

### Step 4: Implement Minimum Code (GREEN)
- Implement only what is necessary to pass the failing tests.
- Avoid unrelated refactors while moving from RED to GREEN.
- Keep changes small and local.

### Step 5: Safe Refactor (REFACTOR)
Optional, only after GREEN:
- Improve readability
- Remove duplication
- Maintain behavior unchanged
- Run affected tests after every non-trivial refactor.

### Step 6: Verify and Report
Always run:
1. Specific tests for the changed area: `go test -v -run ^TestName$ ./path/to/package`
2. Full suite for regressions: `go test ./...`
3. Coverage check (Must pass): `bash scripts/check_coverage.sh`

Report using this format:

```markdown
Verification Report
- Use cases covered: <n>/<total>
- Tests added: <list>
- Tests updated: <list>
- Commands executed: <list>
- Result: <pass/fail + key details>
- Remaining risks: <bullets or "none identified">
```

## Minidyn-Specific Considerations

- **Interpreter Changes**: If modifying `interpreter/language/evaluator.go` or `interpreter/language/functions.go`, tests must be added to `evaluator_test.go` or `functions_test.go`.
- **API Changes**: If DynamoDB input shapes change, remember to regenerate the requests file: `go run ./tools/generate_requests`.
- **Debugging**: Never touch non-test code until a failing reproducing test exists.

## Output Contract

When using this skill, your response MUST include:
1. Explicit use cases
2. Explicit test plan tied to use cases (Given/When/Then)
3. Confirmation that tests were written first (RED evidence)
4. Verification results with executed commands (Coverage & Suite)

If any point is missing, explicitly mark it as a gap before closing.
