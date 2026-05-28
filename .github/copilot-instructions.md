# Minidyn — GitHub Copilot

Before making changes, read [AGENTS.md](../AGENTS.md) and [`.agents/rules/workflow.mdc`](../.agents/rules/workflow.mdc). Those files are the canonical project instructions; this file is a Copilot entry point.

## Skills

Project skills live in `.agents/skills/`. Read a skill only when the task matches its description:

- `defining-minidyn-tdd` — TDD workflow for features, bugs, refactors
- `writing-minidyn-e2e-parity-tests` — DynamoDB Local vs minidyn parity tests
- `writing-minidyn-commits` — commit message format
- `releasing` — cut a release

## Verification

After implementation, follow `.agents/agents/qa-engineer.md`: run `go fix`, unit tests (excluding `e2e/`), `golangci-lint run`, and `bash scripts/check_coverage.sh` from the repo root.

## Workflow reminders

1. Clarify use cases, then write a failing test before implementation code.
2. Never modify `e2e/` tests to make them pass — they are parity references only.
3. Go 1.26; no external services required for unit tests.
4. Every package must contain a `doc.go` file.
5. Regenerate `server/requests.go` with `go run ./tools/generate_requests` when DynamoDB input shapes change.
