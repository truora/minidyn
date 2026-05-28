# Minidyn — Claude Code

Before making changes, read [AGENTS.md](AGENTS.md) and [`.agents/rules/workflow.mdc`](.agents/rules/workflow.mdc). Those files are the canonical project instructions; this file is a Claude Code entry point.

## Skills

Project skills live in `.agents/skills/` (symlinked under `.claude/skills/`). Read a skill only when the task matches its description:

- `defining-minidyn-tdd` — TDD workflow for features, bugs, refactors
- `writing-minidyn-e2e-parity-tests` — DynamoDB Local vs minidyn parity tests
- `writing-minidyn-commits` — commit message format
- `releasing` — cut a release

## Sub-agents

- **qa-engineer** — run after implementation to verify tests, lint, and coverage (see `.agents/agents/qa-engineer.md`)

## Workflow reminders

1. Clarify use cases, then write a failing test before implementation code.
2. Never modify `e2e/` tests to make them pass — they are parity references only.
3. Run qa-engineer verification before treating work as merge-ready.
4. Regenerate `server/requests.go` with `go run ./tools/generate_requests` when DynamoDB input shapes change.
