# Minidyn — Agent Guidelines

Minidyn is an in-memory Amazon DynamoDB testing library written in Go (`github.com/truora/minidyn`).

## Canonical configuration

All agent configuration lives in **`.agents/`** (editor-agnostic). Editor-specific entry points symlink or reference this directory:

| Editor | Entry point |
|--------|-------------|
| Any | `AGENTS.md` (this file) |
| Cursor | `.cursor/` → `.agents/` |
| Claude Code | `CLAUDE.md`, `.claude/` → `.agents/` |
| GitHub Copilot | `.github/copilot-instructions.md` |

| Path | Purpose |
|------|---------|
| `.agents/rules/workflow.mdc` | Mandatory workflow: TDD, verification, project structure |
| `.agents/skills/` | Task-specific skills (TDD, commits, E2E parity, releases) |
| `.agents/agents/qa-engineer.md` | QA sub-agent: tests, lint, coverage gate |

**Read `.agents/rules/workflow.mdc` first** — it defines the implementation flow, debugging rules, and when to invoke skills and sub-agents.

## Skills

Discover skills progressively — only read a skill when it is directly relevant:

- `defining-minidyn-tdd` — TDD workflow for features, bugs, refactors
- `writing-minidyn-e2e-parity-tests` — DynamoDB Local vs minidyn parity tests
- `writing-minidyn-commits` — commit message format
- `releasing` — cut a release (Keep a Changelog, tags, draft GitHub release)

## Verification

After implementation, invoke the **qa-engineer** sub-agent (see `.agents/agents/qa-engineer.md`) before treating work as merge-ready.

## Key constraints

- Go 1.26; no external services required for unit tests
- **Never** modify `e2e/` tests to make them pass — they are parity references only
- `server/requests.go` is generated — run `go run ./tools/generate_requests` when input shapes change
- Every package must contain a `doc.go` file
