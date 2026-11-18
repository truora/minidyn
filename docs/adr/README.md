# Architectural Decision Records

An ADR records one architectural decision: the problem, what was decided, the
alternatives that were rejected, and what the decision costs. It keeps the
reasoning that the code and [`docs/architecture.md`](../architecture.md) do not.

- `docs/architecture.md` describes **how minidyn is built today**. It changes
  whenever `main` changes.
- An ADR records **why** a decision was taken, at the time it was taken. Once
  accepted, it is history: it is not rewritten to follow `main`.

## When an ADR is required

Write an ADR, in the same pull request as the code, when a change:

- adds, changes or reverses a decision in `docs/architecture.md` (D1, D2, …);
- adds a package, a layer or a front end;
- adds a runtime dependency (a `require` that is not test-only);
- changes the public API in a way that breaks callers;
- deliberately diverges from DynamoDB's documented behavior.

A bug fix or a feature that follows the existing decisions does not need one.

## File name

```
docs/adr/YYYY-MM-DD-short-title-in-kebab-case.md
```

- The date is the day the ADR is **created** (first written), not the day it
  merges. Never rename the file afterwards: commits, pull requests and other
  documents cite it by name.
- The title says the decision, not the topic:
  `2026-11-16-validate-requests-before-reading-items.md`, not
  `2026-11-16-validation.md`.
- Dates replace sequence numbers so that parallel branches never pick the same
  identifier. Two ADRs created on the same day are told apart by their titles.
- Cite an ADR by its full file name.

### Backfilled ADRs

A decision made before this rule existed is recorded with the date it was
**originally made**: the author date of the commit that introduced it in its
current form (`git log -S'<symbol>' --format='%h %ad' --date=short | tail -1`).
The first line of its Context says when it was written down:
"Backfilled on YYYY-MM-DD from `<sha>` (#PR)."

## Status

| Status | Meaning |
|---|---|
| `Proposed` | Under review in an open pull request |
| `Accepted` | Merged; the code follows it |
| `Superseded by YYYY-MM-DD-….md` | A later ADR replaced it |
| `Deprecated` | No longer applies, and nothing replaced it |

Once an ADR is `Accepted`, change only its **Status** line. To change the
decision, write a new ADR and mark the old one `Superseded by` the new file.

## Format

Copy [`template.md`](template.md). Every ADR has these sections: **Title**,
**Status**, **Context**, **Decision**, **Consequences**. **References** is
optional.

- **Context** — the problem and the facts that force a decision. Back every
  number and every "no other callers" claim with a command run on the branch.
- **Decision** — what was decided, then each rejected alternative and why it
  lost.
- **Consequences** — positive, negative and neutral effects, including the
  costs.

When the ADR changes a decision in `docs/architecture.md`, update that entry in
the same pull request and link the ADR from it.
