# Make tests the specification, and track known parity gaps with issue markers

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `095be84` (#159). The parity suite compared
minidyn with DynamoDB Local since #108 (`ae9e58d`), but it "only compared the
values each test chose to return, and a failing case could only be fixed or
deleted" (#159). A known gap either broke CI or was removed from the suite and
forgotten.

#159 added `RunE2EKnown`: a case marked with the GitHub issue of a known
divergence is skipped while it fails, and fails once it passes, "so a fixed bug
can't leave a stale skip". The same PR added the differential fuzzer
(`9a33b75`), whose generator avoids areas with an open issue by default, and the
`parity-surface` tool (`f4480d9`), whose allowlist is enforced by
`go test ./tools/...`.

Facts on this branch:

- `git grep -n 'knownIssue: "#' -- 'e2e/*_test.go' | wc -l` prints `11`: ten markers
  for open issues plus the #153 divergence marker.
- `git grep -n '#[0-9][0-9][0-9]' -- e2e/fuzz_gen_test.go | wc -l` prints `11`:
  the fuzzer restrictions that name an issue.
- `go test ./tools/...` passes on this branch. CI runs it (#254), so the
  allowlist check gates every pull request.

## Decision

- Every behavior change starts with a failing test, in the package that owns the
  behavior. A change to front-end code needs tests in both front ends. New
  DynamoDB behavior gets a parity case in `e2e/`.
- A known divergence is tracked as a GitHub issue and marked with
  `knownIssue: "#N"` on every test that hits it, run through `RunE2EKnown`. The
  issue number is the only record, and tests refer to it.
- Parity tests are never weakened. When an issue is fixed, its `knownIssue`
  markers, its fuzzer restrictions and its `parity-surface` allowlist entries are
  removed. An intentional divergence from DynamoDB is documented instead (see the
  ADR on the AWS documentation as the reference).
- The differential fuzzer runs when request handling, key encoding or validation
  order changes, and not after a rebase with no conflicts.

The alternatives were considered and rejected:

- **Fix the failing case or delete it.** This was the only choice before
  `RunE2EKnown` (#159).
- **Keep the known issues as files in the repository.** #159 records the
  choice: "tracked only as GitHub issues ... no issue files in the repo, tests
  reference the issue number".

## Consequences

**Positive.** CI stays green while known gaps stay visible, and a fixed bug
cannot keep a stale skip. The fuzzer runs green by default, so any failure is
new.

**Negative.** `RunE2EKnown` re-runs the case in a child test process, because
`require` failures cannot be intercepted in-process. The `e2e/` suite needs
Docker, so CI does not run it (#159): of the parity tooling, only `go test ./tools/...` gates CI. A
marker, a fuzzer restriction and an allowlist entry must be removed by hand when
an issue is fixed.

**Neutral.** An intentional divergence documented under D1 is asserted per
engine; only #153 still carries a marker.

## References

- `e2e/known_issue_test.go`, `e2e/fuzz_gen_test.go`, `e2e/README.md`,
  `tools/parity-surface/allowlist.txt`
- `docs/architecture.md` (D11), `.agents/agents/qa-engineer.md`
- #108, #153, #159, #254
