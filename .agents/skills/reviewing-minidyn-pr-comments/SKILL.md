---
name: reviewing-minidyn-pr-comments
description: Walks through review comments on a minidyn GitHub pull request one at a time, verifies each claim against the code, gets the maintainer's decision on every comment before changing anything, then implements the batch test-first and asks before pushing or replying. Use when asked to check, go through, answer or address PR comments or a review.
---

# Reviewing minidyn PR comments

Review comments are decided **one at a time, all before any code changes**, then
implemented as one batch. Nothing leaves the machine (push, reply, PR edit, issue
comment) without the maintainer's yes for that step.

## 1. Collect

```bash
gh pr view <PR> --json reviews --jq '.reviews[] | "\(.author.login) [\(.state)]: \(.body)"'
gh api repos/truora/minidyn/pulls/<PR>/comments \
  --jq '.[] | "\(.id)\t\(.user.login)\t\(.path):\(.line // .original_line)\t\(.body)"'
gh api repos/truora/minidyn/issues/<PR>/comments --jq '.[] | "\(.id)\t\(.user.login)\t\(.body)"'
```

- Count every point: inline comments, points inside a review body (nits listed
  there are separate items), and PR conversation comments.
- A comment needs a decision when it has **no reply from us**, or its **last message
  asks for more**. Skip threads already resolved or answered with nothing open.
- Read the code at the PR's head (`git show origin/<branch>:<path>`), not `main`.

## 2. Verify before presenting

Never repeat the reviewer's claim on trust:

- Read the code at the path and line in the comment, and the callers it depends on.
- Check it against `docs/architecture.md` (D1–D14) and `docs/adr/`. Name the decision a
  comment rests on (for example D10 for duplication, D4 for error text).
- For claims about DynamoDB behavior, the order of checks or an error text: DynamoDB's
  docs come first, then DynamoDB Local (D1). Say what was probed or how to probe it.
- Say plainly when the comment is right, partly right, or wrong, with `file:line`
  evidence.

## 3. Present one item, then wait

One item per message, in this format:

```
### #<PR>: <n> of <total> · `<path>:<line>`

**<Reviewer>'s comment**
> <quoted, trimmed with […]>

**Explanation**
<what is true and what is not, with file:line evidence; whether anyone can observe it
today; which decision (D1–D14) it touches>

**Suggested action**
- **a)** <concrete change, rough size, the test that proves it>
- **b)** <alternative, often "reply only" with the argument, or "file an issue">

**Recommendation: <letter>.** <one-line reason>
```

- Number items across the whole review ("3 of 8"). Points from the review body count.
- If one change closes two comments, say so and decide them together.
- When the maintainer answers ("a", "b", "a and b", "ok" = the recommendation),
  record the decision in one line and present the next item in the same message.
- If the maintainer asks something else mid-review, answer it, then bring back the
  pending item.
- After the last item, show a table of every decision before starting work.

## 4. Implement the batch

Only after every item is decided:

- Sync the branch first by **rebasing onto `origin/main`**, never by merging `main` in.
- Test first (`defining-minidyn-tdd`): each behavior change starts with a test that
  fails for the reason in the comment.
- A new parity case is probed against DynamoDB Local
  (`writing-minidyn-e2e-parity-tests`). If minidyn's order or text differs from
  Local, stop and bring it back as a finding instead of changing code on the spot.
  Same for a decided test that unexpectedly fails.
- Verify: `go test -race -count=1` over the non-e2e packages, golangci-lint,
  `./scripts/check_coverage.sh` (exit 0), `go test ./tools/...`, the full
  `go run ./tools/run-e2e -count=1`, and the fuzzer when behavior or order changed.
- Commit with `writing-minidyn-commits`: amend or add to the PR's commit, Why/What,
  lines of 80 columns or fewer, `Issue: #N`, no session links or AI attribution.
- An action decided as "file an issue" is filed after the batch is decided, and its
  number goes into the reply.

## 5. Replies, then ask

Draft one reply per thread (inline comment id, or a single review-level reply for
points made in the review body):

- Changed: "Done in <short sha>: <what changed>. <test that covers it>."
- Not changed: the evidence, with `file:line` or a command output, in two to four
  sentences.
- Filed: "Filed as #N: <one line>."

Then list for the maintainer, and wait for a yes on each:

1. push `<branch>` (`--force-with-lease=<branch>:<sha>` after a rebase)
2. post N replies (show the drafts)
3. PR description updates (show the diff)

After the yes:

```bash
git -c credential.helper= -c 'credential.helper=!gh auth git-credential' \
  push --force-with-lease=<branch>:<old sha> https://github.com/truora/minidyn.git HEAD:refs/heads/<branch>
gh api repos/truora/minidyn/pulls/<PR>/comments/<id>/replies -f body='<reply>'
gh pr edit <PR> --body-file <file>
```

## Rules

- Never change code, push, reply, or edit a PR before the maintainer has decided
  every item and said yes to that step.
- Never merge `main` into a PR branch; rebase.
- Never weaken an existing parity assertion or a CI check to make a comment go away.
- A fix outside the PR's scope gets its own issue or PR, with a reply that points
  to it.
- Report exactly what was pushed and posted, and what was not.
