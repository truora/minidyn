# Support only AWS SDK for Go v2 and remove the v1 client

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `a1810fb` (#116).

Before this decision minidyn shipped a client for each SDK major version:

- The AWS SDK for Go v1 client is the oldest code in the repo. `1a89b93`
  (2021-08-02) has `client.go` at the root importing
  `github.com/aws/aws-sdk-go/service/dynamodb`. `d8fbf86` (2022-09-20) moved
  it to `awsv1/`, and `d75011e` (2022-09-28) renamed it `aws-v1/client` and
  added a mapper to the internal `types.*` (see
  `2022-09-23-keep-core-and-interpreter-free-of-aws-sdk-types.md`).
- The v2 client, `aws-v2/client`, came in `1205850` (2022-10-18, "To support
  sdk-aws-go-v2 in minidyn"). It merged in #65, "Support Go AWS-SDK-V2 in
  Minidyn" (merged 2022-11-10).

Issue #115, "Drop AWS SDK for Go v1 support", was opened on 2026-05-06. Its
whole body is: "AWS SDK for Go v1 is deprecated". The same day `a1810fb`
removed the v1 client; #116, "Drop aws sdk go v1 support" ("also increase
converage and update dependencies with vulnarabilities"), merged on
2026-05-07 as `59d4504`. After the removal, `git ls-files 'aws-v1*' | wc -l`
prints 0 and `git grep -n 'aws-sdk-go/' HEAD -- '*.go' go.mod` prints only the
two URL comments in `core/table.go`.

The v1 client also kept `types/types.go` tied to v1: it imported
`aws/awsutil` and `private/protocol`, which have no v2 equivalent. The commit
message of `a1810fb` says: "Shared types still imported v1-only helpers that
have no direct v2 equivalent, so we carry minimal local types instead."

## Decision

minidyn supports only the AWS SDK for Go v2. `a1810fb` deleted the
`aws-v1/client` package (`git show --stat --format= a1810fb -- aws-v1`: 7
files, 3,714 deletions; the whole commit is 12 files, 19 insertions and
3,736 deletions), removed `github.com/aws/aws-sdk-go`
from `go.mod`, and replaced the v1 `protocol.ResponseMetadata` and `awsutil`
use in `types/` with a local `types.ResponseMetadata`. The in-memory client is
`aws-v2/client`; the HTTP `server` (added in `fb211ca`) is the other front
end.

Rejected alternative:

- **Replaced: keep both clients.** This was the state from `1205850` until
  `a1810fb`. `a1810fb` removed it because "AWS SDK for Go v1 is deprecated;
  the in-memory v1 client is no longer maintained."

No other alternatives were recorded when this was decided (searched:
`a1810fb`, #116 and its review threads, #115 and its one comment, `1205850`, #65). The only comment
on #115 (JuanJoseGonGi, 2026-05-07) is "I think we can close this one".

## Consequences

**Positive.** One client to keep at parity with DynamoDB, and `go.mod` no
longer carries the v1 SDK. `types` and `core` are SDK-free for real, not only
for the v1 helpers they had borrowed.

**Negative.** Breaking for callers on SDK v1: the `aws-v1/client` import path
and its `NewClient` no longer exist. Neither the commit message nor #115 or #116
describes a migration path.
`types.ConditionalCheckFailedException.String()` changed format
(`awsutil.Prettify` to `fmt.Sprintf`), flagged in review on #116 as an
observable change with no release note. The reviewer (JuanJoseGonGi) wrote:
"worth calling out as an observable breaking change in the PR description /
CHANGELOG so consumers know to look at it." The maintainer (kuadrosx) replied:
"We don’t maintain a CHANGELOG today; starting one is reasonable, but I’d
prefer not to introduce it in this PR so the scope stays focused on dropping
the v1 SDK."

**Neutral.** `README.md` still documents the v1 client: Gap (#288).

## References

- `aws-v2/client` — the only in-memory client
- `types/types.go` — now defines `types.ResponseMetadata`
- `a1810fb` — removal (#116, issue #115)
- `1205850` — v2 client added (#65)
- `d8fbf86`, `d75011e` — v1 client moved and renamed
- `2022-09-23-keep-core-and-interpreter-free-of-aws-sdk-types.md`
- `README.md` — stale usage sections
