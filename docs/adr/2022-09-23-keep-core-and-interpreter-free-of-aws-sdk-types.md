# Keep core and interpreter free of AWS SDK types

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `c589609` (#65, merged as `8b68cfc`).

Before this decision, minidyn was one package that spoke the AWS SDK for Go v1
directly. The first commit, `1a89b93` (2021-08-02, "Extract minidyn code from
truora's monorepo"), has `table.go`, `index.go` and `types.go` at the repo
root importing `github.com/aws/aws-sdk-go/service/dynamodb`, and the
interpreter took and returned `*dynamodb.AttributeValue`. Storage, expression
evaluation and the client were all written in SDK types.

The decision was made in three commits by the maintainers of that time:

- `d8fbf86` (2022-09-20) moved storage into `core/` and the client into
  `awsv1/`. It changed layout only: `core/` and `interpreter/` still imported
  the SDK (checked with `git grep -l 'aws-sdk-go' d8fbf86 -- core interpreter`).
- `7c78760` (2022-09-22) added `types/` and replaced the SDK types in `core`.
  Its message gives the reason: "why need a generic core and interpreter".
  The message also says it replaced the types "in core and interpreter", but
  its diff touches no `interpreter/` file (`git show --stat 7c78760`).
- `c589609` (2022-09-23, "Remove aws sdk from core and interpreter") did the
  same in `interpreter/` and `interpreter/language/`, and grew `types/`
  (`types/types.go`: 90 lines added, 4 removed). Its message: "Why: to avoid
  using aws sdk v1". `17d4054` ("Fix types for package core", the same day)
  removed `awserr` from `core`.
  `d75011e` (2022-09-28) then renamed the client to `aws-v1/client` and added
  its `mapper.go`: "we changed the types of Minidyn so now we must map them to
  dynamodb to be able to work with the AWS SDK V1".

`7c78760` covered `core` only and `7bd6c78` removed a helper import without
stating the rule, so the date is `c589609`'s.

The decision was not complete at `c589609`. Non-test code in these packages
still imported SDK v1 helpers (`aws.String`, `awserr`, `awsutil`,
`private/protocol`):

- `interpreter`: gone by `17d4054` (2022-09-23); `core`: `core/types.go` kept
  `aws` until `7bd6c78` (2022-10-07). At `17d4054`,
  `git grep -n '"github.com/aws' 17d4054 -- core interpreter ':!*_test.go'`
  prints only `core/types.go:8`.
- `types`: `types/types.go` kept `aws/awsutil` and `private/protocol` for
  `ResponseMetadata` until `a1810fb` (2026-05-06), which replaced them with a
  local `types.ResponseMetadata` ("Shared types still imported v1-only
  helpers that have no direct v2 equivalent, so we carry minimal local types
  instead").
- `capacity` and `calculate-size` were never affected: `c8bc51b`
  (2026-06-19) added both, and neither has an SDK import.

Present state, checked on this branch (`docs-adrs-layers`, at `eecd863`):

```
$ grep -rln 'github.com/aws' --include='*.go' core interpreter capacity \
    types calculate-size | grep -v _test.go
core/table.go
$ grep -n 'github.com/aws' core/table.go
501:			// https://github.com/aws/aws-sdk-go/issues/3140
554:			// https://github.com/aws/aws-sdk-go/issues/3140
$ go list -deps ./core ./interpreter/... ./capacity ./types ./calculate-size \
    | grep -E 'aws|smithy'
$
```

`core/table.go` matches only because of a URL in two comments, not an import.
`go list -deps` (non-test dependencies) finds no `aws` or `smithy` package
under any of these five packages. Tests are another matter:

```
$ grep -rln 'aws-sdk-go' --include='*_test.go' core interpreter capacity \
    types calculate-size
core/table_test.go
core/update_return_values_test.go
core/partition_test.go
core/key_encoding_test.go
core/update_errors_test.go
core/start_key_missing_item_test.go
```

Each of those six files imports `github.com/aws/aws-sdk-go-v2/aws` (for
`aws.String` and similar helpers). The rule as practiced covers non-test code.

## Decision

`core` and `interpreter` (with `interpreter/language`) hold no AWS SDK types
and import no AWS SDK package in non-test code. The packages added or
cleaned afterwards, `capacity`, `types` and `calculate-size`, follow the same
rule today. They work on `types.Item`, `types.Error` and the other types in
`types/`. Each front end (today `aws-v2/client` and `server`) maps its own
shapes, SDK structs or DynamoDB JSON, to `types.*` on the way in and back on
the way out, and maps `types.Error` to its own error values.

`docs/architecture.md` section 1 lists "Import an AWS SDK package" under
"Must not" for `core`, and "Depend on the SDK" for `capacity`.

Rejected alternative:

- **Replaced: SDK types in the core.** Removed by the commits in Context,
  for the reasons quoted there.

No other alternatives were recorded when this was decided (searched:
`d8fbf86`, `7c78760`, `c589609`, `d75011e`, `1205850`, #65). #65, the pull
request that carried the work, says "We restructured the project in
different packages: core/ interpreter/ scripts/ types/" and "There were some
updates too in the types, we conserved some pointers but others were removed
since aws-sdk-v2 do not use pointers everywhere." It names no option that was
rejected.

## Consequences

**Positive.** A second SDK major version and an HTTP front end could be added
without touching storage or expression evaluation. This is what happened:
`aws-v2/client` was added in `1205850` (2022-10-18) and `server` later, both
over the same `core`. Removing the v1 client (`a1810fb`, see
`2026-05-06-support-only-aws-sdk-go-v2.md`) left `core` and `interpreter`
unchanged.

**Negative.** Every front end carries a mapper, and the mappers can drift from
each other. `docs/architecture.md` records the rule that behavior both front
ends need belongs in `core`. Errors need a second hop: `core` returns
`types.Error`, and each front end maps it (`mapKnownError` in
`aws-v2/client`).

**Neutral.** The rule is held by review, not by a tool: nothing in CI fails if
a non-test file in `core` imports the SDK, and six `core` test files import
`aws-sdk-go-v2/aws`. The commands in Context are the check.

## References

- `types/`, `core/table.go`, `docs/architecture.md` (section 1)
- `1a89b93`, `d8fbf86`, `7c78760`, `c589609`, `17d4054`, `d75011e`, `7bd6c78`,
  `a1810fb`; #65 (`8b68cfc`)
