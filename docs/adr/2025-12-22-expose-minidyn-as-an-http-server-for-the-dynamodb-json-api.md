# Expose minidyn as an HTTP server that speaks the DynamoDB JSON API

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `fb211ca` (#97).

Before this decision, minidyn had one way in: an in-process client that
stands in for the SDK's DynamoDB client (for v1, the
`dynamodbiface.DynamoDBAPI` interface). There was one such client per SDK
major version, `aws-v1/client` and `aws-v2/client`
(`git ls-tree -d --name-only fb211ca^` lists both). A test had to swap the
SDK client for the minidyn one, and a new SDK version meant a new client.

`fb211ca` (#97, issue #80) added the `server` package. Its commit message
gives the reason: "To let teams use minidyn via httptest/NewServer with AWS
SDK clients without maintaining separate SDK-specific variants." Issue #80
says the same: "This way we can relay in httptest and remove the deplicated
code for each aws-sdk version" (sic).

A later commit removed one of those variants for a different reason.
`a1810fb` (#116, issue #115) deleted `aws-v1/client` because "AWS SDK for
Go v1 is deprecated; the in-memory v1 client is no longer maintained". Today
the tree has no `aws-v1` directory: `git ls-files aws-v1 | wc -l` prints `0`.

## Decision

Besides the in-memory client, minidyn ships a `server` package. It is an
`http.Handler` (`server.NewServer()`) that decodes the DynamoDB JSON API and
runs it on the same `core` tables as the in-memory client. A test points an AWS SDK client at it with `httptest.NewServer`, as
`README.md` shows.

The HTTP layer has its own types, because the SDK's `AttributeValue` is an
interface that `encoding/json` cannot decode. `server/types.go` defines a
concrete `AttributeValue` struct, and `server/requests.go` holds the request
shapes (see
`2025-12-22-generate-the-server-request-types-from-the-sdk.md`).

The server has the test controls the in-memory client has. `0c779f5` (#99)
added `Reset` and `ClearTable`, and `5fb1f6b` (#100) added `ClearAllTables`,
all under issue #80, so that tests can isolate themselves between subtests.

Rejected alternative:

- **Recorded: one in-process client per SDK version.** As the status quo
  whose cost the change avoids rather than as an option that was weighed:
  the commit message of `fb211ca` and issue #80 say "without maintaining
  separate SDK-specific variants" and "remove the deplicated code for each
  aws-sdk version". Nothing else was written down about it.

No other alternatives were recorded when this was decided (searched:
`fb211ca`, #97, #80, `0c779f5`, #99, `5fb1f6b`, #100; none of them has
review comments or issue comments).

## Consequences

**Positive.** What the commit asked for: teams can use minidyn through
`httptest.NewServer` with AWS SDK clients, without an SDK-specific variant of
the client for each version.

**Negative.** There are two front ends over `core`, so a behavior that either
needs must live in `core`, and every request path is implemented twice (the
SDK mapper in `aws-v2/client`, the JSON mapper in `server`). The server has
its own `AttributeValue` and mapper (`server/mapper.go`), which can drift from
the SDK ones. The HTTP path adds JSON encoding to every call.

**Neutral.** `docs/architecture.md` section 1 describes the two front ends
and what each owns. `aws-v1` was removed afterwards for its own reasons
(`a1810fb`), not because of the server.

## References

- `server/` — the HTTP front end; `server/types.go` holds the concrete
  `AttributeValue`
- `README.md` — "HTTP server mode"
- `docs/architecture.md` — section 1, layers and request flow
- `fb211ca` (#97, issue #80), `0c779f5` (#99), `5fb1f6b` (#100), `a1810fb`
  (#116, issue #115)
