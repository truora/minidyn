# Treat core and the other non-front-end packages as an unsupported API

**Status:** Accepted

## Context

minidyn is a library, and `core`, `interpreter`, `capacity`, `types` and
`calculate-size` are exported Go packages that anyone can import. Their exported
identifiers change as the front ends need them to.

Two open changes show the cost of treating those identifiers as a public API:

- #199 changes the signature of `Table.UpdateTable` in `core`. It now takes a
  `*types.UpdateTableInput` so that one call can apply the billing mode, the
  capacity, the deletion protection and the index changes completely or not at
  all (D8). Any caller of the old signature stops compiling.
- #250 changes fields of `types` structs to pointers, which breaks every caller
  that builds or reads them.

The ADR rule in `docs/adr/README.md` requires an ADR for a change that "changes
the public API in a way that breaks callers". If `core` and `types` are public
API, both changes need one, and so does almost every change that touches an
exported identifier outside the front ends. The README documents `aws-v2/client`
and `server` as what users import; nothing says that the other packages are API.

## Decision

The supported public API is the two front ends: `aws-v2/client` and `server`.
`core`, `interpreter`, `capacity`, `types`, `calculate-size` and every other
package that is not a front end are **not a supported public API**. Users import
`aws-v2/client` or `server`. The exported identifiers of the other packages may
change without notice, in any release, and such a change does not need an ADR
under the "public API" trigger. "Public API" in `docs/adr/README.md` means the
front-end packages.

`docs/architecture.md` (section 1), `docs/adr/README.md` and `README.md` state
this.

The alternatives were considered and rejected:

- **Write an ADR for every break of `core` or `types`.** It keeps the promise
  that the packages are API, but it records churn that the design does not
  want: the front ends are the only supported callers, and the ADRs would say
  the same thing each time. It also still breaks the callers.
- **Move the packages under `internal/`.** It enforces the decision: the
  compiler stops anyone importing them. It is a larger change (every import path
  changes, `tools/` and the e2e tests import some of them, and the
  `defining-minidyn-tdd` and release tooling name the paths), so it needs its
  own change and its own ADR. This decision does not prevent it.

## Consequences

**Positive.** `core` and `types` can take the shape the front ends need without
an ADR for each signature change. The ADR rule becomes narrower and checkable:
a break of `aws-v2/client` or `server`.

**Negative.** Users who imported `core` or `types` directly have no stability
promise, and the first release that changes a signature breaks them without a
deprecation period. Nothing enforces the boundary until the packages move under
`internal/`.

**Neutral.** The front-end packages keep their compatibility rules. Release
notes still mention a change to the exported identifiers of the other packages
when it is likely to matter.

## References

- `core/table.go` — `Table.UpdateTable` (#199)
- `docs/architecture.md` — section 1, layers and the supported API
- `docs/adr/README.md` — "When an ADR is required"
- #199, #250
