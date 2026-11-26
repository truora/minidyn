# Share the interpreter settings with every table by pointer, and lock native registration

**Status:** Accepted

## Context

Before this change each table held its own copy of the interpreter settings.
`CreateTable` assigned `*c.nativeInterpreter` and `*c.langInterpreter` to the table by
value (`git grep -n 'NativeInterpreter = \*\|LangInterpreter = \*' 59250b4 -- '*.go'`
lists `aws-v2/client/client.go` lines 257, 285 and 287, and `server/client.go` lines 203
and 205). Two bugs followed (#250, found while reviewing #217):

1. `ActivateDebug` changed the client's `Language` only. A table created earlier kept its
   copy, so debug never reached it.
2. `Native` holds matcher and updater maps. A by-value copy shares those maps with the
   client's instance. `AddMatcher` and `AddUpdater`, called on the object
   `GetNativeInterpreter` returns (the README documents
   `client.GetNativeInterpreter().AddUpdater(...)`), wrote the maps without the client
   lock, while an operation read them under it. That is a data race. Running the new
   `TestNativeRegistrationRacesOperations` against the parent commit with
   `go test -race -run TestNativeRegistrationRacesOperations ./server` reports
   `WARNING: DATA RACE`: a read in `interpreter.(*Native).getMatcher` (from
   `core.(*Table).Put` through `matchKey`) against a previous write in
   `interpreter.(*Native).AddMatcher`.

D7 says `core` and the interpreter do no locking, and that the client lock covers every
public operation. Registration on `*Native` is not a client operation, so the lock does
not cover it.

## Decision

`core.Table` holds `*interpreter.Native` and `*interpreter.Language` in its exported
`NativeInterpreter` and `LangInterpreter` fields. Each front end assigns the client's
single instance to every table it creates, so a setting reaches existing and later
tables. `core.NewTable` creates default instances, so a table works without setup.

`Native` guards its matcher and updater maps with its own `sync.RWMutex`. Matchers and
updaters are looked up under the read lock and run outside it, but still under the
client lock, so a matcher must not call the client (as before). D7 gains this as its
second locking exception, after the expression caches.

The alternatives were considered and rejected:

- **Register through client methods that take the lock.** `GetNativeInterpreter`
  returns `*Native`, and the README documents calling `AddUpdater` on it, so the pointer
  would still be writable without the lock. The server has no public knobs to put such
  methods on.
- **Copy-on-write maps behind an `atomic.Pointer`.** More machinery than a mutex, and the
  tables would still need to share the pointer for bug 1.
- **Document "register before concurrent use".** It leaves a `-race` failure reachable
  and does not fix bug 1.

## Consequences

**Positive.** Settings reach every table. Registration is free of data races. `Native`
now contains a mutex, so `go vet`'s copylocks check rejects a by-value copy.

**Negative.** A public break: `core.Table.NativeInterpreter` and `LangInterpreter` went
from values to pointers, so callers that assign or read them as values no longer compile.
There is a second lock, so "the interpreter does no locking" is weaker. A `Native` passed
to `SetInterpreter` on two clients is shared by both. An operation running during a
registration may see the old or the new matcher.

**Neutral.** `NewTable` allocates defaults that the front ends then replace.

## References

- `core/table.go` — the `Table` fields and `NewTable`
- `interpreter/native.go` — `Native` and its `RWMutex`
- `docs/architecture.md` — the decision this changes (D7)
- `docs/adr/2026-11-11-hold-one-client-lock-per-public-operation.md` — stays Accepted;
  this ADR extends it
- #250, #217
