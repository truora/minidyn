# Start errors with DynamoDB's exact text and add minidyn's detail after " | "

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `1ef3d93` (#193, issue #187). Before it, minidyn
appended its own debug detail to error messages after `; `, and the parity
normalizer cut every message at the first `;` to hide it. DynamoDB uses `;` in
its own messages too (for example `...function; operator: DELETE, operand type:
STRING, typeSet: ...`). So the suite never compared DynamoDB's text after the
`;`, and some real mismatches went unnoticed (#193). Some minidyn messages also
differed from DynamoDB's text: unquoted invalid keys and the ADD operand errors.

minidyn still wants to say more than DynamoDB does, because people read these
messages while debugging their tests.

Facts on this branch:

- `git grep -n 'WithDetail(\|types.Detail(' -- '*.go' ':!*_test.go' | wc -l`
  prints `23`: the two helpers (definitions included) are used across `core`,
  the interpreter and both front ends.

## Decision

- The message starts with DynamoDB's exact text, including any `;` part that
  DynamoDB returns.
- What minidyn knows and DynamoDB's text does not say goes after ` | ` as
  `key: value` pairs separated by `; `, built with `types.WithDetail` (or
  `types.Detail` when wrapping an error). The parity normalizer strips
  everything from the last ` | `, so detail never affects parity.
- When a minidyn-only message is changed to DynamoDB's text, the old context
  moves into the detail. It is never dropped. Maintainer review of #221 asked
  whether context had been lost, and detail was added there.
- A DynamoDB message is not reused for a condition it was not written for. When
  that cannot be avoided (for example Local answers with a 500), a `reason:`
  detail says so.

The alternatives were considered and rejected:

- **Keep `; ` as the separator and cut the normalizer at the first `;`.** This
  was the previous design. DynamoDB's own `;` made the suite skip part of its
  text (#193).
- **Return the bare DynamoDB text and drop minidyn's context.** Maintainer
  review of #221 asked whether context had been lost in a message change, and
  the rule is that it is never dropped. `docs/architecture.md` gives the
  reason: people still need a useful message when debugging their tests.

## Consequences

**Positive.** Parity tests compare DynamoDB's message exactly. The normalizer
strips from the last ` | `, so a `|` in echoed user input is kept. Messages
stay useful for debugging.

**Negative.** Many minidyn error texts changed in #193, so callers that matched
whole messages had to update (the PR's release note). A narrow `Syntax error`
cut stayed in the normalizer, tracked in #188. Every error site that knows the
path, index, attribute or type has to add detail: a bare message is a review
finding.

**Neutral.** The keys are reused (`index`, `table`, `attribute`, `field`,
`path`, `operand`, `operator`, `function`, `operand type`, `target type`,
`reason`) so that detail stays uniform.

## References

- `types/` (`WithDetail`, `Detail`), `AGENTS.md` (error message rule)
- `docs/architecture.md` (D4)
- #187, #188, #193, #221
