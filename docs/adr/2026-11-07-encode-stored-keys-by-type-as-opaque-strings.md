# Encode stored keys by type, as opaque order-preserving strings

**Status:** Accepted

## Context

Backfilled on 2026-11-21 from `fe4cc7c` (#219, issues #147, #148 and #149).
Stored table and index keys were the hash and range values joined with `.` as
text. That caused three bugs found by the parity suite:

- Numeric range keys sorted as strings (#147).
- Binary range keys sorted by their decimal text (#148).
- A `.` inside a key value made different items collide, so a `PutItem`
  silently replaced a different item, and equal numbers with different
  spellings (`7`, `7.0`) were different keys (#149, marked "data loss").

Facts on this branch:

- `wc -l core/key_encoding.go` prints `114`, and
  `grep -c '^func Test\|^func Fuzz' core/key_encoding_test.go` prints `12`.
- `git grep -n 'strings.Join' -- core/key_schema.go core/index.go | wc -l` prints
  `0`: nothing joins key parts with `.` any more.

## Decision

Keys stay `string`s, but become an order-preserving, self-delimiting encoding
of the typed values (`core/key_encoding.go`), so plain string comparison orders
them like DynamoDB:

- **S and B:** the bytes with `0x00` escaped, then a terminator.
- **N:** a sign byte, a decimal exponent, the significant digits and a
  terminator, with negatives inverted. It is exact for 38 digits and never goes
  through `float64`. Equal values encode identically (`7`, `7.0`, `07`, `7e0`).
- **Composite:** the hash encoding followed by the range encoding. Every part
  ends with a terminator that cannot appear inside a value, so no two key pairs
  collide and no encoded hash is a prefix of another.

Stored keys are opaque. Code compares and builds them only through the key
schema (`keySchema.GetKey`), never by parsing or formatting strings. An N value
DynamoDB cannot store fails with DynamoDB's own text, also in an
`ExclusiveStartKey`.

The alternatives were considered and rejected. Both were recorded in the issues:

- **Sort by the typed key and keep a joined string.** Issue #147 suggested:
  "Compare by hash key first, then by range key with a type-aware comparison:
  numbers by value (for example `big.Float` or `apd.Decimal`...), strings and
  binary by bytes", with the index search and `GetKeyAt` using the same
  comparison.
- **A struct key (`{hash, range}` with typed values), or length-prefixed
  parts.** Issue #149: "Don't use a joined string as the map key. Use a struct
  key ... or an encoding that can't be ambiguous (length-prefixed parts)."

#219 does not say why it chose an encoded string over those two. The visible
gain is that `Table.Data`, `SortedKeys`, start keys, `LastEvaluatedKey` and batch
duplicate checks keep working on plain strings, unchanged.

## Consequences

**Positive.** Query, Scan and `Limit` paging return numeric and binary range keys
in DynamoDB's order. Keys never collide, and `partitionPrefix` returns exactly
the encoded hash, so the old exception for numeric partition keys is gone.

**Negative.** `Table.Data` and `SortedKeys` are no longer human-readable (an
internal API change in #219's release note). Items that used to overwrite each
other because of `.` now coexist, and equal numbers with different spellings now
overwrite each other. A number DynamoDB cannot store now fails with a
`ValidationException` where it was accepted. Any code that parses a key string
is wrong.

**Neutral.** The encoding is internal; it is not part of the public API.

## References

- `core/key_encoding.go`, `core/key_schema.go`, `core/index.go`
- `docs/architecture.md` (D9)
- #147, #148, #149, #219
