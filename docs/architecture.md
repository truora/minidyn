# Architecture and design decisions

This is the reference for **how minidyn is built and why**. Read it before you
implement a feature or a fix, whether you are a person or a model. It records
the decisions behind the code, so that new work fits the existing design and
does not have to rediscover it.

Other documents cover the rest:

- [`AGENTS.md`](../AGENTS.md) and [`.agents/rules/workflow.mdc`](../.agents/rules/workflow.mdc):
  the **process** (TDD, verification, commits).
- [`docs/interpreter.md`](interpreter.md): the **expression grammar**.
- [`docs/operations.md`](operations.md): **API coverage**.
- [`e2e/README.md`](../e2e/README.md): the **parity tooling** and its commands.
- [`docs/adr/`](adr/README.md): **why** each decision was taken, one record
  per decision, named by creation date.

How to read the entries:

- Each decision describes `main`.
- A sentence starting with **Lands with #N** describes an open pull request.
  Do not rely on it yet, and do not build anything that conflicts with it.
- **Gap (#N)** marks where `main` does not yet follow the decision. New code
  must not widen the gap.
- Each decision ends with a **Check** you can run against a diff.

## 1. Layers and request flow

```
AWS SDK v2 call                         HTTP request (DynamoDB JSON API)
      │                                            │
aws-v2/client  (in-memory client)          server  (httptest-friendly server)
      │  SDK types → types.*                       │  generated requests.go → types.*
      └────────────────────┬───────────────────────┘
                           │  per-client mutex (see D7 for which calls hold it)
                       core.Table   (storage, keys, indexes, validation, writes)
                           │
                     interpreter    (Language: Match, Update, Project;
                           │         Native: Match, Update; parse cache;
                           │         request-level expression validators)
                interpreter/language (lexer, parser, AST, evaluator, objects)

capacity  (SDK-free consumed-capacity math, shared by both front ends)
types     (internal item form types.Item, errors, types.WithDetail)
```

| Package | Owns | Must not |
|---|---|---|
| `aws-v2/client`, `server` | <ul><li>Mapping SDK/JSON shapes to `types.*` and back</li><li>The pre-check of placeholder syntax and unused names/values (`validateExpressionAttributes`)</li><li>Test knobs (D13)</li><li>`ReturnConsumedCapacity` (D12)</li><li>Mapping `types.Error` to SDK errors (`mapKnownError`)</li></ul> | Hold behavior that the other front end also needs. Put that in `core`. |
| `core` | Tables, key schema, indexes, item storage, request validation, conditional writes, `ReturnValues`, snapshots | Import an AWS SDK package |
| `interpreter` | The `Language` interpreter and the user-override `Native` one, the condition parse cache, request-level expression validators, key condition analysis | Know about tables or storage. `MatchInput.TableName` and `UpdateInput.TableName` exist only as the lookup key for native matchers. |
| `interpreter/language` | Lexing, parsing, the AST, evaluation, the object model, built-in functions | Return Go `error`s from evaluation. It returns `*language.Error` objects instead. |
| `capacity` | Capacity-unit math from item sizes | Depend on the SDK |
| `types` | `types.Item`, `types.Error`, `types.WithDetail`/`types.Detail`, `types.ValidateItemMap` | — |

**Supported API.** The supported public API is the two front ends: users import
`aws-v2/client` or `server`. `core`, `interpreter`, `capacity`, `types`,
`calculate-size` and every other package that is not a front end are not a
supported public API: their exported identifiers exist so the front ends can use
them, and they may change without notice, in any release. A change to them is
not a breaking change in the sense of the ADR rule. See
[`docs/adr/2026-11-23-treat-core-and-internal-packages-as-unsupported-api.md`](adr/2026-11-23-treat-core-and-internal-packages-as-unsupported-api.md).

`core` uses the interpreter through two methods:

- **`Table.InterpreterMatch`:** with `UseNativeInterpreter`, it tries the
  registered native matcher first. It falls back to `Language` when none is
  registered.
- **`Table.interpreterUpdate`:** it does **not** fall back. With
  `UseNativeInterpreter`, every update expression needs a registered updater.

`Native` is a hook for users to override evaluation. minidyn's own behavior
never goes through it.

The grammar accepts only an integer literal as a list index, as DynamoDB does
(D1): [`2026-11-22-reject-placeholder-list-indexes-like-dynamodb.md`](adr/2026-11-22-reject-placeholder-list-indexes-like-dynamodb.md).

ADRs for the layers:

- [`2021-08-02-interpret-dynamodb-expressions-with-a-hand-written-lexer-parser-and-evaluator.md`](adr/2021-08-02-interpret-dynamodb-expressions-with-a-hand-written-lexer-parser-and-evaluator.md)
- [`2022-09-23-keep-core-and-interpreter-free-of-aws-sdk-types.md`](adr/2022-09-23-keep-core-and-interpreter-free-of-aws-sdk-types.md)
- [`2025-12-22-expose-minidyn-as-an-http-server-for-the-dynamodb-json-api.md`](adr/2025-12-22-expose-minidyn-as-an-http-server-for-the-dynamodb-json-api.md)
- [`2026-05-06-support-only-aws-sdk-go-v2.md`](adr/2026-05-06-support-only-aws-sdk-go-v2.md)

## 2. Decisions

### D1. DynamoDB's documented behavior is the reference

- **Decision:** minidyn copies DynamoDB, using this order of authority:
  1. The **AWS documentation** (API reference and developer guide).
  2. **DynamoDB Local**, where the docs are silent. This is what the e2e suite
     compares against.
  3. minidyn's own choice, only when neither source decides. Comment the
     choice at the site and cover it with a unit test.

  When the docs and Local disagree, follow the docs:
  - Write a parity test that asserts each engine's behavior separately, so
    the harness checks the divergence on every run. Such a test needs no
    `knownIssue` marker. #153 is the one entry that still carries one.
  - Record it under "Intentional divergences" in `e2e/README.md`, citing the
    doc.
- **Why:** users test code that will run against real DynamoDB. Local has
  quirks that real DynamoDB does not have (#153).
- **Check:** does any behavior that differs from Local cite a doc and appear
  under "Intentional divergences"?
- **ADR:** [`2026-11-07-follow-the-aws-docs-before-dynamodb-local.md`](adr/2026-11-07-follow-the-aws-docs-before-dynamodb-local.md)

### D2. Behavior lives in `core`; front ends stay thin and symmetric

- **Decision:** anything both front ends need goes in `core` (or `capacity`).
  - A front-end-only change is allowed only for SDK or JSON mapping. It covers
    the existing `validateExpressionAttributes` pre-check, which runs before
    `core` in both.
  - A change in one front end needs the same change, and the same test, in the
    other.
  - A new request-level check belongs in `core`, unless it is about the shape
    of the SDK or JSON request.
- **Why:** `server` and `aws-v2/client` each keep their own request
  validation and error mapping, and they have drifted apart before.
- **Check:** does the diff touch `server/client.go` or `aws-v2/client/client.go`
  without touching both? Are there tests in both `server` and `aws-v2/client`?
- **ADR:** [`2025-12-22-put-shared-behavior-in-core-behind-two-symmetric-front-ends.md`](adr/2025-12-22-put-shared-behavior-in-core-behind-two-symmetric-front-ends.md)

### D3. Validate the request up front, once, in DynamoDB's order

- **Decision:** whatever DynamoDB can reject without reading an item is
  rejected before the first item is read. These checks fail even on an empty
  table, or when a condition would never be evaluated. They run once per
  request, not once per item. Per-item evaluation produces only errors that
  depend on the item, such as an update operand of the wrong type read from
  the item.

  On `main` these checks are up front:
  - the `IndexName` of a Query or Scan (#241): after the table itself and
    before the placeholders, the key condition, `Select` and
    `ConsistentRead`, and ahead of the front ends' pre-check of the
    expression attributes (`Table.ValidateSearchIndex`). Local checks the
    Query `Limit` (#242) before the index;
  - `ConsistentRead: true` on a GSI of a Query or Scan (#247), which Local
    rejects: a Scan right after the unknown index, ahead of its placeholders;
    a Query after its placeholders and before the projection, key condition,
    `Select` and start key (`Table.ValidateConsistentRead`);
  - the N values DynamoDB cannot store (#226, #273): not a number, out of range, or
    over 38 digits, in any item, key, set member or expression value
    (`types.ValidateItemMap`, `types.ValidateExpressionAttributeValues`, which
    use `types.ParseNumber`). They come before the other problems of the item or
    key. For UpdateItem and DeleteItem the shape and type of the key come before
    the numbers of the expression values. A transaction checks every operation
    first (`core.ValidateWriteValues`), and a batch write every request
    (`core.ValidateBatchWrite`). See
    [`docs/adr/2026-11-22-validate-number-values-on-every-write.md`](adr/2026-11-22-validate-number-values-on-every-write.md);
  - a list index that is not a literal, such as `l[:i]`, one with a leading
    zero, or one above 2147483647 (#164): DynamoDB reports it while it checks
    the expression, before any placeholder, so it comes ahead of the
    undeclared names, the undefined values and the front ends' pre-check of
    unused names and values (`interpreter.HasInvalidListIndex`). A syntax error
    elsewhere in the same text comes before the range error. See
    [`docs/adr/2026-11-22-reject-placeholder-list-indexes-like-dynamodb.md`](adr/2026-11-22-reject-placeholder-list-indexes-like-dynamodb.md);
  - undeclared `#names`, then undefined `:values` (#190). Across all of a
    request's expressions an undeclared name comes before an undefined value;
    an UpdateExpression comes before a ConditionExpression; and, for undefined
    values, a FilterExpression comes before a KeyConditionExpression;
  - function operand counts;
  - the `ProjectionExpression` of a `GetItem` (its syntax, then undeclared
    `#names`), before the key (`Table.GetItem`); a `BatchGetItem` is the
    opposite: every key of a table, then the projection, once per table
    (`Table.BatchGetItem`);
  - the keys of a `BatchGetItem` (#246), before the tables, the keys' values
    or any projection (`core.ValidateBatchGetKeys`): no tables, a table with
    no keys, then each key's numbers (an invalid one is reported before the
    key is compared) and the key against the keys before it, as the request
    spells them (so `1` and `1.0` differ; `rawKeyID` builds a length-prefixed
    identity of the key map, so the scan is linear), then the 100 key count.
    The front ends run it in the order of `BatchWriteItem` (D13):
    `ReturnConsumedCapacity`, the emulated failure, then these checks. The
    count stays with the duplicate check rather than with the front ends'
    shape checks, because Local reports a duplicate ahead of the limit. An
    error names the table and the position of the repeated key (`index: 3`),
    never its value. Local goes through the tables in the order of the wire
    request, which the SDK marshals from a map, so it is random; minidyn goes
    by table name, in the duplicate scan and in the per-table reads, so the
    error is the same on every run (D1 level 3). The parity cases that mix
    failing tables compare only that the batch is rejected;
  - the requests of a `BatchWriteItem` (`core.ValidateBatchWrite`): no
    tables, a table with no requests, then, per table in request order, each
    request validated and compared by its stored key with the requests
    before it (#235, a put and a delete included). The error names the table
    and the position of the repeated request (`request: 3`);
  - the shape and types of a key condition, with BETWEEN bounds checked
    first (same type, then lower <= upper, comparing by type);
  - the `ExclusiveStartKey` of a Query or Scan (`Table.validateStartKey`, #171,
    [ADR](adr/2026-11-21-reject-invalid-exclusive-start-keys.md)), right after the
    key condition and before the index checks below and `Select`: a number it
    cannot store, then its size, each key attribute (present and of the declared
    type, the table's first), empty values and, for a Query, the key condition.
    It need not name an existing item (#258) and may sit on the inclusive bound
    the walk ends at, which Local rejects but minidyn hands out (#153). An empty map
    counts as none in `core/table.go` (#277);
  - the `ProjectionExpression` of a Query or Scan (its syntax, then undeclared
    `#names`), before the key condition, and then the attributes of an index
    Query's `FilterExpression`, which must all be projected by the index
    (`Table.validateFilterProjected`), after the key condition and before
    `Select`; then a GSI Query or Scan's `ProjectionExpression`, which must
    only name attributes the GSI projects (`Table.validateIndexProjection`);
  - empty S and B values in the key attributes of the table and of every
    index (#163), with LSIs reported before GSIs;
  - `ReturnValues` and `ReturnValuesOnConditionCheckFailure`, including the
    nested shapes of `TransactWriteItems`;
  - `Select`;
  - the `Segment` and `TotalSegments` of a parallel Scan (#155; a Query ignores
    them): after the start key's own checks and `Select`, but before the projection of a GSI. Each needs the other, then `Segment` must lie in
    `[0, TotalSegments-1]`, then `TotalSegments` is at most 1000000
    (`Table.validateSegment`, `core/segment.go`). An item's segment is a hash of
    the table's encoded partition key, so a partition never splits and an
    `ExclusiveStartKey` of another segment is rejected. Local's assignment is not
    reproducible (D1, level 3): the parity cases compare the union of the segments.

  The code is at `core/table.go`, in `validateSearch*`, `validateWriteExpressions`,
  `validateKeyCondition*` and `ValidateNoneOrAllOldReturnValues`, and in
  `core/select.go` and `core/segment.go`. Placeholders are checked through
  `interpreter.ValidateExpressionPlaceholders`.

  For a request with several problems, the order is pinned by
  `e2e/parity_validation_order_test.go` against DynamoDB Local. For example,
  `PutItem`, `UpdateItem` and `DeleteItem` report key problems before
  placeholder problems.
- **Table and index definitions** (#200) follow Local's order too:
  - `UpdateTable` (#199) first checks that there is something to update, then
    the billing mode enum, then the GSI updates (null action, name, one update
    per index, the online-index limit, exists or missing, projection, key
    schema, the capacity of an index update), then the capacity of the table
    (at least 1, different from the current one, present when going back to
    provisioned), then the on-demand limits (`OnDemandThroughput`), then the
    attribute definitions, then builds the created index. A problem with an index change is reported ahead of one with the
    table settings, as Local does;
  - `CreateTable` (`Table.AddIndexes`) checks each LSI completely (name, shape,
    key attributes) before any GSI.
- **Gap (#243):** the front ends call `Table.ValidateSearchIndex` before their
  own pre-check of unused names and values, because Local reports a missing
  table and an unknown index before them. For a Scan they also call
  `Table.ValidateConsistentRead` before that pre-check, for the same reason:
  Local reports ConsistentRead on a GSI right after the unknown index. This is a stopgap until #243 moves
  that pre-check into `core`, where `SearchData` already orders it. The same
  pre-check carries the front ends' skips for a list index DynamoDB rejects
  (`interpreter.HasInvalidListIndex`) and for an undefined value
  (`hasUndefinedValue`), so #243 removes them all together.
- **Why:** with per-item checks, the same bad request passes on an empty table
  and fails on a full one, and the error depends on the data.
- **Check:** is a new validation reachable on an empty table? Does a unit test
  pin the error order when a request has several problems?
- **ADR:** [`2026-06-22-validate-a-request-once-before-reading-any-item.md`](adr/2026-06-22-validate-a-request-once-before-reading-any-item.md)

### D4. Errors: DynamoDB's exact text, then minidyn detail after ` | `

- **Decision:**
  - The message starts with DynamoDB's exact text, including any `;` part
    DynamoDB returns.
  - Whatever minidyn knows that DynamoDB's text does not say goes after
    ` | ` as `key: value` pairs. Build it with `types.WithDetail(msg, k, v, ...)`,
    or `types.Detail(...)` when wrapping an error. Examples of such context: the
    attribute or path, the index, the table, the operator or function, the
    operand position, the actual type.
  - Reuse the existing keys: `index`, `table`, `attribute`, `field`, `path`,
    `operand`, `operator`, `function`, `operand type`, `target type`, `reason`.
  - When you change a minidyn-only message to DynamoDB's text, **keep the old
    context** by moving it into the detail. Never drop it.
  - Do not reuse a DynamoDB message for a condition it was not written for. If
    you must (for example, Local answers with a 500), add a `reason:` detail.
- **Mechanics on `main`:**
  - `core` returns `types.NewError(code, message, nil)`. It also wraps
    interpreter errors from conditions, updates and `SearchData` into
    `ValidationException`.
  - Front ends must not check `errors.Is(err, interpreter.ErrSyntaxError)`
    on errors from `core`: `types.Error` does not unwrap, so such a check
    never matches. The last ones were removed in #233, and
    `core/update_errors_test.go` pins that `Update` errors arrive as
    `ValidationException`.
  - `GetItem` goes through `core` too (`Table.GetItem`): it checks the
    projection, then the key, and returns the error as a `types.Error`.
    `BatchGetItem` goes through `Table.BatchGetItem` once per table, which
    checks every key, then the projection. The front ends have no conversion
    of their own left. A transactional `ConditionCheck` reads its item with
    `Table.GetItem`, and the keys of a `TransactGetItems` go through
    `Table.CheckedKeyOf`, so they use the same key check.
  - `ResourceNotFoundException` comes from `core` only for a missing index.
    The front ends build the SDK error for a missing table themselves.
  - `ErrUnsupportedFeature` is what `Native` returns when nothing is
    registered, which drives the `Match` fallback. `Language` returns it for an
    attribute type it cannot evaluate.
  - Each front end's `mapKnownError` (`server/client.go`,
    `aws-v2/client/mapper.go`) turns a `types.Error` into the SDK error.
  - Build a failed condition with `core.ConditionalCheckFailed`. With
    `ReturnValuesOnConditionCheckFailure=ALL_OLD` it attaches a copy of the
    existing item, and nothing when the item is missing. `Put`, `Update`,
    `Delete` and transactional `ConditionCheck` all use it (#157).
  - The parity normalizer strips everything from the last ` | `, so detail
    never affects parity.
- **Why:** the parity tests can compare messages exactly, and people still
  get a useful message when debugging their tests.
- **Check:** is any new or changed error a bare DynamoDB message where the code
  at that site knows the path, index, attribute or type?
- **ADR:** [`2026-11-06-append-minidyn-detail-to-dynamodb-errors-after-a-pipe.md`](adr/2026-11-06-append-minidyn-detail-to-dynamodb-errors-after-a-pipe.md)

### D5. Interpreter objects: capabilities through interfaces, missing values are `UNDEFINED`

- **Decision:**
  - Values are `language.Object`s.
  - What an object can do is expressed as an interface it implements (in
    `interpreter/language/object.go`):
    - `ContainerObject` (`contains`)
    - `SizableObject` (`size`)
    - `PrefixObject` (`begins_with`)
    - `AppendableObject` (`ADD`)
    - `DetachableObject` (`DELETE`)
  - A new built-in or operator dispatches through such an interface. Add a new
    interface when none fits; do not use a type switch on `*String`, `*Binary`
    and so on.
  - A path that does not resolve evaluates to `UNDEFINED`.
  - In conditions, functions and comparisons return `false` (or no size) when
    they receive a missing value, or a value of the wrong type read from the
    item. They return an error only when the type is known from the expression
    alone, such as a `:value` of the wrong type (#143, since #221).
  - Evaluation returns `*language.Error` objects, never panics, and never uses
    Go errors.
- **Why:** an interface keeps each type's behavior next to the type, and adding
  a type stays a local change. The `UNDEFINED` and `false` rules are how
  DynamoDB behaves on real data. The interface call costs nothing measurable
  next to the rest of an evaluation, so speed is not a reason to skip it.
- **Note:** a concrete assertion is fine where that type is the only one
  possible, such as arithmetic on `*Number`, `list_append` on `*List`, or
  `indexAccessor.Get`/`Set` on `*Map`/`*List`.
- **Numbers are decimal text, never `float64`:** `*Number` holds the N text in
  the form DynamoDB stores (`types.NormalizeNumber`: no leading or trailing
  zeros, no exponent), and `*NumberSet` keys its members the same way. Equality
  is text equality; ordering and arithmetic go through `types.Decimal`, which
  is exact up to 38 significant digits and fails with DynamoDB's "only supports
  precision up to 38 digits" beyond. Every item read into the interpreter is
  written back (an update rewrites the whole item), so a `float64` anywhere on
  that path rounds values it never meant to change (#259).
- **Check:** does the diff add a new `switch x.(type)` over value types, or an
  `x.(*String)` assertion, where an interface fits? Does a number go through
  `float64`?
- **ADR:** [`2026-11-07-dispatch-interpreter-values-through-capability-interfaces.md`](adr/2026-11-07-dispatch-interpreter-values-through-capability-interfaces.md)

### D6. Parse and scan each expression text once

- **Decision:**
  - Anything derived only from the expression text belongs in the bounded,
    process-wide cache keyed by that text: the parse, placeholder lists,
    operand-count errors, key condition analysis. It does not belong in a fresh
    lexer pass per request or per item.
  - The AST is never changed by evaluation, so cached entries are shared.
  - Do not lex the same expression twice in one request. Do not repeat in an
    inner layer a check an outer layer has already made.
- **On `main`:**
  - `parseCondition` and `maxParsedExpressions` in
    `interpreter/language_cache.go` cache condition, filter and key-condition
    texts. `Match`, `ValidateFunctionOperandCounts` and the key condition code
    use them.
  - `exprPlaceholders` caches the `#names` and `:values` of any expression
    text, update and projection expressions included, scanned in one lexer
    pass. Every placeholder validator reads it (#207).
  - `parseUpdate` caches update expressions (#165). `Language.Update`, the
    primary-key target check and the `UPDATED_*` target paths
    (`interpreter.UpdateTargetPaths`) all read the same parse.
  - `parseProjection` caches projection expressions. `interpreter.CheckProjection`
    validates one and returns the parse, which `Language.ProjectChecked` uses,
    so `Table.GetItem` and `Table.BatchGetItem` check a projection once.
- **Why:** tests repeat the same expressions constantly, and repeated lexing
  was the largest avoidable cost in `UpdateItem`.
- **Check:** does the diff call `language.NewLexer` or `NewParser` on a request
  path for a text the cache could hold?
- **ADR:** [`2026-11-02-parse-each-expression-text-once-in-a-shared-cache.md`](adr/2026-11-02-parse-each-expression-text-once-in-a-shared-cache.md)

### D7. One lock per client; `core` is not safe for concurrent use

- **Decision:**
  - Each front end's `Client` has one `sync.Mutex`. A public operation takes
    it once at the top and calls `core` directly. It does not call other locked
    public methods.
  - `core.Table` and the interpreter do no locking. The exceptions are
    `interpreter/language_cache.go`, whose four expression caches
    (placeholders, conditions, updates, projections) each have their own
    mutex, and `interpreter.Native`, whose matcher and updater maps are
    guarded by its own `RWMutex` (#250). Every table of a client holds the
    client's `*Native` and `*Language` by pointer, so `AddMatcher`,
    `AddUpdater` (callable on the object `GetNativeInterpreter` returns) and
    `ActivateDebug` reach existing and later tables, and registration is safe
    while operations run (an operation may still see the old or the new
    matcher). Matchers and updaters run outside that mutex, under the client
    lock. See [`2026-11-22-share-interpreter-settings-by-pointer-and-lock-native-registration.md`](adr/2026-11-22-share-interpreter-settings-by-pointer-and-lock-native-registration.md).
- **On `main`:**
  - Every public operation of both front ends takes the lock once at the top
    and holds it for the whole call: the table operations (`CreateTable`,
    `UpdateTable`, `DeleteTable`, `DescribeTable`, `ListTables`), the item
    operations, the batches (`BatchWriteItem`, `BatchGetItem`), the
    transactions, `ClearTable`, `ClearAllTables`, `Reset` and the test knobs
    (failure emulation, the interpreter and index-activation settings). In
    `aws-v2` the knobs also include `SetInterpreter` and `GetNativeInterpreter`;
    `server` has no such methods.
  - A batch or transaction does not call another public method. It calls
    unexported helpers that assume the lock is held: `putItemLocked`,
    `deleteItemLocked` and `getItemLocked`, `batchEmulationLocked`,
    `batchGetTable`, `validateBatchWriteRequests` and the `runTransact*`
    helpers. Name such a helper with a `Locked` suffix, or say in its comment
    that the caller must hold the lock.
  - So a batch or transaction is not interleaved with any other call, and its
    unprocessed-item matchers run with the lock held (a matcher must not call
    the client). The same holds for native-interpreter matchers and updaters
    (`AddMatcher`, `AddUpdater`) and for a custom interpreter set with
    `SetInterpreter`: they run under the lock too, so they must not call the
    client either.
- **Why:** a single lock is simple to reason about. minidyn's goal is
  correctness, not throughput.
- **Check:** does a new public method take the lock before it touches
  `tables`, and avoid calling another locked method?
- **ADR:** [`2026-11-11-hold-one-client-lock-per-public-operation.md`](adr/2026-11-11-hold-one-client-lock-per-public-operation.md)

### D8. A request applies completely or not at all

- **Decision:**
  - Validate everything before changing anything.
  - Where a failure can only be found mid-write, for example an index
    rejecting the item after the item was stored, restore the previous state so
    the table and every index match their state before the request.
- **On `main`, item writes:** `Put` and `Update` compute every index key
  (`indexKeysOf`) before storing anything, so an index that rejects the item
  leaves the table and its indexes untouched. `Update` restores the item it
  changed in place when evaluation or index validation fails (#163).
- **On `main`, table definitions:**
  - `Table.UpdateTable` validates every GSI change and the table settings
    (billing mode, capacity, on-demand limits, deletion protection) and builds
    any created index before it changes anything, and restores the attribute
    definitions and the settings if the build fails.
  - `CreateTable` registers the table only after `Table.AddIndexes` has built
    every index, so a rejected request leaves no table behind.
- **On `main`, transactions:**
  - `prepareTransact` (in both front ends) snapshots every table a
    `TransactWriteItems` touches with `Table.Snapshot`.
  - The items are applied in order.
  - The first failure restores every snapshot and becomes the
    `TransactionCanceledException` reason at that position.
  - This is table-level, not item-level, isolation (see `docs/operations.md`).
  - A snapshot costs time in proportion to the table size (see
    `benchmarks/README.md`), so do not add more callers.
- **On `main`, batches:**
  - `BatchWriteItem` validates every request (key shape and values, index
    keys, table existence) before it applies any, so an invalid request
    rejects the whole batch. The orchestration is `core.ValidateBatchWrite`,
    built on `Table.CheckedKeyOf` and the put check that returns the storage
    key (`validatedPutKey`), the same steps Put and Delete run. The front
    ends only map shapes. minidyn reports a missing table only when every
    request of the existing tables is valid, and between two invalid tables
    the first by name, because the request is a map and has no order. Both
    are minidyn's choices (D1 level 3): DynamoDB Local's order between a
    missing table and an invalid request depends on the wire order and the
    table schema, so the parity test compares only that the batch is
    rejected and nothing is written.
  - The validation and the writes run under one lock acquisition (D7), so
    the guarantee also holds against another call that changes a table while
    the batch runs.
  - The requests are then applied one at a time. DynamoDB does not make a
    batch atomic either.
- **Why:** a half-applied write leaves the base table and its indexes out of
  step. Such bugs appear later as wrong query results.
- **Check:** can any new error path return after a mutation without restoring
  it? Does a test assert the table **and its indexes** after a failure?
- **ADR:** [`2026-11-07-apply-a-write-completely-or-not-at-all.md`](adr/2026-11-07-apply-a-write-completely-or-not-at-all.md)

### D9. Storage and keys

- **Decision:**
  - Items live in `Table.Data` under an encoded primary key, and
    `Table.SortedKeys` orders them.
  - Each index keeps references (`refs`, `sortedRefs`) from its own key to
    primary keys, and applies its projection when read (`index.ProjectItem`).
  - Stored keys are **opaque**: compare and build them only through the key
    schema (`keySchema.GetKey`), never by parsing or formatting strings.
- **On `main`:** keys use a typed, order-preserving encoding
  (`core/key_encoding.go`, #147, #148, #149), and two different keys never
  encode to the same value:
  - N sorts numerically. Equal spellings (`1`, `1.0`, `01`, `1e0`) encode to
    the same key.
  - B and S sort by their bytes.
  - A partition's prefix never matches another partition.
  - Range queries, `begins_with` and `ExclusiveStartKey` paging work on the
    encoding.
  - An N value DynamoDB cannot store fails with DynamoDB's own text.
- **Why:** keys must sort the way DynamoDB sorts them, and two different keys
  must never collide.
- **Check:** does new code split, parse or concatenate key strings instead of
  going through the key schema?
- **ADR:** [`2026-11-07-encode-stored-keys-by-type-as-opaque-strings.md`](adr/2026-11-07-encode-stored-keys-by-type-as-opaque-strings.md)

### D10. Reuse before adding; measure before optimizing

- **Decision:**
  - Before writing a helper, look for an existing one. Examples:
    - `ExtractPath` for document paths;
    - `types.WithDetail` for error detail;
    - `types.ValidateItemMap` for items;
    - `keySchema.validatePrimaryKeyMap` for key maps;
    - `index.ProjectItem` for projections;
    - the `capacity` functions.
  - Do not duplicate a loop or a validator with only its message changed.
    Share the logic and let each caller build its own message.
  - Prefer a plain predicate to a callback. A validator that takes its error
    text as a function reads across two functions (#252).
  - Prefer separate entry points to a flag parameter that changes the order of
    checks: `Table.GetItem` and `Table.BatchGetItem` each have a fixed order
    (#245).
  - Put code in the file that owns its concept, not in a new file per issue
    or fix. Key validation belongs in `core/key_schema.go`, table and index
    checks in `core/table.go` or `core/index.go`, and expression behavior in
    the interpreter file it extends. Create a file only for a new concept
    that stands on its own, such as `core/select.go` for the `Select`
    parameter or `core/key_encoding.go` for the key encoding. Test files can
    be grouped by behavior.
  - A performance argument needs a benchmark in `benchmarks/`, with ns/op,
    B/op and allocs/op taken before and after on an otherwise idle machine.
- **Check:** before adding a helper, grep for the DynamoDB message and for the
  path or key operation. If one exists, call it. Does the diff add a
  non-test file named after a fix instead of a concept?
- **ADR:** [`2026-11-08-reuse-existing-code-and-put-it-in-the-file-that-owns-the-concept.md`](adr/2026-11-08-reuse-existing-code-and-put-it-in-the-file-that-owns-the-concept.md)

### D11. Tests are the specification

- **Decision:**
  - Every behavior change starts with a failing test, in the package that owns
    the behavior (D2). A change to front-end code needs tests in both front
    ends. New DynamoDB behavior gets a parity case in `e2e/`.
  - Parity tests are never weakened. When an issue is fixed, remove its
    `knownIssue` markers, fuzzer restrictions (`e2e/fuzz_gen_test.go`) and
    `parity-surface` allowlist entries. The exception is an intentional
    divergence (D1): its test asserts each engine, and the one marker
    that is kept (#153) stays.
  - Run the differential fuzzer (see `e2e/README.md`) when request handling,
    key encoding or validation order changes. Skip it after a rebase with no
    conflicts.
- The commands and the coverage gate are in `.agents/agents/qa-engineer.md`
  and `e2e/README.md`.
- **Check:** is there a test that failed before the change? Was any `e2e/`
  assertion edited?
- **ADR:** [`2026-11-02-make-parity-tests-the-specification-with-known-issue-markers.md`](adr/2026-11-02-make-parity-tests-the-specification-with-known-issue-markers.md)

### D12. `ReturnValues` in `core`, consumed capacity in the front ends

- **Decision:**
  - `core` validates the `ReturnValues` enums and computes the attributes to
    return: `ALL_OLD` for `Put`/`Delete`, `*_OLD`/`*_NEW` for `Update`, and the
    failed-condition item.
  - Consumed capacity is a front-end concern:
    - every operation parses the mode (`capacity.ParseMode`) before it
      calls `core`;
    - it measures item sizes before and after the write through `capacity`
      (`server/capacity.go`, `aws-v2/client/capacity.go`);
    - it never changes what `core` returns.
- **Check:** does capacity logic appear in `core`? Does a `ReturnValues` rule
  appear in a front end?
- **ADR:** [`2026-06-19-compute-return-values-in-core-and-consumed-capacity-in-the-front-ends.md`](adr/2026-06-19-compute-return-values-in-core-and-consumed-capacity-in-the-front-ends.md)

### D13. Test knobs are front-end state

- **Decision:** these knobs live in each front-end `Client`, with the same
  meaning in both:
  - failure emulation (`EmulateFailure`, `EmulateFailureForTable`, the
    unprocessed-items emulation);
  - the index activation delay;
  - the native interpreter.

  `core` carries no failure-injection logic. Two knobs are stored on
  `core.Table` because the table needs them: `UseNativeInterpreter` (matching
  and updating) and `IndexActivationDelay` (`Description`). The front ends set
  both; `core` never reads client state.
- **Order:** in the single-item operations, an emulated failure is checked
  after `ReturnConsumedCapacity` is parsed, and before expression validation
  and the table lookup.
- **Batches and transactions (#275):** these check, in order, the input shape,
  `ReturnConsumedCapacity`, the emulated failure, then each request or item.
  `TransactWriteItems` has no structural shape check yet (#291).
  `ReturnValuesOnConditionCheckFailure` is an item-level check, so it comes
  after the failure, as in the single-item operations. A table-scoped failure
  fails the whole call when any table it touches is the scoped one.
  Unprocessed-items emulation is the only partial path.
- **Indexes:** GSIs are written synchronously and are strongly consistent. The
  only emulated delay is `Table.IndexActivationDelay`:
  - `Description` reports a new index as `CREATING` until the delay has
    passed;
  - its data can be queried immediately.
- **Check:** is a new knob added to both front ends, at the same point in the
  request?
- **ADR:** [`2026-06-18-keep-test-knobs-in-the-front-end-clients.md`](adr/2026-06-18-keep-test-knobs-in-the-front-end-clients.md)

### D14. Generated files

- `server/requests.go` is generated by `go run ./tools/generate_requests`. Do
  not edit it by hand. Regenerate it when DynamoDB input shapes change.
- CI runs the generator and fails when `git diff --exit-code server/requests.go`
  shows a change (#254), so a stale file cannot merge.
- **ADR:** [`2025-12-22-generate-the-server-request-types-from-the-sdk.md`](adr/2025-12-22-generate-the-server-request-types-from-the-sdk.md)

## 3. Checklist for a fix or feature

1. Find the behavior in the AWS docs, then in DynamoDB Local (D1). If they
   differ, plan the divergence entry.
2. Decide the layer (section 1, D2, D12, D13).
3. Decide which checks belong to the request rather than to an item, and the
   order they run in (D3).
4. Write failing tests:
   - unit tests in the package that owns the behavior;
   - both front ends if their code changes;
   - an e2e parity case for new behavior.

   For a fixed issue, remove its `knownIssue` markers, fuzzer restrictions and
   `parity-surface` allowlist entries (D11).
5. Implement:
   - error text and detail (D4);
   - interfaces, not type switches (D5);
   - no repeated parsing (D6);
   - hold the lock and call `core` directly (D7);
   - complete-or-nothing writes (D8);
   - keys through the key schema (D9);
   - reuse existing helpers (D10).
6. Verify with qa-engineer, and run the fuzzer when D11 says so. File an issue
   for any new divergence you find but do not fix.
7. If you changed a decision, update this document.

## 4. Changing this document

Add or edit a decision in the same pull request as the code that motivates it,
together with an ADR in [`docs/adr/`](adr/README.md) that records the context
and the rejected alternatives. Link the ADR from the entry. Keep each entry in
this shape:

- **Decision**, **Why** and **Check**;
- **ADR**, linking the record of the decision;
- "On `main`" when the current state needs saying;
- "**Lands with #PR**" or "**Gap (#issue)**" for what is still open.

When a pull request merges or an issue closes, remove its marker and reword
the entry so that it describes `main`.
