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

`core` uses the interpreter through two methods:

- **`Table.InterpreterMatch`:** with `UseNativeInterpreter`, it tries the
  registered native matcher first. It falls back to `Language` when none is
  registered.
- **`Table.interpreterUpdate`:** it does **not** fall back. With
  `UseNativeInterpreter`, every update expression needs a registered updater.

`Native` is a hook for users to override evaluation. minidyn's own behavior
never goes through it.

## 2. Decisions

### D1. DynamoDB's documented behavior is the reference

- **Decision:** minidyn copies DynamoDB, using this order of authority:
  1. The **AWS documentation** (API reference and developer guide).
  2. **DynamoDB Local**, where the docs are silent. This is what the e2e suite
     compares against.
  3. minidyn's own choice, only when neither source decides. Comment the
     choice at the site and cover it with a unit test.

  When the docs and Local disagree, follow the docs:
  - Write a parity test that asserts both behaviors and **keeps its
    `knownIssue` marker**. The harness then checks the divergence on every run.
  - Record it under "Intentional divergences" in `e2e/README.md`, citing the
    doc.
- **Why:** users test code that will run against real DynamoDB. Local has
  quirks that real DynamoDB does not have (#153).
- **Check:** does any behavior that differs from Local cite a doc and appear
  under "Intentional divergences"?

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

### D3. Validate the request up front, once, in DynamoDB's order

- **Decision:** whatever DynamoDB can reject without reading an item is
  rejected before the first item is read. These checks fail even on an empty
  table, or when a condition would never be evaluated. They run once per
  request, not once per item. Per-item evaluation produces only errors that
  depend on the item, such as an update operand of the wrong type read from
  the item.

  On `main` these checks are up front:
  - undeclared `#names`, then undefined `:values` (#190). Across all of a
    request's expressions an undeclared name comes before an undefined value;
    an UpdateExpression comes before a ConditionExpression; and, for undefined
    values, a FilterExpression comes before a KeyConditionExpression;
  - function operand counts;
  - the shape and types of a key condition, with BETWEEN bounds checked
    first (same type, then lower <= upper, comparing by type);
  - empty S and B values in the key attributes of the table and of every
    index (#163), with LSIs reported before GSIs;
  - `ReturnValues` and `ReturnValuesOnConditionCheckFailure`, including the
    nested shapes of `TransactWriteItems`;
  - `Select`.

  The code is at `core/table.go`, in `validateSearch*`, `validateWriteExpressions`,
  `validateKeyCondition*` and `ValidateNoneOrAllOldReturnValues`, and in
  `core/select.go`. Placeholders are checked through
  `interpreter.ValidateExpressionPlaceholders`.

  For a request with several problems, the order is pinned by
  `e2e/parity_validation_order_test.go` against DynamoDB Local. For example,
  `PutItem`, `UpdateItem` and `DeleteItem` report key problems before
  placeholder problems.
- **Table and index definitions** (#200) follow Local's order too:
  - `UpdateTable` checks the GSI updates (null action, name, one update per
    index, the online-index limit, exists or missing, projection, key schema),
    then the attribute definitions, then builds the created index;
  - `CreateTable` (`Table.AddIndexes`) checks each LSI completely (name, shape,
    key attributes) before any GSI.
- **Why:** with per-item checks, the same bad request passes on an empty table
  and fails on a full one, and the error depends on the data.
- **Check:** is a new validation reachable on an empty table? Does a unit test
  pin the error order when a request has several problems?

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
  - The `errors.Is(err, interpreter.ErrSyntaxError)` checks left in aws-v2
    `UpdateItem` and in `TransactWriteItems` (both front ends) never match:
    `types.Error` does not unwrap, so the error has already been converted in
    `core`. Do not copy them. **Lands with #233:** they are removed.
  - The one live conversion in the front ends is `GetItem`'s projection. It
    calls `LangInterpreter.Project` directly (`getItemAttributesForOutput` in
    both front ends) and maps the error itself. A new read path should go
    through `core` instead.
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
- **Check:** does the diff add a new `switch x.(type)` over value types, or an
  `x.(*String)` assertion, where an interface fits?

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
  - Projection expressions are still parsed on every call
    (`Language.Project`). A change there should extend the cache rather than
    add another lexer pass.
- **Why:** tests repeat the same expressions constantly, and repeated lexing
  was the largest avoidable cost in `UpdateItem`.
- **Check:** does the diff call `language.NewLexer` or `NewParser` on a request
  path for a text the cache could hold?

### D7. One lock per client; `core` is not safe for concurrent use

- **Decision:**
  - Each front end's `Client` has one `sync.Mutex`. A public operation takes
    it once at the top and calls `core` directly. It does not call other locked
    public methods.
  - `core.Table` and the interpreter do no locking. The only exception is the
    expression cache, which has its own mutex.
- **On `main`:** `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`, `Query`,
  `Scan`, `ListTables` and `TransactWriteItems` hold the lock for the whole
  call.
- **Gap (#217):**
  - `CreateTable`, `UpdateTable`, `DeleteTable` and `DescribeTable` take no
    lock.
  - `BatchWriteItem`, `BatchGetItem` and `TransactGetItems` lock once per
    sub-request by calling the public item methods, so another call can
    interleave inside a batch.
  - `SetInterpreter` and `GetNativeInterpreter` take no lock.
- **Why:** a single lock is simple to reason about. minidyn's goal is
  correctness, not throughput.
- **Check:** does a new public method take the lock before it touches
  `tables`, and avoid calling another locked method?

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
  - `Table.UpdateTable` validates every GSI change and builds any created
    index before it changes anything, and restores the attribute definitions
    if the build fails.
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
- **Gaps:**
  - `BatchWriteItem` applies requests one at a time. It is not atomic in
    DynamoDB either, but it should validate every request before applying any
    (#216).
- **Why:** a half-applied write leaves the base table and its indexes out of
  step. Such bugs appear later as wrong query results.
- **Check:** can any new error path return after a mutation without restoring
  it? Does a test assert the table **and its indexes** after a failure?

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

### D11. Tests are the specification

- **Decision:**
  - Every behavior change starts with a failing test, in the package that owns
    the behavior (D2). A change to front-end code needs tests in both front
    ends. New DynamoDB behavior gets a parity case in `e2e/`.
  - Parity tests are never weakened. When an issue is fixed, remove its
    `knownIssue` markers, fuzzer restrictions (`e2e/fuzz_gen_test.go`) and
    `parity-surface` allowlist entries. The exception is an intentional
    divergence (D1): its marker stays.
  - Run the differential fuzzer (see `e2e/README.md`) when request handling,
    key encoding or validation order changes. Skip it after a rebase with no
    conflicts.
- The commands and the coverage gate are in `.agents/agents/qa-engineer.md`
  and `e2e/README.md`.
- **Check:** is there a test that failed before the change? Was any `e2e/`
  assertion edited?

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

### D13. Test knobs are front-end state

- **Decision:** these knobs live in each front-end `Client`, with the same
  meaning in both:
  - failure emulation (`EmulateFailure`, `EmulateFailureForTable`, the
    unprocessed-items emulation);
  - the index activation delay;
  - the native interpreter.

  `core` never knows about them.
- **Order:** in the single-item operations, an emulated failure is checked
  after `ReturnConsumedCapacity` is parsed, and before expression validation
  and the table lookup.
- **Gap:** the transaction operations check failures at different points in
  each front end. A new knob should follow the single-item order.
- **Batches:** a table-scoped failure fails a whole batch. Unprocessed-items
  emulation is the only partial path.
- **Indexes:** GSIs are written synchronously and are strongly consistent. The
  only emulated delay is `Table.IndexActivationDelay`:
  - `Description` reports a new index as `CREATING` until the delay has
    passed;
  - its data can be queried immediately.
- **Check:** is a new knob added to both front ends, at the same point in the
  request?

### D14. Generated files

- `server/requests.go` is generated by `go run ./tools/generate_requests`. Do
  not edit it by hand. Regenerate it when DynamoDB input shapes change.

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

Add or edit a decision in the same pull request as the code that motivates it.
Keep each entry in this shape:

- **Decision**, **Why** and **Check**;
- "On `main`" when the current state needs saying;
- "**Lands with #PR**" or "**Gap (#issue)**" for what is still open.

When a pull request merges or an issue closes, remove its marker and reword
the entry so that it describes `main`.
