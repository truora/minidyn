# Supported Operations

Minidyn aims to accurately mock DynamoDB behavior for local testing. However, it does not support the entire DynamoDB API. The following operations are currently supported by both the in-memory client and the HTTP server mode (unless otherwise specified):

- `BatchGetItem`
- `BatchWriteItem`
- `CreateTable`
- `DeleteItem`
- `DeleteTable`
- `DescribeTable`
- `GetItem`
- `ListTables`
- `PutItem`
- `Query`
- `Scan`
- `TransactGetItems`
- `TransactWriteItems`
- `UpdateItem`
- `UpdateTable`

## Partially Supported Features

- **[TransactWriteItems](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html)**: Transactions are supported, but minidyn handles rollbacks using **table-level snapshots** instead of item-level locks and snapshots like real DynamoDB. In a highly concurrent environment, this could cause full table rollbacks where real DynamoDB would only lock and rollback specific items.
- **[Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html)**: Condition Expressions, Update Expressions, and Projection Expressions are largely supported through the internal interpreter, but some complex nested functions or specific clauses may have edge case differences compared to real DynamoDB.
- **[Secondary Indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html)**: Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI) creation, querying, and scanning are supported. Index projections (`ALL`, `KEYS_ONLY`, `INCLUDE`) are applied when returning items from a secondary index `Query` / `Scan`; optional `ProjectionExpression` is evaluated against that projected attribute set (matching DynamoDB). However, the following real DynamoDB features are **not** currently simulated:
  - **Eventual Consistency**: Global Secondary Indexes are updated synchronously and are always strongly consistent in minidyn. Real DynamoDB updates GSIs asynchronously (eventually consistent).
  - **Throughput/Limits**: Minidyn does not enforce index-specific read/write capacity limits.
- **Limits and Restrictions**: Real DynamoDB limits (such as 400KB item sizes, 1MB limits per Query/Scan, or max limits for pagination) are not enforced in minidyn. Queries and Scans will return all matching items unless explicitly limited.
- **ReturnConsumedCapacity**: Supported for all operations that report it (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems`). `NONE` (or unset) omits `ConsumedCapacity`; any other value outside `INDEXES`, `TOTAL` and `NONE` (the enum is case sensitive, so `total` is invalid) is rejected with DynamoDB's `ValidationException` before the operation runs, although DynamoDB Local silently accepts it. `TOTAL` returns table-level totals; `INDEXES` additionally returns a per-table/index breakdown. Under `INDEXES`, the `Table` sub-object is always reported (matching DynamoDB) — including for an index `Query`/`Scan`, where the base table is charged zero units and the read is attributed to the index. Capacity units are derived from the documented DynamoDB item-size algorithm (4&nbsp;KB per read unit, 1&nbsp;KB per write unit, eventually consistent reads halved, transactional operations doubled). Because that algorithm is the contract, the exact unit values may differ slightly from a given DynamoDB Local build's rounding.
  - **Writes to a table with secondary indexes** (`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem`, `TransactWriteItems`) report the base table **plus every affected index**, at both `TOTAL` (in the total) and `INDEXES` (also per index). The base table is billed by the larger of the item before and after the write, and a delete by the item it removes (sizes are measured before the write is applied). An index is affected when the item is, or becomes, indexed there: entering or leaving a sparse index writes one index entry, changing the index key deletes the old entry and writes the new one (two entries), and rewriting an entry bills one.
  - **Batch and transactional operations** return one `ConsumedCapacity` entry per table, ordered by table name.
  - **Known approximations**: `Query`/`Scan` bill the items they return, so a `FilterExpression` or `ProjectionExpression` reports less than DynamoDB, which bills the items read; and a transactional `ConditionCheck` is billed by its key, whereas DynamoDB Local bills the checked item.

---

# Not Supported Operations

Operations related to administrative, backup, streaming, and global table features are generally not supported. Some common unsupported operations include:

- **Item Operations**:
  - `ExecuteStatement`, `BatchExecuteStatement` (PartiQL)

- **Table & Tagging Operations**:
  - `DescribeEndpoints`
  - `DescribeLimits`
  - `DescribeTimeToLive`, `UpdateTimeToLive`
  - `ListTagsOfResource`, `TagResource`, `UntagResource`

- **Backup & Restore**:
  - `CreateBackup`, `DeleteBackup`, `DescribeBackup`, `ListBackups`, `RestoreTableFromBackup`
  - `DescribeContinuousBackups`, `UpdateContinuousBackups`, `RestoreTableToPointInTime`

- **Global Tables**:
  - `CreateGlobalTable`, `DescribeGlobalTable`, `UpdateGlobalTable`

- **Streams**:
  - `DescribeStream`, `GetRecords`, `GetShardIterator`

If you need support for an operation not listed here, please consider contributing to the project or opening an issue on GitHub.
