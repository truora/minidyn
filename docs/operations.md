# Supported Operations

Minidyn aims to accurately mock DynamoDB behavior for local testing. However, it does not support the entire DynamoDB API. The following operations are currently supported by both the in-memory client and the HTTP server mode (unless otherwise specified):

- `BatchGetItem`
- `BatchWriteItem`
- `CreateTable`
- `DeleteItem`
- `DeleteTable`
- `DescribeTable`
- `GetItem`
- `PutItem`
- `Query`
- `Scan`
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
- **ReturnConsumedCapacity**: Operations in minidyn do not accurately calculate or return the consumed capacity units. The `ReturnConsumedCapacity` parameter is largely ignored, and mock/empty capacity reports are returned or omitted entirely.

---

# Not Supported Operations

Operations related to administrative, backup, streaming, and global table features are generally not supported. Some common unsupported operations include:

- **Item Operations**:
  - `TransactGetItems`
  - `ExecuteStatement`, `BatchExecuteStatement` (PartiQL)

- **Table & Tagging Operations**:
  - `ListTables`
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
