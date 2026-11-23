# minidyn

Amazon DynamoDB testing library written in Go.

## Goals

* Make local testing for DynamoDB as accurate as possible.
* Run DynamoDB tests in a CI without external dependencies.
* Identify errors caused by DynamoDB restrictions.

## Installation

### As a Go module dependency

Requires **Go 1.27** or newer (see `go.mod`). From your project root:

```bash
go get github.com/truora/minidyn@latest
```

Pin a specific version or pseudo-version instead of `@latest` if your policy requires it. `go get` updates `go.mod` (and `go.sum`); run `go mod tidy` after you remove imports or upgrade other modules.

Import the packages you use, for example:

```go
import "github.com/truora/minidyn/aws-v2/client" // in-memory AWS SDK v2 client

import miniserver "github.com/truora/minidyn/server" // optional: HTTP DynamoDB API
```

The supported packages are `aws-v2/client` and `server`. `core`, `interpreter`, `capacity`, `types`, `calculate-size` and the other packages are internal to minidyn: they are not a supported API and their exported identifiers may change without notice.

### From a clone of this repository

To work on minidyn itself, clone the repo, then build or test from the root (no separate install step):

```bash
git clone https://github.com/truora/minidyn.git
cd minidyn
go test ./...
```

## Usage

### In-memory client

minidyn supports only the AWS SDK for Go v2. `client.NewClient()` returns a fake that implements the DynamoDB operations of that SDK (`CreateTable`, `PutItem`, `GetItem`, `Query`, ...) with the SDK's own input and output types, so you can use it wherever your code accepts a DynamoDB API interface. The samples use these import aliases: `ddbtypes` is `github.com/aws/aws-sdk-go-v2/service/dynamodb/types`, `miniserver` is `github.com/truora/minidyn/server`, `types` is `github.com/truora/minidyn/types`, `interpreter` is `github.com/truora/minidyn/interpreter`, and `config`, `credentials` and `logging` are `github.com/aws/aws-sdk-go-v2/config`, `github.com/aws/aws-sdk-go-v2/credentials` and `github.com/aws/smithy-go/logging`. The imports of each sample are in the two example files.

The code samples in this README are compiled and run by `go test`: they come from the `Example` functions in [`aws-v2/client/example_test.go`](aws-v2/client/example_test.go) and [`server/example_test.go`](server/example_test.go), so they cannot drift from the API (`TestReadmeSamplesMatchExamples` also fails if the README text differs from the examples).

Create the client, define the table and indexes schemas with the SDK's `CreateTable`, then write and read items:

```go
ctx := context.Background()
c := client.NewClient()

_, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{
	TableName: aws.String("pokemons"),
	AttributeDefinitions: []ddbtypes.AttributeDefinition{
		{
			AttributeName: aws.String("id"),
			AttributeType: ddbtypes.ScalarAttributeTypeS,
		},
	},
	BillingMode: ddbtypes.BillingModePayPerRequest,
	KeySchema: []ddbtypes.KeySchemaElement{
		{
			AttributeName: aws.String("id"),
			KeyType:       ddbtypes.KeyTypeHash,
		},
	},
})
if err != nil {
	panic(err)
}

_, err = c.PutItem(ctx, &dynamodb.PutItemInput{
	TableName: aws.String("pokemons"),
	Item: map[string]ddbtypes.AttributeValue{
		"id":   &ddbtypes.AttributeValueMemberS{Value: "001"},
		"name": &ddbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
	},
})
if err != nil {
	panic(err)
}

out, err := c.GetItem(ctx, &dynamodb.GetItemInput{
	TableName: aws.String("pokemons"),
	Key: map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "001"},
	},
})
if err != nil {
	panic(err)
}

fmt.Println(out.Item["name"].(*ddbtypes.AttributeValueMemberS).Value)
```

Or you can use the `AddTable` and `AddIndex` helpers.

```go
ctx := context.Background()
c := client.NewClient()

err := client.AddTable(ctx, c, "pokemons", "id", "primary_type")
if err != nil {
	panic(err)
}

err = client.AddIndex(ctx, c, "pokemons", "type_index", "primary_type", "")
if err != nil {
	panic(err)
}

out, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("pokemons")})
if err != nil {
	panic(err)
}

fmt.Println(*out.Table.GlobalSecondaryIndexes[0].IndexName)
```

**NOTE** these helpers only support string attributes.

### HTTP server mode

You can run minidyn as an HTTP server compatible with the DynamoDB JSON API. This is handy for using `httptest.NewServer` and real AWS SDK clients without swapping implementations.

```go
ctx := context.Background()

srv := httptest.NewServer(miniserver.NewServer())
defer srv.Close()

cfg, err := config.LoadDefaultConfig(ctx,
	config.WithRegion("us-east-1"),
	config.WithLogger(logging.Nop{}),
	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
	panic(err)
}

ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
	o.BaseEndpoint = aws.String(srv.URL)
})

// use ddb as usual: CreateTable, PutItem, Query, etc.
_, err = ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
	TableName: aws.String("pokemons"),
	AttributeDefinitions: []ddbtypes.AttributeDefinition{
		{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
	},
	BillingMode: ddbtypes.BillingModePayPerRequest,
	KeySchema: []ddbtypes.KeySchemaElement{
		{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
	},
})
if err != nil {
	panic(err)
}

_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
	TableName: aws.String("pokemons"),
	Item: map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "001"},
	},
})
if err != nil {
	panic(err)
}

out, err := ddb.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("pokemons")})
if err != nil {
	panic(err)
}

fmt.Println(out.Count)
```

### Failure emulation

minidyn can inject DynamoDB-style failures so you can exercise your error- and
retry-handling without a real backend. There are three knobs, available both on the
in-process `aws-v2/client` (package functions) and on the HTTP `server` (methods on
`*Server`).

```go
ctx := context.Background()
c := client.NewClient()

if err := client.AddTable(ctx, c, "pokemons", "id", ""); err != nil {
	panic(err)
}

item := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}}
put := &dynamodb.PutItemInput{TableName: aws.String("pokemons"), Item: item}

// Global failure: every item operation (PutItem, GetItem, UpdateItem,
// DeleteItem, Query, Scan, the batches and the transactions) returns the
// emulated error until cleared.
client.EmulateFailure(c, client.FailureConditionInternalServerError)

_, err := c.PutItem(ctx, put)

var internal *ddbtypes.InternalServerError

fmt.Println(errors.As(err, &internal))

client.EmulateFailure(c, client.FailureConditionNone) // clear

_, err = c.PutItem(ctx, put)
fmt.Println(err)
```

```go
ctx := context.Background()
c := client.NewClient()

for _, table := range []string{"pokemons", "trainers"} {
	if err := client.AddTable(ctx, c, table, "id", "primary_type"); err != nil {
		panic(err)
	}
}

if err := client.AddIndex(ctx, c, "pokemons", "type_index", "primary_type", ""); err != nil {
	panic(err)
}

// Table/index-scoped failure: only operations touching that table (or one of
// its indexes) fail; everything else keeps working.
client.EmulateFailureForTable(c, "pokemons", client.FailureConditionInternalServerError)
client.EmulateFailureForTable(c, "pokemons", client.FailureConditionInternalServerError, "type_index") // only that index

item := map[string]ddbtypes.AttributeValue{
	"id":           &ddbtypes.AttributeValueMemberS{Value: "001"},
	"primary_type": &ddbtypes.AttributeValueMemberS{Value: "grass"},
}

_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pokemons"), Item: item})
fmt.Println(err != nil)

_, err = c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("trainers"), Item: item})
fmt.Println(err)

client.EmulateFailureForTable(c, "pokemons", client.FailureConditionNone) // clear that scope
```

```go
ctx := context.Background()
c := client.NewClient()

if err := client.AddTable(ctx, c, "pokemons", "id", ""); err != nil {
	panic(err)
}

// Partial batch failure: leave selected BatchWriteItem / BatchGetItem
// sub-requests in UnprocessedItems / UnprocessedKeys while the rest are applied.
client.EmulateUnprocessedItems(c, "pokemons", func(n int, raw map[string]ddbtypes.AttributeValue) bool {
	v, ok := raw["id"].(*ddbtypes.AttributeValueMemberS)
	return ok && v.Value == "001"
})

var requests []ddbtypes.WriteRequest

for _, id := range []string{"001", "002"} {
	requests = append(requests, ddbtypes.WriteRequest{
		PutRequest: &ddbtypes.PutRequest{
			Item: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: id}},
		},
	})
}

out, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
	RequestItems: map[string][]ddbtypes.WriteRequest{"pokemons": requests},
})
if err != nil {
	panic(err)
}

fmt.Println(len(out.UnprocessedItems["pokemons"]))

client.ClearUnprocessedItems(c) // clear all predicates
```

Behavior:

* A global `EmulateFailure` **and** a table-scoped `EmulateFailureForTable` **hard-fail
  the whole** `BatchWriteItem`/`BatchGetItem` call (matching DynamoDB returning a 500),
  not just the affected items.
* `EmulateUnprocessedItems` is the only way to get partial `UnprocessedItems`/
  `UnprocessedKeys`. It applies to batch operations only — single-item `PutItem`,
  `GetItem`, and `DeleteItem` are unaffected.
* A predicate runs while the client lock is held, so it **must not call the client**
  (or the `*Server`); doing so deadlocks.
* Failure conditions and predicates are **sticky** until cleared
  (`FailureConditionNone`, a `nil` predicate, or `ClearUnprocessedItems`). A global
  failure overrides a table-scoped one.

The HTTP server exposes the same controls as methods on the `*Server` you pass to
`httptest.NewServer`:

```go
ctx := context.Background()

s := miniserver.NewServer()
ts := httptest.NewServer(s)
defer ts.Close()

// point an AWS SDK client at ts.URL as shown above
cfg, err := config.LoadDefaultConfig(ctx,
	config.WithRegion("us-east-1"),
	config.WithLogger(logging.Nop{}),
	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	// The default retryer would retry the 500 with backoff; fail on the first one.
	config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
)
if err != nil {
	panic(err)
}

ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
	o.BaseEndpoint = aws.String(ts.URL)
})

_, err = ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
	TableName: aws.String("pokemons"),
	AttributeDefinitions: []ddbtypes.AttributeDefinition{
		{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
	},
	BillingMode: ddbtypes.BillingModePayPerRequest,
	KeySchema: []ddbtypes.KeySchemaElement{
		{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
	},
})
if err != nil {
	panic(err)
}

// The same knobs as the in-memory client: s.EmulateFailureForTable(table,
// condition, indexes...), s.EmulateUnprocessedItems(table, predicate) and
// s.ClearUnprocessedItems().
s.EmulateFailure(miniserver.FailureConditionInternalServerError)

_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
	TableName: aws.String("pokemons"),
	Item: map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "001"},
	},
})

var internal *ddbtypes.InternalServerError

fmt.Println(errors.As(err, &internal))

s.EmulateFailure(miniserver.FailureConditionNone) // clear
```

## Supported Operations and Features

For a detailed list of supported DynamoDB operations, types, and expressions, please refer to the documentation:

* [Supported Operations](docs/operations.md)
* [Language Interpreter Support](docs/interpreter.md)

## Developer notes

### Regenerating HTTP request structs

The HTTP server uses generated JSON input shapes in `server/requests.go` so we can cleanly unmarshal DynamoDB JSON without the SDK’s `AttributeValue` interfaces. If you update DynamoDB inputs or need to refresh these shapes, run:

```bash
go run ./tools/generate_requests
```

This will rewrite `server/requests.go` based on the AWS SDK v2 DynamoDB input types, replacing `AttributeValue` interfaces with the concrete JSON-friendly `AttributeValue` defined in `server/types.go`.

### E2E tests (minidyn vs DynamoDB Local)

The `e2e` package runs the same AWS SDK v2 calls against **minidyn** (`httptest` + `server.NewServer`) and **DynamoDB Local** in Docker ([testcontainers-go](https://github.com/testcontainers/testcontainers-go)), then compares results.

**Prerequisites:** a working Docker engine (`docker info` must succeed), since DynamoDB Local is started as `amazon/dynamodb-local`.

From the repository root:

```bash
go test ./e2e/... -v
```

If Docker is unavailable, those tests **skip** after `docker info` fails (so the comparison against DynamoDB Local is not run).

#### Install and run `amazon/dynamodb-local` yourself

To run the same [official image](https://hub.docker.com/r/amazon/dynamodb-local) outside of the Go tests (for manual checks or any AWS SDK client):

```bash
docker pull amazon/dynamodb-local:latest
docker run --rm -p 8000:8000 amazon/dynamodb-local:latest
```

DynamoDB Local listens on port **8000**. Point your client at `http://localhost:8000` with a real region (for example `us-east-1`) and any credentials; signing still applies, but the local server does not validate them.

### What to do when the interpreter does not work properly?

When it happens you can override the interpretation of an expression with the native interpreter. Register an updater (or a matcher for key, filter and conditional expressions) for a table and expression text; the functions receive minidyn's `types.Item` values (`github.com/truora/minidyn/types`), not the SDK's `AttributeValue`:

```go
ctx := context.Background()
c := client.NewClient()

if err := client.AddTable(ctx, c, "pokemons", "id", ""); err != nil {
	panic(err)
}

c.ActivateNativeInterpreter()

c.GetNativeInterpreter().AddUpdater("pokemons", "SET secondary_type = :secondary_type", func(item map[string]*types.Item, updates map[string]*types.Item) {
	item["secondary_type"] = updates[":secondary_type"]
})

// Filters and conditions are overridden the same way, by expression type.
c.GetNativeInterpreter().AddMatcher("pokemons", interpreter.ExpressionTypeFilter, "secondary_type = :secondary_type", func(item map[string]*types.Item, values map[string]*types.Item) bool {
	return *item["secondary_type"].S == *values[":secondary_type"].S
})

key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}}

if _, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pokemons"), Item: key}); err != nil {
	panic(err)
}

_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
	TableName:        aws.String("pokemons"),
	Key:              key,
	UpdateExpression: aws.String("SET secondary_type = :secondary_type"),
	ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
		":secondary_type": &ddbtypes.AttributeValueMemberS{Value: "poison"},
	},
})
if err != nil {
	panic(err)
}

out, err := c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("pokemons"), Key: key})
if err != nil {
	panic(err)
}

fmt.Println(out.Item["secondary_type"].(*ddbtypes.AttributeValueMemberS).Value)

scan, err := c.Scan(ctx, &dynamodb.ScanInput{
	TableName:        aws.String("pokemons"),
	FilterExpression: aws.String("secondary_type = :secondary_type"),
	ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
		":secondary_type": &ddbtypes.AttributeValueMemberS{Value: "poison"},
	},
})
if err != nil {
	panic(err)
}

fmt.Println(scan.Count)
```

**Note:** Please, report us the issue with the interpreter through https://github.com/truora/minidyn/issues

## License

The MIT License
