# minidyn

Amazon DynamoDB testing library written in Go.

## Goals

* Make local testing for DynamoDB as accurate as possible.
* Run DynamoDB tests in a CI without external dependencies.
* Identify errors caused by DynamoDB restrictions.

## Installation

### As a Go module dependency

Requires **Go 1.26** or newer (see `go.mod`). From your project root:

```bash
go get github.com/truora/minidyn@latest
```

Pin a specific version or pseudo-version instead of `@latest` if your policy requires it. `go get` updates `go.mod` (and `go.sum`); run `go mod tidy` after you remove imports or upgrade other modules.

Import the packages you use, for example:

```go
import "github.com/truora/minidyn"

import miniserver "github.com/truora/minidyn/server" // optional: HTTP DynamoDB API
```

### From a clone of this repository

To work on minidyn itself, clone the repo, then build or test from the root (no separate install step):

```bash
git clone https://github.com/truora/minidyn.git
cd minidyn
go test ./...
```

## Usage

### In-memory client (existing)

Create the dynamodb client:

```go
client := minidyn.NewClient()
```

Define the tables and indexes schemas,you can use the SDKs methods to create tables.

```go
client.CreateTable(&dynamodb.CreateTableInput{
  TableName: aws.String("pokemons"),
  AttributeDefinitions: []*dynamodb.AttributeDefinition{
    {
      AttributeName: aws.String("id"),
      AttributeType: aws.String("S"),
    },
  },
  BillingMode: aws.String("PAY_PER_REQUEST"),
  KeySchema: []*dynamodb.KeySchemaElement{
    {
      AttributeName: aws.String("id"),
      KeyType:       aws.String("HASH"),
    },
  },
})
```

Or you can use the AddTable and AddIndex method helper.

```go
err := client.AddTable("pokemons", "id", "primary_type")
if err != nil {
  return err
}

err = client.AddIndex("pokemons", "type_index", "primary_type", "")
if err != nil {
  return err
}
```

**NOTE** these methods only support string attributes.

### HTTP server mode (new)

You can now run minidyn as an HTTP server compatible with the DynamoDB JSON API. This is handy for using `httptest.NewServer` and real AWS SDK clients without swapping implementations.

```go
import (
  "net/http/httptest"

  miniserver "github.com/truora/minidyn/server"
  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/config"
  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

srv := httptest.NewServer(miniserver.NewServer())
defer srv.Close()

cfg, _ := config.LoadDefaultConfig(ctx,
  config.WithEndpointResolverWithOptions(
    aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
      return aws.Endpoint{URL: srv.URL, PartitionID: "aws", SigningRegion: "us-east-1"}, nil
    })),
)
ddb := dynamodb.NewFromConfig(cfg)

// use ddb as usual: CreateTable, PutItem, Query, etc.
```

### Failure emulation

minidyn can inject DynamoDB-style failures so you can exercise your error- and
retry-handling without a real backend. There are three knobs, available both on the
in-process `aws-v2/client` (package functions) and on the HTTP `server` (methods on
`*Server`).

```go
import (
  "github.com/truora/minidyn/aws-v2/client"
  ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

c := client.NewClient()

// 1. Global failure — every operation returns the emulated error until cleared.
client.EmulateFailure(c, client.FailureConditionInternalServerError)
client.EmulateFailure(c, client.FailureConditionNone) // clear

// 2. Table/index-scoped failure — only operations touching that table (or one of
//    its indexes) fail; everything else keeps working.
client.EmulateFailureForTable(c, "pokemons", client.FailureConditionInternalServerError)
client.EmulateFailureForTable(c, "pokemons", client.FailureConditionInternalServerError, "type_index")
client.EmulateFailureForTable(c, "pokemons", client.FailureConditionNone) // clear that scope

// 3. Partial batch failure — leave selected BatchWriteItem / BatchGetItem
//    sub-requests in UnprocessedItems / UnprocessedKeys while the rest are applied.
//    The predicate receives the sub-request index within the table's slice and its
//    raw payload (a PutRequest's item, or a Delete/Get key), so you can match by
//    position, key, or any attribute.
client.EmulateUnprocessedItems(c, "pokemons", func(n int, raw map[string]ddbtypes.AttributeValue) bool {
  v, ok := raw["id"].(*ddbtypes.AttributeValueMemberS)
  return ok && v.Value == "001"
})
client.ClearUnprocessedItems(c) // clear all predicates
```

Behavior:

* A global `EmulateFailure` **and** a table-scoped `EmulateFailureForTable` **hard-fail
  the whole** `BatchWriteItem`/`BatchGetItem` call (matching DynamoDB returning a 500),
  not just the affected items.
* `EmulateUnprocessedItems` is the only way to get partial `UnprocessedItems`/
  `UnprocessedKeys`. It applies to batch operations only — single-item `PutItem`,
  `GetItem`, and `DeleteItem` are unaffected.
* Failure conditions and predicates are **sticky** until cleared
  (`FailureConditionNone`, a `nil` predicate, or `ClearUnprocessedItems`). A global
  failure overrides a table-scoped one.

The HTTP server exposes the same controls as methods on the `*Server` you pass to
`httptest.NewServer`:

```go
s := miniserver.NewServer()
ts := httptest.NewServer(s)
defer ts.Close()
// ... point an AWS SDK client at ts.URL as shown above ...

s.EmulateFailure(miniserver.FailureConditionInternalServerError)
s.EmulateFailureForTable("pokemons", miniserver.FailureConditionInternalServerError, "type_index")
s.EmulateUnprocessedItems("pokemons", func(n int, raw map[string]*miniserver.AttributeValue) bool {
  return n == 0 // leave the first sub-request of each batch on this table unprocessed
})
s.ClearUnprocessedItems()
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

When it happens you can override the intepretation using like this:

```go
client.ActivateNativeInterpreter()

client.GetNativeInterpreter().AddUpdater(table, "SET secondary_type = :secondary_type", func(item map[string]*dynamodb.AttributeValue, updates map[string]*dynamodb.AttributeValue) {
   item["secondary_type"] = updates[":secondary_type"]
})
```

**Note:** Please, report us the issue with the interpreter through https://github.com/truora/minidyn/issues

## License

The MIT License
