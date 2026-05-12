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
