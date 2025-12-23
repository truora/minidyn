# minidyn

Amazon DynamoDB testing library written in Go.

## Goals

* Make local testing for DynamoDB as accurate as possible.
* Run DynamoDB tests in a CI without external dependencies.
* Identify errors caused by DynamoDB restrictions.

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

Supported operations in HTTP mode include CreateTable, DescribeTable, UpdateTable, DeleteTable, PutItem, GetItem, UpdateItem, DeleteItem, Query, Scan, and BatchWriteItem.

## Language interpreter

This library has an interpreter implementation for the DynamoDB Expressions.

### Conditional Expressions

#### Types

| Name      | Type     | Short | Supported? |
| --------- | -------- | ----- | ---------- |
| Number    | scalar   | N     | y          |
| String    | scalar   | S     | y          |
| Binary    | scalar   | B     | y          |
| Bool      | scalar   | BOOL  | y          |
| Null      | scalar   | NULL  | y          |
| List      | document | L     | y          |
| Map       | document | M     | y          |
| StringSet | set      | SS    | y          |
| NumberSet | set      | NS    | y          |
| BinarySet | set      | BS    | y          |

#### Expressions

|                                              | Syntax                                                                              | Supported? |
| -------------------------------------------- | ----------------------------------------------------------------------------------- | ---------- |
| operand comparator operand                   | = <> < <= > and >=                                                                  | y          |
| operand BETWEEN operand AND operand          | N,S,B                                                                               | y          |
| operand IN ( operand (',' operand (, ...) )) |                                                                                     | y          |
| function                                     | attribute_exists, attribute_not_exists, attribute_type, begins_with, contains, size | y          |
| condition AND condition                      |                                                                                     | y          |
| condition OR condition                       |                                                                                     | y          |
| NOT condition                                |                                                                                     | y          |

### Update Expressions

#### Expressions

|          | Syntax                       | Supported? |
| -------- | ---------------------------- | ---------- |
| SET      | SET action [, action] ...    | y          |
| REMOVE   | REMOVE action [, action] ... | y          |
| ADD      | ADD action [, action] ...    | y          |
| DELETE   | DELETE action [, action] ... | y          |
| function | list_append, if_not_exists   | y          |

## Developer notes

### Regenerating HTTP request structs

The HTTP server uses generated JSON input shapes in `server/requests.go` so we can cleanly unmarshal DynamoDB JSON without the SDK’s `AttributeValue` interfaces. If you update DynamoDB inputs or need to refresh these shapes, run:

```bash
go run ./tools/generate_requests
```

This will rewrite `server/requests.go` based on the AWS SDK v2 DynamoDB input types, replacing `AttributeValue` interfaces with the concrete JSON-friendly `AttributeValue` defined in `server/types.go`.

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
