# minidyn

Amazon DynamoDB testing library written in Go.

## Goals

* Make local testing for DynamoDB as accurate as possible.
* Run DynamoDB tests in a CI without external dependencies.
* Identify errors caused by DynamoDB restrictions.

## Usage

Create the dynamodb client:

```go
client := minidyn.NewClient()
```

Define the tables and indexes schemas

```go
client := minidyn.NewClient()

err := client.AddTable("pokemons", "id", "ty")
if err != nil {
  return err
}

err = client.AddIndex("pokemons", "type_index", "type", "")
if err != nil {
  return err
}
```

### Define the interpretation of the unsupported expressions

```go
client.GetNativeInterpreter().AddUpdater(table, "SET secondary_type = :secondary_type", func(item map[string]*dynamodb.AttributeValue, updates map[string]*dynamodb.AttributeValue) {
   item["secondary_type"] = updates[":secondary_type"]
})
```

**Note:** It is only necessary for the expressions which do not have support in our interpreter. See language interpreter section for more information.

## Language interpreter

This library has an interpreter implementation for the DynamoDB Expressions.

### Conditional Expressions

#### Types

| Name      | Type     | Short | Supported? |
|-----------|----------|-------|-----------|
| Number    | scalar   | N     | y         |
| String    | scalar   | S     | y         |
| Binary    | scalar   | B     | y         |
| Bool      | scalar   | BOOL  | y         |
| Null      | scalar   | NULL  | y         |
| List      | document | L     | y         |
| Map       | document | M     | y         |
| StringSet | set      | SS    | y         |
| NumberSet | set      | NS    | y         |
| BinarySet | set      | BS    | y         |

#### Expressions

|                                              |                                                                                     | Supported? |
|----------------------------------------------|-------------------------------------------------------------------------------------|------------|
| operand comparator operand                   | =, <>, <, <=. > and >=                                                              | y          |
| operand BETWEEN operand AND operand          | N,S,B                                                                               | y          |
| operand IN ( operand (',' operand (, ...) )) |                                                                                     | n          |
| function                                     | attribute_exists, attribute_not_exists, attribute_type, begins_with, contains, size | y          |
| condition AND condition                      |                                                                                     | y          |
| condition OR condition                       |                                                                                     | y          |
| NOT condition                                |                                                                                     | y          |

### Update Expressions

These kinds of expressions are not supported yet.

## Missing Validations

* Validate usage of reserved words in an expression.
* Validate when an attribute is declared but not used in a Query.

## License

The MIT License
