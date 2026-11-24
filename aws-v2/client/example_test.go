package client_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/aws-v2/client"
	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// The samples below are copied verbatim into README.md. Keep them in sync.

func ExampleNewClient() {
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
	// Output: Bulbasaur
}

func ExampleAddTable() {
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
	// Output: type_index
}

func ExampleClient_GetNativeInterpreter() {
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
	// Output:
	// poison
	// 1
}

func ExampleEmulateFailure() {
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
	// Output:
	// true
	// <nil>
}

func ExampleEmulateFailureForTable() {
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
	// Output:
	// true
	// <nil>
}

func ExampleEmulateUnprocessedItems() {
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
	// Output: 1
}
