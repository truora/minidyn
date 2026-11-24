package server_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/logging"
	miniserver "github.com/truora/minidyn/server"
)

// The samples below are copied verbatim into README.md. Keep them in sync.

func ExampleNewServer() {
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
	// Output: 1
}

func ExampleServer_EmulateFailure() {
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
	// Output: true
}
