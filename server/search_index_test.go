package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const unknownIndexMsg = "The table does not have the specified index: nope | table: orders"

// An IndexName the table does not have is rejected before anything else of the request,
// including the pre-check of the expression attributes, as DynamoDB does.
func TestServerQueryAndScanUnknownIndex(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("orders"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("customer"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("customer"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("orders"),
		Item:      map[string]ddbtypes.AttributeValue{"customer": &ddbtypes.AttributeValueMemberS{Value: "alice"}},
	})
	require.NoError(t, err)

	alice := map[string]ddbtypes.AttributeValue{":c": &ddbtypes.AttributeValueMemberS{Value: "alice"}}

	queries := map[string]func(in *dynamodb.QueryInput){
		"Plain":          func(*dynamodb.QueryInput) {},
		"UnusedName":     func(in *dynamodb.QueryInput) { in.ExpressionAttributeNames = map[string]string{"#u": "customer"} },
		"UnusedValue":    func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues[":u"] = alice[":c"] },
		"UndeclaredName": func(in *dynamodb.QueryInput) { in.KeyConditionExpression = aws.String("#n = :c") },
		"UndefinedValue": func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues = nil },
		"InvalidSelect":  func(in *dynamodb.QueryInput) { in.Select = "BOGUS" },
		"EmptyHashValue": func(in *dynamodb.QueryInput) {
			in.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{":c": &ddbtypes.AttributeValueMemberS{Value: ""}}
		},
	}

	for name, mod := range queries {
		t.Run("Query"+name, func(t *testing.T) {
			in := &dynamodb.QueryInput{
				TableName:                 aws.String("orders"),
				IndexName:                 aws.String("nope"),
				KeyConditionExpression:    aws.String("customer = :c"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":c": alice[":c"]},
			}
			mod(in)

			_, err := cli.Query(ctx, in)
			requireValidationError(t, err, unknownIndexMsg)
		})
	}

	scans := map[string]func(in *dynamodb.ScanInput){
		"Plain":      func(*dynamodb.ScanInput) {},
		"UnusedName": func(in *dynamodb.ScanInput) { in.ExpressionAttributeNames = map[string]string{"#u": "customer"} },
		"UndeclaredName": func(in *dynamodb.ScanInput) {
			in.FilterExpression = aws.String("#n = :c")
			in.ExpressionAttributeValues = alice
		},
		"UndefinedValue": func(in *dynamodb.ScanInput) { in.FilterExpression = aws.String("customer = :c") },
		"InvalidSelect":  func(in *dynamodb.ScanInput) { in.Select = "BOGUS" },
	}

	for name, mod := range scans {
		t.Run("Scan"+name, func(t *testing.T) {
			in := &dynamodb.ScanInput{TableName: aws.String("orders"), IndexName: aws.String("nope")}
			mod(in)

			_, err := cli.Scan(ctx, in)
			requireValidationError(t, err, unknownIndexMsg)
		})
	}
}

// A missing table is reported before the pre-check of the expression attributes.
func TestServerQueryAndScanMissingTableBeforeUnusedName(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("missing"),
		KeyConditionExpression:    aws.String("customer = :c"),
		ExpressionAttributeNames:  map[string]string{"#u": "customer"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":c": &ddbtypes.AttributeValueMemberS{Value: "alice"}},
	})
	require.ErrorContains(t, err, "ResourceNotFoundException")

	_, err = cli.Scan(ctx, &dynamodb.ScanInput{
		TableName:                aws.String("missing"),
		ExpressionAttributeNames: map[string]string{"#u": "customer"},
	})
	require.ErrorContains(t, err, "ResourceNotFoundException")
}
