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

// ConsistentRead true on a GSI is rejected for a Query and a Scan, and the flag reaches core
// from the front end. A Scan reports it ahead of its unused names, a Query after them.
func TestServerQueryAndScanConsistentReadOnGSI(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	str := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
	hash := func(name string) ddbtypes.KeySchemaElement {
		return ddbtypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: ddbtypes.KeyTypeHash}
	}
	attr := func(name string) ddbtypes.AttributeDefinition {
		return ddbtypes.AttributeDefinition{AttributeName: aws.String(name), AttributeType: ddbtypes.ScalarAttributeTypeS}
	}

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("orders"),
		KeySchema:            []ddbtypes.KeySchemaElement{hash("customer")},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{attr("customer"), attr("status")},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-status"),
			KeySchema:  []ddbtypes.KeySchemaElement{hash("status")},
			Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)

	_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("orders"),
		Item:      map[string]ddbtypes.AttributeValue{"customer": str("alice"), "status": str("open")},
	})
	require.NoError(t, err)

	const want = "Consistent reads are not supported on global secondary indexes | index: by-status"

	query := func(index string, consistent *bool) *dynamodb.QueryInput {
		in := &dynamodb.QueryInput{
			TableName:                 aws.String("orders"),
			KeyConditionExpression:    aws.String("customer = :v"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": str("alice")},
			ConsistentRead:            consistent,
		}

		if index != "" {
			in.IndexName = aws.String(index)
			in.KeyConditionExpression = aws.String("#s = :v")
			in.ExpressionAttributeNames = map[string]string{"#s": "status"}
			in.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{":v": str("open")}
		}

		return in
	}

	_, err = cli.Query(ctx, query("by-status", aws.Bool(true)))
	requireValidationError(t, err, want)

	_, err = cli.Query(ctx, query("by-status", aws.Bool(false)))
	require.NoError(t, err)

	_, err = cli.Query(ctx, query("by-status", nil))
	require.NoError(t, err)

	_, err = cli.Query(ctx, query("", aws.Bool(true)))
	require.NoError(t, err)

	_, err = cli.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("orders"), IndexName: aws.String("by-status"), ConsistentRead: aws.Bool(true)})
	requireValidationError(t, err, want)

	_, err = cli.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("orders"), IndexName: aws.String("by-status"), ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]string{"#u": "status"},
	})
	requireValidationError(t, err, want)

	_, err = cli.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("orders"), IndexName: aws.String("by-status"), ConsistentRead: aws.Bool(false)})
	require.NoError(t, err)

	_, err = cli.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("orders"), ConsistentRead: aws.Bool(true)})
	require.NoError(t, err)
}
