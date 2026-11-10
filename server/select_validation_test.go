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

func TestServerQueryAndScanSelectValidation(t *testing.T) {
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

	tests := []struct {
		name       string
		sel        ddbtypes.Select
		projection string
		wantErr    string
	}{
		{name: "SpecificWithoutProjection", sel: "SPECIFIC_ATTRIBUTES", wantErr: "Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"},
		{name: "AllProjectedOnTable", sel: "ALL_PROJECTED_ATTRIBUTES", wantErr: "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"},
		{name: "CountWithProjection", sel: "COUNT", projection: "customer", wantErr: "Cannot specify the ProjectionExpression when choosing to get only the Count"},
		{name: "AllAttributesWithProjection", sel: "ALL_ATTRIBUTES", projection: "customer", wantErr: "Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"},
		{name: "Unknown", sel: "BOGUS", wantErr: "Select must be ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT, or SPECIFIC_ATTRIBUTES"},
		{name: "SpecificWithProjection", sel: "SPECIFIC_ATTRIBUTES", projection: "customer"},
	}

	for _, tt := range tests {
		t.Run("Query"+tt.name, func(t *testing.T) {
			in := &dynamodb.QueryInput{
				TableName:              aws.String("orders"),
				Select:                 tt.sel,
				KeyConditionExpression: aws.String("customer = :c"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":c": &ddbtypes.AttributeValueMemberS{Value: "alice"},
				},
			}

			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			_, err := cli.Query(ctx, in)
			assertServerSelectResult(t, err, tt.wantErr)
		})

		t.Run("Scan"+tt.name, func(t *testing.T) {
			in := &dynamodb.ScanInput{TableName: aws.String("orders"), Select: tt.sel}

			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			_, err := cli.Scan(ctx, in)
			assertServerSelectResult(t, err, tt.wantErr)
		})
	}
}

// A key condition error comes before an invalid Select, as in DynamoDB.
func TestServerQueryKeyConditionErrorBeforeInvalidSelect(t *testing.T) {
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

	str := func(s string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: s} }
	num := func(s string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: s} }

	tests := []struct {
		name    string
		value   ddbtypes.AttributeValue
		wantErr string
	}{
		{name: "EmptyHash", value: str(""), wantErr: "empty string value"},
		{name: "TypeMismatch", value: num("1"), wantErr: "Condition parameter type does not match schema type"},
		{name: "ValidKeyReportsSelect", value: str("ana"), wantErr: "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cli.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String("orders"),
				Select:                    "ALL_PROJECTED_ATTRIBUTES",
				KeyConditionExpression:    aws.String("customer = :c"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":c": tt.value},
			})

			assertServerSelectResult(t, err, tt.wantErr)
		})
	}
}

func assertServerSelectResult(t *testing.T, err error, wantErr string) {
	t.Helper()

	if wantErr == "" {
		require.NoError(t, err)

		return
	}

	require.Error(t, err)
	require.Contains(t, err.Error(), "ValidationException")
	require.Contains(t, err.Error(), wantErr)
}
