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

// An ExclusiveStartKey does not have to be an existing item (#258).
func TestServerExclusiveStartKeyOfMissingItem(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("customer"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("customer"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeN},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	num := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: v} }

	for _, sk := range []string{"1", "10", "20"} {
		_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("orders"),
			Item: map[string]ddbtypes.AttributeValue{
				"customer": &ddbtypes.AttributeValueMemberS{Value: "alice"},
				"sk":       num(sk),
			},
		})
		require.NoError(t, err)
	}

	sks := func(items []map[string]ddbtypes.AttributeValue) []string {
		out := []string{}
		for _, it := range items {
			out = append(out, it["sk"].(*ddbtypes.AttributeValueMemberN).Value)
		}

		return out
	}

	start := map[string]ddbtypes.AttributeValue{"customer": &ddbtypes.AttributeValueMemberS{Value: "alice"}, "sk": num("5")}

	for forward, want := range map[bool][]string{true: {"10", "20"}, false: {"1"}} {
		out, qerr := cli.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String("orders"),
			KeyConditionExpression:    aws.String("customer = :c"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":c": &ddbtypes.AttributeValueMemberS{Value: "alice"}},
			ExclusiveStartKey:         start,
			ScanIndexForward:          aws.Bool(forward),
		})
		require.NoError(t, qerr)
		require.Equal(t, want, sks(out.Items))
	}

	scan, err := cli.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("orders"), ExclusiveStartKey: start})
	require.NoError(t, err)
	require.Equal(t, []string{"10", "20"}, sks(scan.Items))
}
