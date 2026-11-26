package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// A wrong-type index key value is a ValidationException of the request, except for an S, N or B
// value inside a transaction, which DynamoDB reports as the ValidationError of its item (#213).
func TestServerIndexKeyTypeMismatch(t *testing.T) {
	ctx := context.Background()
	client := NewClient()

	_, err := client.CreateTable(ctx, &CreateTableInput{
		TableName:   aws.String("ik"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		KeySchema:   []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("g"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-g"),
			KeySchema:  []ddbtypes.KeySchemaElement{{AttributeName: aws.String("g"), KeyType: ddbtypes.KeyTypeHash}},
			Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)

	item := func(g *AttributeValue) map[string]*AttributeValue {
		return map[string]*AttributeValue{"id": {S: aws.String("a")}, "g": g}
	}
	transact := func(g *AttributeValue) error {
		_, txErr := client.TransactWriteItems(ctx, &TransactWriteItemsInput{TransactItems: []TransactWriteItem{
			{Put: &Put{TableName: aws.String("ik"), Item: item(g)}},
		}})

		return txErr
	}

	number := &AttributeValue{N: aws.String("1")}
	null := &AttributeValue{NULL: aws.Bool(true)}

	_, err = client.PutItem(ctx, &PutItemInput{TableName: aws.String("ik"), Item: item(number)})
	requireServerAPIError(t, err, "ValidationException",
		"One or more parameter values were invalid: Type mismatch for Index Key | index: by-g; attribute: g; expected: S; actual: N")

	_, err = client.PutItem(ctx, &PutItemInput{TableName: aws.String("ik"), Item: item(null)})
	requireServerAPIError(t, err, "ValidationException",
		"Invalid attribute value type | index: by-g; attribute: g; expected: S; actual: NULL")

	var tce *ddbtypes.TransactionCanceledException

	require.ErrorAs(t, transact(number), &tce)
	require.Equal(t, "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]", aws.ToString(tce.Message))
	require.Len(t, tce.CancellationReasons, 1)
	require.Equal(t, "ValidationError", aws.ToString(tce.CancellationReasons[0].Code))
	require.Equal(t, "One or more parameter values were invalid: Type mismatch for Index Key", aws.ToString(tce.CancellationReasons[0].Message))

	requireServerAPIError(t, transact(null), "ValidationException",
		"Invalid attribute value type | index: by-g; attribute: g; expected: S; actual: NULL")
}
