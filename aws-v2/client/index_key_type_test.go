package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// A wrong-type index key value is a ValidationException of the request, except for an S, N or B
// value inside a transaction, which DynamoDB reports as the ValidationError of its item (#213).
func TestIndexKeyTypeMismatch(t *testing.T) {
	ctx := context.Background()
	client := NewClient()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("ik"),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema:   []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("g"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-g"),
			KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("g"), KeyType: dynamodbtypes.KeyTypeHash}},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)

	item := func(g dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
		return map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "a"}, "g": g}
	}
	transact := func(g dynamodbtypes.AttributeValue) error {
		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: aws.String("ik"), Item: item(g)}},
		}})

		return err
	}

	number := &dynamodbtypes.AttributeValueMemberN{Value: "1"}
	null := &dynamodbtypes.AttributeValueMemberNULL{Value: true}

	t.Run("PutItem", func(t *testing.T) {
		for _, tt := range []struct {
			g       dynamodbtypes.AttributeValue
			message string
		}{
			{number, "One or more parameter values were invalid: Type mismatch for Index Key"},
			{null, "Invalid attribute value type"},
		} {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("ik"), Item: item(tt.g)})

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "ValidationException", apiErr.ErrorCode())
			require.Contains(t, apiErr.ErrorMessage(), tt.message)
		}
	})

	t.Run("TransactWriteItems number", func(t *testing.T) {
		var tce *dynamodbtypes.TransactionCanceledException

		require.ErrorAs(t, transact(number), &tce)
		require.Equal(t, "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]", aws.ToString(tce.Message))
		require.Len(t, tce.CancellationReasons, 1)
		require.Equal(t, "ValidationError", aws.ToString(tce.CancellationReasons[0].Code))
		require.Equal(t, "One or more parameter values were invalid: Type mismatch for Index Key", aws.ToString(tce.CancellationReasons[0].Message))
	})

	t.Run("TransactWriteItems null", func(t *testing.T) {
		var apiErr smithy.APIError

		require.ErrorAs(t, transact(null), &apiErr)
		require.Equal(t, "ValidationException", apiErr.ErrorCode())
		require.Contains(t, apiErr.ErrorMessage(), "Invalid attribute value type")
	})
}
