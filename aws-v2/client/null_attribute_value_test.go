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

// A Go caller can leave a nil AttributeValue inside an L or an M. DynamoDB Local rejects it
// with a ValidationException; it must not become a NULL or reach the interpreter (#268).
func TestRejectsNilAttributeValueInListOrMap(t *testing.T) {
	client := setupClient(tableName)
	ctx := context.Background()

	require.NoError(t, ensurePokemonTable(client))

	nilList := &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{nil}}
	nilMap := &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"k": nil}}
	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}

	const listMsg = "The supplied Item contains a null AttributeValue"

	tests := []struct {
		name string
		want string
		call func() error
	}{
		{"put attribute", listMsg + ": x is null", func() error {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item:      map[string]dynamodbtypes.AttributeValue{"id": key["id"], "x": nil},
			})

			return err
		}},
		{"put list element", listMsg, func() error {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item:      map[string]dynamodbtypes.AttributeValue{"id": key["id"], "l": nilList},
			})

			return err
		}},
		{"put map value", listMsg + ": k is null", func() error {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item:      map[string]dynamodbtypes.AttributeValue{"id": key["id"], "m": nilMap},
			})

			return err
		}},
		{"update list element", listMsg, func() error {
			_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                 aws.String(tableName),
				Key:                       key,
				UpdateExpression:          aws.String("SET l = :v"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": nilList},
			})

			return err
		}},
		{"condition map value", listMsg + ": k is null", func() error {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName:                 aws.String(tableName),
				Item:                      key,
				ConditionExpression:       aws.String("l = :v"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": nilMap},
			})

			return err
		}},
		{"transact condition check nested nil", listMsg, func() error {
			return transactConditionCheck(ctx, client, key, nilList)
		}},
		{"transact condition check top-level nil", "ExpressionAttributeValues contains invalid value: " + listMsg + " for key :v", func() error {
			return transactConditionCheck(ctx, client, key, nil)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var apiErr smithy.APIError

			err := tt.call()
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "ValidationException", apiErr.ErrorCode())
			require.Contains(t, apiErr.ErrorMessage(), tt.want)
		})
	}
}

func transactConditionCheck(ctx context.Context, client FakeClient, key map[string]dynamodbtypes.AttributeValue, v dynamodbtypes.AttributeValue) error {
	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName:                 aws.String(tableName),
			Key:                       key,
			ConditionExpression:       aws.String("x = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": v},
		}}},
	})

	return err
}
