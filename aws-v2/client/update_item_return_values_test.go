package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An update that creates the item has no old item: ALL_OLD returns no attributes (#214).
func TestUpdateItemAllOldOnMissingItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	update := func(name string) *dynamodb.UpdateItemOutput {
		out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"id": &dynamodbtypes.AttributeValueMemberS{Value: "025"},
			},
			ReturnValues:             dynamodbtypes.ReturnValueAllOld,
			UpdateExpression:         aws.String("SET #n = :n"),
			ExpressionAttributeNames: map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":n": &dynamodbtypes.AttributeValueMemberS{Value: name},
			},
		})
		c.NoError(err)

		return out
	}

	c.Empty(update("Pichu").Attributes)

	c.Equal(map[string]dynamodbtypes.AttributeValue{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: "025"},
		"name": &dynamodbtypes.AttributeValueMemberS{Value: "Pichu"},
	}, update("Pikachu").Attributes)
}

// An invalid ReturnValues gets the text PutItem and DeleteItem use, and DynamoDB reports it
// before the key and the expressions.
func TestUpdateItemInvalidReturnValues(t *testing.T) {
	client := setupClient(tableName)
	require.NoError(t, ensurePokemonTable(client))

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	tests := map[string]*dynamodb.UpdateItemInput{
		"valid key": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("x")},
		},
		"empty key": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("")}, UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("x")},
		},
		"undefined value": {Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET n = :v")},
		"undeclared name": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET #n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("x")},
		},
		"syntax error": {Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET")},
		"invalid RVOCCF": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("")}, UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues:           map[string]dynamodbtypes.AttributeValue{":v": str("x")},
			ReturnValuesOnConditionCheckFailure: "BOGUS",
		},
	}

	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			in.TableName, in.ReturnValues = aws.String(tableName), "BOGUS"

			_, err := client.UpdateItem(context.Background(), in)

			require.ErrorContains(t, err, "ValidationException: Return values set to invalid value")
			require.NotContains(t, err.Error(), "validation error detected")
		})
	}
}

// An unknown table is reported before an invalid ReturnValues.
func TestUpdateItemUnknownTableBeforeInvalidReturnValues(t *testing.T) {
	client := setupClient(tableName)

	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String("no-such-table"),
		Key:              map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "025"}},
		UpdateExpression: aws.String("SET n = :v"),
		ReturnValues:     "BOGUS",
	})

	require.ErrorContains(t, err, "ResourceNotFoundException")
}
