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
