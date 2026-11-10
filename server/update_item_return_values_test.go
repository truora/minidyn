package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An update that creates the item has no old item: ALL_OLD returns no attributes (#214).
func TestServerUpdateItemAllOldOnMissingItem(t *testing.T) {
	c := require.New(t)
	srv := NewServer()
	ctx := context.Background()

	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	update := func(name string) *UpdateItemOutput {
		out, uerr := srv.client.UpdateItem(ctx, &UpdateItemInput{
			TableName:                 aws.String("pokemons"),
			Key:                       map[string]*AttributeValue{"id": {S: aws.String("025")}},
			ReturnValues:              ddbtypes.ReturnValueAllOld,
			UpdateExpression:          aws.String("SET #n = :n"),
			ExpressionAttributeNames:  map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]*AttributeValue{":n": {S: aws.String(name)}},
		})
		c.NoError(uerr)

		return out
	}

	c.Empty(update("Pichu").Attributes)

	c.Equal(map[string]*AttributeValue{
		"id":   {S: aws.String("025")},
		"name": {S: aws.String("Pichu")},
	}, update("Pikachu").Attributes)
}
