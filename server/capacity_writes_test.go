package server

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func bigPokemonItem(id string, bytes int) map[string]*AttributeValue {
	return map[string]*AttributeValue{
		"id":   {S: aws.String(id)},
		"name": {S: aws.String(strings.Repeat("x", bytes))},
	}
}

func putBigPokemonItem(t *testing.T, client *Client, id string, bytes int) {
	t.Helper()

	_, err := client.PutItem(context.Background(), &PutItemInput{
		TableName: aws.String("pokemons"),
		Item:      bigPokemonItem(id, bytes),
	})
	require.NoError(t, err)
}

func idKey(id string) map[string]*AttributeValue {
	return map[string]*AttributeValue{"id": {S: aws.String(id)}}
}

// DynamoDB bills a delete by the item it removed and an update or replacing put by the
// larger of the item before and after; transactional writes cost twice as much. The sizes
// have to be measured before the writes are applied.
func TestServerTransactWriteConsumedCapacityMeasuresItemsBeforeWriting(t *testing.T) {
	const bigBytes = 20000

	setNameUpdate := func(id string, bytes int) *Update {
		return &Update{
			TableName:                aws.String("pokemons"),
			Key:                      idKey(id),
			UpdateExpression:         aws.String("SET #n = :n"),
			ExpressionAttributeNames: map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]*AttributeValue{
				":n": {S: aws.String(strings.Repeat("y", bytes))},
			},
		}
	}

	tests := []struct {
		name string
		seed func(t *testing.T, client *Client)
		item TransactWriteItem
	}{
		{
			name: "delete bills the deleted item",
			seed: func(t *testing.T, client *Client) { putBigPokemonItem(t, client, "big", bigBytes) },
			item: TransactWriteItem{Delete: &Delete{TableName: aws.String("pokemons"), Key: idKey("big")}},
		},
		{
			name: "update that shrinks bills the item before",
			seed: func(t *testing.T, client *Client) { putBigPokemonItem(t, client, "big", bigBytes) },
			item: TransactWriteItem{Update: setNameUpdate("big", 1)},
		},
		{
			name: "update that grows bills the item after",
			seed: func(t *testing.T, client *Client) { putBigPokemonItem(t, client, "small", 10) },
			item: TransactWriteItem{Update: setNameUpdate("small", bigBytes)},
		},
		{
			name: "put replacing a larger item bills the item before",
			seed: func(t *testing.T, client *Client) { putBigPokemonItem(t, client, "big", bigBytes) },
			item: TransactWriteItem{Put: &Put{TableName: aws.String("pokemons"), Item: bigPokemonItem("big", 1)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := newServerWithPokemonTable(t)
			tt.seed(t, client)

			out, err := client.TransactWriteItems(context.Background(), &TransactWriteItemsInput{
				ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
				TransactItems:          []TransactWriteItem{tt.item},
			})
			c.NoError(err)
			c.Len(out.ConsumedCapacity, 1)
			c.Equal(40.0, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
		})
	}
}

func TestServerBatchWriteConsumedCapacityMeasuresItemsBeforeWriting(t *testing.T) {
	tests := []struct {
		name string
		req  WriteRequest
	}{
		{"delete bills the deleted item", WriteRequest{DeleteRequest: &DeleteRequest{Key: idKey("big")}}},
		{"put replacing a larger item bills the item before", WriteRequest{PutRequest: &PutRequest{Item: bigPokemonItem("big", 1)}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := newServerWithPokemonTable(t)
			putBigPokemonItem(t, client, "big", 20000)

			out, err := client.BatchWriteItem(context.Background(), &BatchWriteItemInput{
				ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
				RequestItems:           map[string][]WriteRequest{"pokemons": {tt.req}},
			})
			c.NoError(err)
			c.Len(out.ConsumedCapacity, 1)
			c.Equal(20.0, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
		})
	}
}

func TestServerDeleteItemConsumedCapacityBillsDeletedItemWithoutReturnValues(t *testing.T) {
	c := require.New(t)
	client := newServerWithPokemonTable(t)
	putBigPokemonItem(t, client, "big", 20000)

	out, err := client.DeleteItem(context.Background(), &DeleteItemInput{
		TableName:              aws.String("pokemons"),
		Key:                    idKey("big"),
		ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.Nil(out.Attributes)
	c.Equal(20.0, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
}

func TestServerUpdateItemConsumedCapacityBillsLargerOfBeforeAndAfter(t *testing.T) {
	c := require.New(t)
	client := newServerWithPokemonTable(t)
	putBigPokemonItem(t, client, "big", 20000)

	out, err := client.UpdateItem(context.Background(), &UpdateItemInput{
		TableName:                aws.String("pokemons"),
		Key:                      idKey("big"),
		UpdateExpression:         aws.String("SET #n = :n"),
		ExpressionAttributeNames: map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]*AttributeValue{
			":n": {S: aws.String("s")},
		},
		ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.Equal(20.0, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
}
