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

func newServerWithTypeIndex(t *testing.T) *Client {
	t.Helper()

	c := NewClient()

	_, err := c.CreateTable(context.Background(), &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("by-type"),
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
			},
			Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)

	_, err = c.PutItem(context.Background(), &PutItemInput{
		TableName: aws.String("pokemons"),
		Item:      map[string]*AttributeValue{"id": {S: aws.String("001")}, "type": {S: aws.String("grass")}},
	})
	require.NoError(t, err)

	return c
}

// A write charges the secondary indexes the written item lands in or the deleted item leaves.
func TestServerWriteConsumedCapacityIndexesChargesGSI(t *testing.T) {
	ctx := context.Background()
	table := aws.String("pokemons")
	indexes := ddbtypes.ReturnConsumedCapacityIndexes

	t.Run("put", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.PutItem(ctx, &PutItemInput{
			TableName: table, ReturnConsumedCapacity: indexes,
			Item: map[string]*AttributeValue{"id": {S: aws.String("002")}, "type": {S: aws.String("fire")}},
		})
		require.NoError(t, err)
		require.Contains(t, out.ConsumedCapacity.GlobalSecondaryIndexes, "by-type")
	})

	t.Run("delete without ReturnValues", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.DeleteItem(ctx, &DeleteItemInput{TableName: table, Key: idKey("001"), ReturnConsumedCapacity: indexes})
		require.NoError(t, err)
		require.Contains(t, out.ConsumedCapacity.GlobalSecondaryIndexes, "by-type")
	})

	t.Run("update", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.UpdateItem(ctx, &UpdateItemInput{
			TableName: table, Key: idKey("001"), ReturnConsumedCapacity: indexes,
			UpdateExpression:          aws.String("SET #t = :t"),
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]*AttributeValue{":t": {S: aws.String("poison")}},
		})
		require.NoError(t, err)
		require.Contains(t, out.ConsumedCapacity.GlobalSecondaryIndexes, "by-type")
	})

	t.Run("transact delete", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.TransactWriteItems(ctx, &TransactWriteItemsInput{
			ReturnConsumedCapacity: indexes,
			TransactItems:          []TransactWriteItem{{Delete: &Delete{TableName: table, Key: idKey("001")}}},
		})
		require.NoError(t, err)
		require.Len(t, out.ConsumedCapacity, 1)
		require.Contains(t, out.ConsumedCapacity[0].GlobalSecondaryIndexes, "by-type")
	})

	t.Run("batch delete", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.BatchWriteItem(ctx, &BatchWriteItemInput{
			ReturnConsumedCapacity: indexes,
			RequestItems:           map[string][]WriteRequest{"pokemons": {{DeleteRequest: &DeleteRequest{Key: idKey("001")}}}},
		})
		require.NoError(t, err)
		require.Len(t, out.ConsumedCapacity, 1)
		require.Contains(t, out.ConsumedCapacity[0].GlobalSecondaryIndexes, "by-type")
	})
}

// A write is billed for the base table plus every secondary index it changes: entering a sparse
// index writes the new entry, leaving it deletes the old one, and changing the index key does
// both.
func TestServerWriteConsumedCapacityChargesSparseIndexTransitions(t *testing.T) {
	ctx := context.Background()
	table := aws.String("pokemons")
	indexes := ddbtypes.ReturnConsumedCapacityIndexes

	update := func(expr string, names map[string]string, values map[string]*AttributeValue) *UpdateItemInput {
		return &UpdateItemInput{
			TableName: table, Key: idKey("001"), UpdateExpression: aws.String(expr),
			ExpressionAttributeNames: names, ExpressionAttributeValues: values, ReturnConsumedCapacity: indexes,
		}
	}

	tests := []struct {
		name      string
		in        *UpdateItemInput
		wantIndex float64
	}{
		{"out of the index", update("REMOVE #t", map[string]string{"#t": "type"}, nil), 1},
		{"key changed", update("SET #t = :v", map[string]string{"#t": "type"}, map[string]*AttributeValue{":v": {S: aws.String("poison")}}), 2},
		{"projected attribute changed", update("SET #t = :v", map[string]string{"#t": "name"}, map[string]*AttributeValue{":v": {S: aws.String("x")}}), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newServerWithTypeIndex(t)

			out, err := client.UpdateItem(ctx, tt.in)
			require.NoError(t, err)
			require.Equal(t, 1+tt.wantIndex, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
			require.Equal(t, tt.wantIndex, aws.ToFloat64(out.ConsumedCapacity.GlobalSecondaryIndexes["by-type"].CapacityUnits))
		})
	}

	t.Run("put over an indexed item", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.PutItem(ctx, &PutItemInput{
			TableName: table, ReturnConsumedCapacity: indexes,
			Item: map[string]*AttributeValue{"id": {S: aws.String("001")}, "type": {S: aws.String("fire")}},
		})
		require.NoError(t, err)
		require.Equal(t, 3.0, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
	})

	t.Run("transact update out of the index", func(t *testing.T) {
		client := newServerWithTypeIndex(t)

		out, err := client.TransactWriteItems(ctx, &TransactWriteItemsInput{
			ReturnConsumedCapacity: indexes,
			TransactItems: []TransactWriteItem{{Update: &Update{
				TableName: table, Key: idKey("001"), UpdateExpression: aws.String("REMOVE #t"),
				ExpressionAttributeNames: map[string]string{"#t": "type"},
			}}},
		})
		require.NoError(t, err)
		require.Len(t, out.ConsumedCapacity, 1)
		require.Equal(t, 4.0, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
	})
}
