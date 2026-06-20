package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/capacity"
)

func TestToWireConsumedBreakdowns(t *testing.T) {
	c := require.New(t)

	c.Nil(toWireConsumed(nil))

	gsi := toWireConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, ReadCapacityUnits: 1, Breakdown: true, IndexName: "i", IndexKind: "GSI"})
	c.Contains(gsi.GlobalSecondaryIndexes, "i")
	c.Nil(gsi.Table)

	lsi := toWireConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, WriteCapacityUnits: 1, Breakdown: true, IndexName: "i", IndexKind: "LSI"})
	c.Contains(lsi.LocalSecondaryIndexes, "i")

	tbl := toWireConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, WriteCapacityUnits: 1, Breakdown: true})
	c.NotNil(tbl.Table)
}

func TestWireConsumedSlice(t *testing.T) {
	c := require.New(t)

	c.Nil(wireConsumedSlice(capacity.ModeNone, map[string]float64{"t": 1}, true))
	c.Nil(wireConsumedSlice(capacity.ModeTotal, map[string]float64{}, true))
	c.Nil(wireConsumedSlice(capacity.ModeTotal, map[string]float64{"t": 0}, true))

	out := wireConsumedSlice(capacity.ModeTotal, map[string]float64{"t": 3}, true)
	c.Len(out, 1)
	c.Equal(3.0, aws.ToFloat64(out[0].ReadCapacityUnits))
}

func newServerWithPokemonTable(t *testing.T) *Client {
	t.Helper()

	c := NewClient()

	_, err := c.CreateTable(context.Background(), &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return c
}

func TestServerItemSizeByKey(t *testing.T) {
	c := require.New(t)
	client := newServerWithPokemonTable(t)

	_, err := client.PutItem(context.Background(), &PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]*AttributeValue{
			"id": {S: aws.String("001")},
			"n":  {S: aws.String("bulbasaur")},
		},
	})
	c.NoError(err)

	key := map[string]*AttributeValue{"id": {S: aws.String("001")}}

	c.Greater(client.itemSizeByKey("pokemons", key), 0)                                                         // stored item
	c.Greater(client.itemSizeByKey("pokemons", map[string]*AttributeValue{"id": {S: aws.String("absent")}}), 0) // missing item
	c.Greater(client.itemSizeByKey("no-such-table", key), 0)                                                    // missing table
	c.GreaterOrEqual(client.itemSizeByKey("pokemons", map[string]*AttributeValue{}), 0)                         // bad key
}

func TestServerTransactWriteUpdateConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := newServerWithPokemonTable(t)
	ctx := context.Background()

	_, err := client.PutItem(ctx, &PutItemInput{
		TableName: aws.String("pokemons"),
		Item:      map[string]*AttributeValue{"id": {S: aws.String("001")}, "n": {S: aws.String("bulbasaur")}},
	})
	c.NoError(err)

	out, err := client.TransactWriteItems(ctx, &TransactWriteItemsInput{
		ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
		TransactItems: []TransactWriteItem{
			{Update: &Update{
				TableName:                aws.String("pokemons"),
				Key:                      map[string]*AttributeValue{"id": {S: aws.String("001")}},
				UpdateExpression:         aws.String("SET #n = :n"),
				ExpressionAttributeNames: map[string]string{"#n": "n"},
				ExpressionAttributeValues: map[string]*AttributeValue{
					":n": {S: aws.String("ivysaur")},
				},
			}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity[0].WriteCapacityUnits), 0.0)
}

func TestServerBatchAndTransactGetConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := newServerWithPokemonTable(t)
	ctx := context.Background()

	_, err := client.PutItem(ctx, &PutItemInput{
		TableName: aws.String("pokemons"),
		Item:      map[string]*AttributeValue{"id": {S: aws.String("001")}, "n": {S: aws.String("bulbasaur")}},
	})
	c.NoError(err)

	bw, err := client.BatchWriteItem(ctx, &BatchWriteItemInput{
		ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]WriteRequest{
			"pokemons": {{PutRequest: &PutRequest{Item: map[string]*AttributeValue{"id": {S: aws.String("002")}}}}},
		},
	})
	c.NoError(err)
	c.Len(bw.ConsumedCapacity, 1)

	bg, err := client.BatchGetItem(ctx, &BatchGetItemInput{
		ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string]KeysAndAttributes{
			"pokemons": {Keys: []map[string]*AttributeValue{{"id": {S: aws.String("001")}}}},
		},
	})
	c.NoError(err)
	c.Len(bg.ConsumedCapacity, 1)

	tg, err := client.TransactGetItems(ctx, &TransactGetItemsInput{
		ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityTotal,
		TransactItems: []TransactGetItem{
			{Get: &Get{TableName: aws.String("pokemons"), Key: map[string]*AttributeValue{"id": {S: aws.String("001")}}}},
		},
	})
	c.NoError(err)
	c.Len(tg.ConsumedCapacity, 1)
}
