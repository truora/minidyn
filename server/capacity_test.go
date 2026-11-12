package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/types"
)

func TestToWireConsumedBreakdowns(t *testing.T) {
	c := require.New(t)

	c.Nil(toWireConsumed(nil))

	// An index read: the Table sub-object is always reported (DynamoDB includes it even when an
	// index served the request) alongside the index entry.
	gsi := toWireConsumed(&capacity.Consumed{
		TableName: "t", CapacityUnits: 1, Breakdown: true,
		Indexes: []capacity.IndexUnits{{Name: "i", Kind: "GSI", Units: 1}},
	})
	c.Contains(gsi.GlobalSecondaryIndexes, "i")
	c.NotNil(gsi.Table)

	lsi := toWireConsumed(&capacity.Consumed{
		TableName: "t", CapacityUnits: 1, Breakdown: true, TableUnits: 1,
		Indexes: []capacity.IndexUnits{{Name: "i", Kind: "LSI", Units: 1}},
	})
	c.Contains(lsi.LocalSecondaryIndexes, "i")
	c.NotNil(lsi.Table)

	tbl := toWireConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, Breakdown: true, TableUnits: 1})
	c.NotNil(tbl.Table)
	c.Empty(tbl.GlobalSecondaryIndexes)
}

func TestWireConsumedSlice(t *testing.T) {
	c := require.New(t)

	c.Nil(wireConsumedSlice(capacity.ModeNone, map[string]float64{"t": 1}))
	c.Nil(wireConsumedSlice(capacity.ModeTotal, map[string]float64{}))
	c.Nil(wireConsumedSlice(capacity.ModeTotal, map[string]float64{"t": 0}))

	out := wireConsumedSlice(capacity.ModeTotal, map[string]float64{"t": 3})
	c.Len(out, 1)
	c.Equal(3.0, aws.ToFloat64(out[0].CapacityUnits))
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

func TestServerWriteTargetsTolerateUnknownTables(t *testing.T) {
	c := require.New(t)
	client := newServerWithPokemonTable(t)

	c.NotNil(client.tableOrNil("pokemons"))
	c.Nil(client.tableOrNil("missing"))

	// An unknown table is billed by size alone, without index entries.
	target := capacity.WriteTarget{
		Table: "missing", Kind: capacity.WriteDelete,
		Key: map[string]*types.Item{"id": {S: aws.String("001")}},
	}
	target.Snapshot(client.tableOrNil)

	size, indexes := target.Bill(client.tableOrNil)
	c.Positive(size)
	c.Nil(indexes)
}

func TestServerBatchWriteTargetOfAnEmptyRequestIsAnUnbilledCheck(t *testing.T) {
	target := batchWriteTarget("pokemons", WriteRequest{})
	require.Equal(t, capacity.WriteCheck, target.Kind)
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
	c.Greater(aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits), 0.0)
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

func TestWireConsumedSliceIsSortedByTableName(t *testing.T) {
	units := map[string]float64{}
	want := make([]string, 0, 12)

	for i := range 12 {
		name := fmt.Sprintf("table-%02d", i)
		units[name] = 1
		want = append(want, name)
	}

	for range 20 {
		out := wireConsumedSlice(capacity.ModeTotal, units)
		require.Len(t, out, len(want))

		got := make([]string, 0, len(out))
		for _, cc := range out {
			got = append(got, cc.TableName)
		}

		require.Equal(t, want, got)
	}
}
