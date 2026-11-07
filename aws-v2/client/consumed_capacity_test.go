package client

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/capacity"
	mtypes "github.com/truora/minidyn/types"
)

func setupCapacityClient(t *testing.T) *Client {
	t.Helper()

	client := NewClient()
	require.NoError(t, ensurePokemonTable(client))
	require.NoError(t, createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}))

	return client
}

func keyID(id string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"id": &dynamodbtypes.AttributeValueMemberS{Value: id},
	}
}

func TestToSDKConsumedBreakdowns(t *testing.T) {
	c := require.New(t)

	c.Nil(toSDKConsumed(nil))

	// An index read: the Table sub-object is always reported (DynamoDB includes it even when an
	// index served the request) alongside the index entry.
	gsi := toSDKConsumed(&capacity.Consumed{
		TableName: "t", CapacityUnits: 1, ReadCapacityUnits: 1, Breakdown: true,
		Indexes: []capacity.IndexUnits{{Name: "i", Kind: "GSI", Units: 1}},
	})
	c.Contains(gsi.GlobalSecondaryIndexes, "i")
	c.NotNil(gsi.Table)

	lsi := toSDKConsumed(&capacity.Consumed{
		TableName: "t", CapacityUnits: 1, WriteCapacityUnits: 1, Breakdown: true, TableUnits: 1,
		Indexes: []capacity.IndexUnits{{Name: "i", Kind: "LSI", Units: 1}},
	})
	c.Contains(lsi.LocalSecondaryIndexes, "i")
	c.NotNil(lsi.Table)

	tbl := toSDKConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, ReadCapacityUnits: 1, Breakdown: true, TableUnits: 1})
	c.NotNil(tbl.Table)
	c.Empty(tbl.GlobalSecondaryIndexes)
}

func TestSDKConsumedSlice(t *testing.T) {
	c := require.New(t)

	c.Nil(sdkConsumedSlice(capacity.ModeNone, map[string]float64{"t": 1}, true))
	c.Nil(sdkConsumedSlice(capacity.ModeTotal, map[string]float64{}, true))
	c.Nil(sdkConsumedSlice(capacity.ModeTotal, map[string]float64{"t": 0}, true)) // zero-unit table skipped

	out := sdkConsumedSlice(capacity.ModeTotal, map[string]float64{"t": 2}, false)
	c.Len(out, 1)
	c.Equal(2.0, aws.ToFloat64(out[0].WriteCapacityUnits))
}

func TestWriteTargetsTolerateUnknownTables(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	c.NotNil(client.tableOrNil(tableName))
	c.Nil(client.tableOrNil("missing"))

	// An unknown table is billed by size alone, without index entries.
	target := capacity.WriteTarget{
		Table: "missing", Kind: capacity.WriteDelete,
		Key: map[string]*mtypes.Item{"id": {S: aws.String("001")}},
	}
	client.snapshotLocked(&target)

	size, indexes := client.billLocked(&target)
	c.Positive(size)
	c.Nil(indexes)
}

func TestBatchWriteTargetOfAnEmptyRequestIsAnUnbilledCheck(t *testing.T) {
	target := batchWriteTarget(tableName, dynamodbtypes.WriteRequest{})
	require.Equal(t, capacity.WriteCheck, target.Kind)
}

func TestTransactWriteItemsConsumedCapacityUpdate(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		TransactItems: []dynamodbtypes.TransactWriteItem{
			{Update: &dynamodbtypes.Update{
				TableName:                aws.String(tableName),
				Key:                      keyID("001"),
				UpdateExpression:         aws.String("SET #t = :t"),
				ExpressionAttributeNames: map[string]string{"#t": "type"},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":t": &dynamodbtypes.AttributeValueMemberS{Value: "ice"},
				},
			}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity[0].WriteCapacityUnits), 0.0)
}

func TestGetItemConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	ctx := context.Background()

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyID("001"),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Equal(tableName, aws.ToString(out.ConsumedCapacity.TableName))
	c.Greater(aws.ToFloat64(out.ConsumedCapacity.CapacityUnits), 0.0)
	c.Equal(aws.ToFloat64(out.ConsumedCapacity.CapacityUnits), aws.ToFloat64(out.ConsumedCapacity.ReadCapacityUnits))
	c.Nil(out.ConsumedCapacity.Table) // TOTAL has no breakdown

	// Eventually consistent read costs half of a strongly consistent one.
	eventual, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyID("001"),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.Equal(aws.ToFloat64(out.ConsumedCapacity.CapacityUnits)/2, aws.ToFloat64(eventual.ConsumedCapacity.CapacityUnits))
}

func TestGetItemConsumedCapacityOmittedWhenNone(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       keyID("001"),
	})
	c.NoError(err)
	c.Nil(out.ConsumedCapacity)
}

func TestGetItemConsumedCapacityMissingItem(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyID("does-not-exist"),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Equal(1.0, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits)) // min 1 read unit
}

func TestPutItemConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id":   &dynamodbtypes.AttributeValueMemberS{Value: "010"},
			"name": &dynamodbtypes.AttributeValueMemberS{Value: "Caterpie"},
		},
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity.CapacityUnits), 0.0)
	c.Equal(aws.ToFloat64(out.ConsumedCapacity.CapacityUnits), aws.ToFloat64(out.ConsumedCapacity.WriteCapacityUnits))
	c.Nil(out.ConsumedCapacity.ReadCapacityUnits)
}

func TestUpdateItemConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName),
		Key:                      keyID("001"),
		UpdateExpression:         aws.String("SET #t = :t"),
		ExpressionAttributeNames: map[string]string{"#t": "type"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":t": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
		},
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity.WriteCapacityUnits), 0.0)
}

func TestDeleteItemConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyID("001"),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity.WriteCapacityUnits), 0.0)
}

func TestQueryConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                aws.String(tableName),
		KeyConditionExpression:   aws.String("#id = :id"),
		ExpressionAttributeNames: map[string]string{"#id": "id"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
		},
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity.ReadCapacityUnits), 0.0)
}

func TestQueryIndexConsumedCapacityIndexes(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	c.NoError(ensurePokemonTypeIndex(client))
	c.NoError(createPokemon(client, pokemon{ID: "004", Type: "fire", Name: "Charmander"}))

	out, err := client.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                aws.String(tableName),
		IndexName:                aws.String("by-type"),
		KeyConditionExpression:   aws.String("#t = :t"),
		ExpressionAttributeNames: map[string]string{"#t": "type"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":t": &dynamodbtypes.AttributeValueMemberS{Value: "fire"},
		},
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Contains(out.ConsumedCapacity.GlobalSecondaryIndexes, "by-type")
	// DynamoDB always reports the Table sub-object at the INDEXES level, even for an index read
	// (with zero units, since the base table is not directly read).
	c.NotNil(out.ConsumedCapacity.Table)
	c.Equal(0.0, aws.ToFloat64(out.ConsumedCapacity.Table.CapacityUnits))
}

func TestScanConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName:              aws.String(tableName),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity.ReadCapacityUnits), 0.0)
}

func TestBatchGetItemConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
			tableName: {Keys: []map[string]dynamodbtypes.AttributeValue{keyID("001")}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Equal(tableName, aws.ToString(out.ConsumedCapacity[0].TableName))
	c.Greater(aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits), 0.0)
}

func TestBatchWriteItemConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {
				{PutRequest: &dynamodbtypes.PutRequest{Item: map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{Value: "020"},
				}}},
			},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity[0].WriteCapacityUnits), 0.0)
}

func TestTransactGetItemsConsumedCapacity(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	out, err := client.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		TransactItems: []dynamodbtypes.TransactGetItem{
			{Get: &dynamodbtypes.Get{TableName: aws.String(tableName), Key: keyID("001")}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Greater(aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits), 0.0)
}

func TestTransactWriteItemsConsumedCapacityDoublesWrite(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	item := map[string]dynamodbtypes.AttributeValue{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: "030"},
		"name": &dynamodbtypes.AttributeValueMemberS{Value: "Pidgey"},
	}

	batch, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {{PutRequest: &dynamodbtypes.PutRequest{Item: item}}},
		},
	})
	c.NoError(err)
	c.Len(batch.ConsumedCapacity, 1)

	txn, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		TransactItems: []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: aws.String(tableName), Item: item}},
		},
	})
	c.NoError(err)
	c.Len(txn.ConsumedCapacity, 1)

	c.Equal(
		2*aws.ToFloat64(batch.ConsumedCapacity[0].CapacityUnits),
		aws.ToFloat64(txn.ConsumedCapacity[0].CapacityUnits),
	)
}

func TestPutItemConsumedCapacityIndexesChargesGSI(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	c.NoError(ensurePokemonTypeIndex(client))

	out, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id":   &dynamodbtypes.AttributeValueMemberS{Value: "010"},
			"type": &dynamodbtypes.AttributeValueMemberS{Value: "bug"},
		},
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
	})
	c.NoError(err)
	c.NotNil(out.ConsumedCapacity)
	c.Contains(out.ConsumedCapacity.GlobalSecondaryIndexes, "by-type")
}

func TestDeleteItemConsumedCapacityBillsDeletedItemWithoutReturnValues(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id":   &dynamodbtypes.AttributeValueMemberS{Value: "big"},
			"name": &dynamodbtypes.AttributeValueMemberS{Value: strings.Repeat("x", 20000)},
		},
	})
	c.NoError(err)

	out, err := client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyID("big"),
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
	})
	c.NoError(err)
	c.Nil(out.Attributes)
	c.Equal(20.0, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
}

func TestSDKConsumedSliceIsSortedByTableName(t *testing.T) {
	units := map[string]float64{}
	want := make([]string, 0, 12)

	for i := range 12 {
		name := fmt.Sprintf("table-%02d", i)
		units[name] = 1
		want = append(want, name)
	}

	for range 20 {
		out := sdkConsumedSlice(capacity.ModeTotal, units, true)
		require.Len(t, out, len(want))

		got := make([]string, 0, len(out))
		for _, cc := range out {
			got = append(got, aws.ToString(cc.TableName))
		}

		require.Equal(t, want, got)
	}
}
