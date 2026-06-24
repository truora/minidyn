package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/capacity"
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

	gsi := toSDKConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, ReadCapacityUnits: 1, Breakdown: true, IndexName: "i", IndexKind: "GSI"})
	c.Contains(gsi.GlobalSecondaryIndexes, "i")
	c.Nil(gsi.Table)

	lsi := toSDKConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, WriteCapacityUnits: 1, Breakdown: true, IndexName: "i", IndexKind: "LSI"})
	c.Contains(lsi.LocalSecondaryIndexes, "i")

	tbl := toSDKConsumed(&capacity.Consumed{TableName: "t", CapacityUnits: 1, ReadCapacityUnits: 1, Breakdown: true})
	c.NotNil(tbl.Table)
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

func TestItemSizeByKey(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	c.Greater(client.itemSizeByKey(tableName, keyID("001")), 0)                                     // stored item
	c.Greater(client.itemSizeByKey(tableName, keyID("absent")), 0)                                  // missing item -> key size
	c.Greater(client.itemSizeByKey("no-such-table", keyID("001")), 0)                               // missing table -> key size
	c.GreaterOrEqual(client.itemSizeByKey(tableName, map[string]dynamodbtypes.AttributeValue{}), 0) // bad key -> key size
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
	c.Nil(out.ConsumedCapacity.Table) // index read attributes capacity to the index
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
