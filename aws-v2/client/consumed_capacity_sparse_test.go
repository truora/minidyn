package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// A write is billed for the base table plus every secondary index it changes: entering a sparse
// index writes the new entry, leaving it deletes the old one, and changing the index key does
// both. TOTAL carries the same sum as INDEXES.
func TestUpdateItemConsumedCapacityChargesSparseIndexTransitions(t *testing.T) {
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	tests := []struct {
		name      string
		seedType  string // empty: the item is not in the by-type index
		expr      string
		names     map[string]string
		values    map[string]dynamodbtypes.AttributeValue
		wantIndex float64
	}{
		{"into the index", "", "SET #t = :v", map[string]string{"#t": "type"}, map[string]dynamodbtypes.AttributeValue{":v": str("grass")}, 1},
		{"out of the index", "grass", "REMOVE #t", map[string]string{"#t": "type"}, nil, 1},
		{"within the index, key changed", "grass", "SET #t = :v", map[string]string{"#t": "type"}, map[string]dynamodbtypes.AttributeValue{":v": str("poison")}, 2},
		{"within the index, projected attribute changed", "grass", "SET #t = :v", map[string]string{"#t": "name"}, map[string]dynamodbtypes.AttributeValue{":v": str("ivysaur")}, 1},
		{"outside the index", "", "SET #t = :v", map[string]string{"#t": "name"}, map[string]dynamodbtypes.AttributeValue{":v": str("ivysaur")}, 0},
	}

	for _, tt := range tests {
		for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{
			dynamodbtypes.ReturnConsumedCapacityTotal, dynamodbtypes.ReturnConsumedCapacityIndexes,
		} {
			t.Run(tt.name+"/"+string(mode), func(t *testing.T) {
				c := require.New(t)
				client := setupCapacityClient(t)
				c.NoError(ensurePokemonTypeIndex(client))

				seed := map[string]dynamodbtypes.AttributeValue{"id": str("002"), "name": str("bulbasaur")}
				if tt.seedType != "" {
					seed["type"] = str(tt.seedType)
				}

				_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: seed})
				c.NoError(err)

				out, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
					TableName: aws.String(tableName), Key: keyID("002"), UpdateExpression: aws.String(tt.expr),
					ExpressionAttributeNames: tt.names, ExpressionAttributeValues: tt.values,
					ReturnConsumedCapacity: mode,
				})
				c.NoError(err)

				c.Equal(1+tt.wantIndex, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))

				if mode == dynamodbtypes.ReturnConsumedCapacityIndexes {
					c.Equal(1.0, aws.ToFloat64(out.ConsumedCapacity.Table.CapacityUnits))

					if tt.wantIndex == 0 {
						c.Empty(out.ConsumedCapacity.GlobalSecondaryIndexes)
					} else {
						c.Equal(tt.wantIndex, aws.ToFloat64(out.ConsumedCapacity.GlobalSecondaryIndexes["by-type"].CapacityUnits))
					}
				}
			})
		}
	}
}

func TestPutItemConsumedCapacityChargesSparseIndexTransitions(t *testing.T) {
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	tests := []struct {
		name      string
		item      map[string]dynamodbtypes.AttributeValue
		wantIndex float64
	}{
		{"not indexed", map[string]dynamodbtypes.AttributeValue{"id": str("001"), "name": str("x")}, 1},
		{"same index key", map[string]dynamodbtypes.AttributeValue{"id": str("001"), "type": str("grass"), "name": str("x")}, 1},
		{"other index key", map[string]dynamodbtypes.AttributeValue{"id": str("001"), "type": str("fire"), "name": str("x")}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := setupCapacityClient(t) // item 001 is a grass pokemon
			c.NoError(ensurePokemonTypeIndex(client))

			out, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName), Item: tt.item,
				ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
			})
			c.NoError(err)
			c.Equal(1+tt.wantIndex, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
			c.Equal(tt.wantIndex, aws.ToFloat64(out.ConsumedCapacity.GlobalSecondaryIndexes["by-type"].CapacityUnits))
		})
	}
}

func TestTransactUpdateConsumedCapacityChargesSparseIndexTransitions(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	c.NoError(ensurePokemonTypeIndex(client))

	out, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
		TransactItems: []dynamodbtypes.TransactWriteItem{{Update: &dynamodbtypes.Update{
			TableName: aws.String(tableName), Key: keyID("001"),
			UpdateExpression:         aws.String("REMOVE #t"),
			ExpressionAttributeNames: map[string]string{"#t": "type"},
		}}},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	// Transactional writes cost twice as much: base 2 + the deleted index entry 2.
	c.Equal(4.0, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
	c.Equal(2.0, aws.ToFloat64(out.ConsumedCapacity[0].GlobalSecondaryIndexes["by-type"].CapacityUnits))
}
