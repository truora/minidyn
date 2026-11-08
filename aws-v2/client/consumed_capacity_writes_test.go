package client

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func bigPokemon(id string, bytes int) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: id},
		"type": &dynamodbtypes.AttributeValueMemberS{Value: "normal"},
		"name": &dynamodbtypes.AttributeValueMemberS{Value: strings.Repeat("x", bytes)},
	}
}

func putBigPokemon(t *testing.T, client *Client, id string, bytes int) {
	t.Helper()

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      bigPokemon(id, bytes),
	})
	require.NoError(t, err)
}

func setNameUpdate(bytes int) *dynamodbtypes.Update {
	return &dynamodbtypes.Update{
		TableName:                aws.String(tableName),
		UpdateExpression:         aws.String("SET #n = :n"),
		ExpressionAttributeNames: map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":n": &dynamodbtypes.AttributeValueMemberS{Value: strings.Repeat("y", bytes)},
		},
	}
}

// DynamoDB bills a delete by the item it removed and an update or replacing put by the
// larger of the item before and after; transactional writes cost twice as much. The sizes
// have to be measured before the writes are applied.
func TestTransactWriteItemsConsumedCapacityMeasuresItemsBeforeWriting(t *testing.T) {
	const bigBytes = 20000

	tests := []struct {
		name  string
		setup func(t *testing.T, client *Client)
		item  func() dynamodbtypes.TransactWriteItem
		want  float64
	}{
		{
			name:  "delete bills the deleted item",
			setup: func(t *testing.T, client *Client) { putBigPokemon(t, client, "big", bigBytes) },
			item: func() dynamodbtypes.TransactWriteItem {
				return dynamodbtypes.TransactWriteItem{Delete: &dynamodbtypes.Delete{
					TableName: aws.String(tableName), Key: keyID("big"),
				}}
			},
			want: 40,
		},
		{
			name:  "update that shrinks bills the item before",
			setup: func(t *testing.T, client *Client) { putBigPokemon(t, client, "big", bigBytes) },
			item: func() dynamodbtypes.TransactWriteItem {
				u := setNameUpdate(1)
				u.Key = keyID("big")

				return dynamodbtypes.TransactWriteItem{Update: u}
			},
			want: 40,
		},
		{
			name:  "update that grows bills the item after",
			setup: func(t *testing.T, client *Client) { putBigPokemon(t, client, "small", 10) },
			item: func() dynamodbtypes.TransactWriteItem {
				u := setNameUpdate(bigBytes)
				u.Key = keyID("small")

				return dynamodbtypes.TransactWriteItem{Update: u}
			},
			want: 40,
		},
		{
			name:  "put replacing a larger item bills the item before",
			setup: func(t *testing.T, client *Client) { putBigPokemon(t, client, "big", bigBytes) },
			item: func() dynamodbtypes.TransactWriteItem {
				return dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{
					TableName: aws.String(tableName), Item: bigPokemon("big", 1),
				}}
			},
			want: 40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := setupCapacityClient(t)
			tt.setup(t, client)

			out, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				TransactItems:          []dynamodbtypes.TransactWriteItem{tt.item()},
			})
			c.NoError(err)
			c.Len(out.ConsumedCapacity, 1)
			c.Equal(tt.want, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
		})
	}
}

func TestTransactWriteItemsConsumedCapacityDeleteChargesIndexes(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	c.NoError(ensurePokemonTypeIndex(client))

	out, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
		TransactItems: []dynamodbtypes.TransactWriteItem{
			{Delete: &dynamodbtypes.Delete{TableName: aws.String(tableName), Key: keyID("001")}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Contains(out.ConsumedCapacity[0].GlobalSecondaryIndexes, "by-type")
}

func TestBatchWriteItemConsumedCapacityDeleteBillsStoredItem(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	putBigPokemon(t, client, "big", 20000)

	out, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: keyID("big")}}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Equal(20.0, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
}

func TestBatchWriteItemConsumedCapacityPutReplacingLargerItem(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)
	putBigPokemon(t, client, "big", 20000)

	out, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {{PutRequest: &dynamodbtypes.PutRequest{Item: bigPokemon("big", 1)}}},
		},
	})
	c.NoError(err)
	c.Len(out.ConsumedCapacity, 1)
	c.Equal(20.0, aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits))
}

// An empty list or map is a 3 byte container, not a 1 byte NULL: this item is 1026 bytes (2 WCU)
// with it and would be 1024 (1 WCU) if it were sized as NULL.
func TestPutItemConsumedCapacitySizesEmptyContainers(t *testing.T) {
	attrName := strings.Repeat("a", 1020)

	values := map[string]struct {
		value dynamodbtypes.AttributeValue
		want  float64
	}{
		"empty list": {&dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{}}, 2},
		"empty map":  {&dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{}}, 2},
		"null":       {&dynamodbtypes.AttributeValueMemberNULL{Value: true}, 1},
	}

	for name, tc := range values {
		t.Run(name, func(t *testing.T) {
			c := require.New(t)
			client := setupCapacityClient(t)

			out, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{Value: "x"}, attrName: tc.value,
				},
				ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
			})
			c.NoError(err)
			c.Equal(tc.want, aws.ToFloat64(out.ConsumedCapacity.CapacityUnits))
		})
	}
}
