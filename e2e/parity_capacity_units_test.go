package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// capacityUnits is the exact TOTAL ConsumedCapacity reported by an engine. DynamoDB Local
// reports only CapacityUnits and TableName at the TOTAL level (no top-level Read/Write
// units), so only those are compared.
type capacityUnits struct {
	Present bool
	Table   string
	Units   float64
}

func exactCapacity(cc *dynamodbtypes.ConsumedCapacity) capacityUnits {
	if cc == nil {
		return capacityUnits{}
	}

	return capacityUnits{
		Present: true,
		Table:   aws.ToString(cc.TableName),
		Units:   aws.ToFloat64(cc.CapacityUnits),
	}
}

func exactCapacities(ccs []dynamodbtypes.ConsumedCapacity) []capacityUnits {
	out := make([]capacityUnits, 0, len(ccs))
	for i := range ccs {
		out = append(out, exactCapacity(&ccs[i]))
	}

	return out
}

func capacityKey(id string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: id}}
}

func capacityStrValue(v string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberS{Value: v}
}

// capacityBigItem puts an item whose name attribute makes the item roughly bytes long.
func capacityBigItem(ctx context.Context, t *testing.T, client *dynamodb.Client, id string, bytes int) {
	t.Helper()

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(parityPokemonTable),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id":   capacityStrValue(id),
			"type": capacityStrValue("normal"),
			"name": capacityStrValue(strings.Repeat("x", bytes)),
		},
	})
	require.NoError(t, err)
}

// TestE2E_ConsumedCapacityExactWrites pins exact write units for deletes and updates of
// large items. DynamoDB bills a delete by the size of the deleted item and an update by the
// larger of the item before and after, for plain and transactional writes (transactional
// writes cost twice as much).
func TestE2E_ConsumedCapacityExactWrites(t *testing.T) {
	const bigBytes = 20000

	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "DeleteItemLarge",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName:              aws.String(parityPokemonTable),
					Key:                    capacityKey("big"),
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)
				t.Logf("DeleteItem large = %#v", out.ConsumedCapacity)

				return exactCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactDeleteLarge",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Delete: &dynamodbtypes.Delete{
							TableName: aws.String(parityPokemonTable),
							Key:       capacityKey("big"),
						}},
					},
				})
				require.NoError(t, err)
				t.Logf("TransactWrite delete large = %#v", out.ConsumedCapacity)

				return exactCapacities(out.ConsumedCapacity)
			},
		},
		{
			name: "UpdateItemShrink",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                 aws.String(parityPokemonTable),
					Key:                       capacityKey("big"),
					UpdateExpression:          aws.String("SET #n = :n"),
					ExpressionAttributeNames:  map[string]string{"#n": "name"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": capacityStrValue("s")},
					ReturnConsumedCapacity:    dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)
				t.Logf("UpdateItem shrink = %#v", out.ConsumedCapacity)

				return exactCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactUpdateShrink",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Update: &dynamodbtypes.Update{
							TableName:                 aws.String(parityPokemonTable),
							Key:                       capacityKey("big"),
							UpdateExpression:          aws.String("SET #n = :n"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": capacityStrValue("s")},
						}},
					},
				})
				require.NoError(t, err)
				t.Logf("TransactWrite update shrink = %#v", out.ConsumedCapacity)

				return exactCapacities(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactUpdateGrow",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "small", 10)

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Update: &dynamodbtypes.Update{
							TableName:                 aws.String(parityPokemonTable),
							Key:                       capacityKey("small"),
							UpdateExpression:          aws.String("SET #n = :n"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": capacityStrValue(strings.Repeat("y", bigBytes))},
						}},
					},
				})
				require.NoError(t, err)
				t.Logf("TransactWrite update grow = %#v", out.ConsumedCapacity)

				return exactCapacities(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactPutReplaceLarger",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": capacityStrValue("big"), "name": capacityStrValue("s"),
							},
						}},
					},
				})
				require.NoError(t, err)
				t.Logf("TransactWrite put over larger item = %#v", out.ConsumedCapacity)

				return exactCapacities(out.ConsumedCapacity)
			},
		},
		{
			name: "BatchDeleteLarge",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: capacityKey("big")}}},
					},
				})
				require.NoError(t, err)
				t.Logf("BatchWrite delete large = %#v", out.ConsumedCapacity)

				return exactCapacities(out.ConsumedCapacity)
			},
		},
		{
			name: "BatchPutReplaceLarger",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				capacityBigItem(ctx, t, client, "big", bigBytes)

				out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: map[string]dynamodbtypes.AttributeValue{
							"id": capacityStrValue("big"), "name": capacityStrValue("s"),
						}}}},
					},
				})
				require.NoError(t, err)
				t.Logf("BatchWrite put over larger item = %#v", out.ConsumedCapacity)

				return exactCapacities(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactDeleteWithGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Delete: &dynamodbtypes.Delete{TableName: aws.String(parityPokemonTable), Key: capacityKey("001")}},
					},
				})
				require.NoError(t, err)
				t.Logf("TransactWrite delete(GSI) = %#v", out.ConsumedCapacity)

				return sortedBreakdowns(out.ConsumedCapacity)
			},
		},
		{
			name: "BatchDeleteWithGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: capacityKey("001")}}},
					},
				})
				require.NoError(t, err)
				t.Logf("BatchWrite delete(GSI) = %#v", out.ConsumedCapacity)

				return sortedBreakdowns(out.ConsumedCapacity)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
