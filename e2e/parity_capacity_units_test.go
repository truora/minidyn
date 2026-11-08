package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
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

// capacitySeedPokemon puts a pokemon that has a type attribute (and so lands in the by-type
// index) only when typ is not empty.
func capacitySeedPokemon(ctx context.Context, t *testing.T, client *dynamodb.Client, id, typ string) {
	t.Helper()

	item := map[string]dynamodbtypes.AttributeValue{"id": capacityStrValue(id), "name": capacityStrValue("bulbasaur")}
	if typ != "" {
		item["type"] = capacityStrValue(typ)
	}

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(parityPokemonTable), Item: item})
	require.NoError(t, err)
}

// capacityDetail is the exact INDEXES breakdown: the total and the units of the table and of
// every secondary index charged.
type capacityDetail struct {
	Present bool
	Total   float64
	Table   float64
	GSI     map[string]float64
}

func detailCapacity(cc *dynamodbtypes.ConsumedCapacity) capacityDetail {
	if cc == nil {
		return capacityDetail{}
	}

	d := capacityDetail{Present: true, Total: aws.ToFloat64(cc.CapacityUnits)}
	if cc.Table != nil {
		d.Table = aws.ToFloat64(cc.Table.CapacityUnits)
	}

	for name, c := range cc.GlobalSecondaryIndexes {
		if d.GSI == nil {
			d.GSI = map[string]float64{}
		}

		d.GSI[name] = aws.ToFloat64(c.CapacityUnits)
	}

	return d
}

// TestE2E_ConsumedCapacitySparseGSIWrites pins the exact units of writes against a table with
// a sparse GSI (by-type is keyed on the optional "type" attribute): the total is the base
// table plus every affected index, including an update that moves an item into, out of or
// within the index.
func TestE2E_ConsumedCapacitySparseGSIWrites(t *testing.T) {
	update := func(expr, attr string, values map[string]dynamodbtypes.AttributeValue) func(*dynamodb.UpdateItemInput) {
		return func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression = aws.String(expr)
			in.ExpressionAttributeNames = map[string]string{"#t": attr}
			in.ExpressionAttributeValues = values
		}
	}

	cases := []struct {
		name  string
		seed  string // the item's type, empty when it is not in the sparse index
		apply func(*dynamodb.UpdateItemInput)
	}{
		{
			"UpdateIntoIndex", "",
			update("SET #t = :t", "type", map[string]dynamodbtypes.AttributeValue{":t": capacityStrValue("grass")}),
		},
		{
			"UpdateOutOfIndex", "grass",
			update("REMOVE #t", "type", nil),
		},
		{
			"UpdateWithinIndex", "grass",
			update("SET #t = :t", "type", map[string]dynamodbtypes.AttributeValue{":t": capacityStrValue("poison")}),
		},
		{
			"UpdateIndexedNonKeyAttribute", "grass",
			update("SET #t = :n", "name", map[string]dynamodbtypes.AttributeValue{":n": capacityStrValue("ivysaur")}),
		},
		{
			"UpdateOutsideIndex", "",
			update("SET #t = :n", "name", map[string]dynamodbtypes.AttributeValue{":n": capacityStrValue("ivysaur")}),
		},
	}

	for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{
		dynamodbtypes.ReturnConsumedCapacityTotal, dynamodbtypes.ReturnConsumedCapacityIndexes,
	} {
		for _, tc := range cases {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
					t.Helper()
					ctx := context.Background()
					parityCreatePokemonTable(ctx, t, client)
					parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
					capacitySeedPokemon(ctx, t, client, "001", tc.seed)

					in := &dynamodb.UpdateItemInput{
						TableName: aws.String(parityPokemonTable), Key: capacityKey("001"), ReturnConsumedCapacity: mode,
					}
					tc.apply(in)

					out, err := client.UpdateItem(ctx, in)
					require.NoError(t, err)
					t.Logf("%s %s = %#v", mode, tc.name, out.ConsumedCapacity)

					return detailCapacity(out.ConsumedCapacity)
				})
			})
		}

		t.Run(string(mode)+"/PutWithoutIndexKey", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable), ReturnConsumedCapacity: mode,
					Item: map[string]dynamodbtypes.AttributeValue{"id": capacityStrValue("001"), "name": capacityStrValue("x")},
				})
				require.NoError(t, err)

				return detailCapacity(out.ConsumedCapacity)
			})
		})

		t.Run(string(mode)+"/PutOverwriteWithinIndex", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				capacitySeedPokemon(ctx, t, client, "001", "grass")

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable), ReturnConsumedCapacity: mode,
					Item: map[string]dynamodbtypes.AttributeValue{
						"id": capacityStrValue("001"), "type": capacityStrValue("grass"), "name": capacityStrValue("ivysaur"),
					},
				})
				require.NoError(t, err)

				return detailCapacity(out.ConsumedCapacity)
			})
		})

		t.Run(string(mode)+"/PutOverwriteChangesIndexKey", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				capacitySeedPokemon(ctx, t, client, "001", "grass")

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable), ReturnConsumedCapacity: mode,
					Item: map[string]dynamodbtypes.AttributeValue{
						"id": capacityStrValue("001"), "type": capacityStrValue("poison"), "name": capacityStrValue("bulbasaur"),
					},
				})
				require.NoError(t, err)

				return detailCapacity(out.ConsumedCapacity)
			})
		})

		t.Run(string(mode)+"/PutOverwriteMovesOutOfIndex", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable), ReturnConsumedCapacity: mode,
					Item: map[string]dynamodbtypes.AttributeValue{"id": capacityStrValue("001"), "name": capacityStrValue("x")},
				})
				require.NoError(t, err)
				t.Logf("%s put out of index = %#v", mode, out.ConsumedCapacity)

				return detailCapacity(out.ConsumedCapacity)
			})
		})

		t.Run(string(mode)+"/BatchPutWithGSI", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					ReturnConsumedCapacity: mode,
					RequestItems: map[string][]dynamodbtypes.WriteRequest{parityPokemonTable: {
						{PutRequest: &dynamodbtypes.PutRequest{Item: parityMarshalPokemon(t, parityPokemon{ID: "a", Type: "fire", Name: "a"})}},
						{PutRequest: &dynamodbtypes.PutRequest{Item: map[string]dynamodbtypes.AttributeValue{"id": capacityStrValue("b")}}},
					}},
				})
				require.NoError(t, err)
				require.Len(t, out.ConsumedCapacity, 1)

				return detailCapacity(&out.ConsumedCapacity[0])
			})
		})

		t.Run(string(mode)+"/TransactUpdateMovesIntoIndex", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				capacitySeedPokemon(ctx, t, client, "001", "")

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: mode,
					TransactItems: []dynamodbtypes.TransactWriteItem{{Update: &dynamodbtypes.Update{
						TableName: aws.String(parityPokemonTable), Key: capacityKey("001"),
						UpdateExpression:          aws.String("SET #t = :t"),
						ExpressionAttributeNames:  map[string]string{"#t": "type"},
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":t": capacityStrValue("grass")},
					}}},
				})
				require.NoError(t, err)
				require.Len(t, out.ConsumedCapacity, 1)
				t.Logf("%s transact into index = %#v", mode, out.ConsumedCapacity)

				return detailCapacity(&out.ConsumedCapacity[0])
			})
		})

		t.Run(string(mode)+"/TransactUpdateMovesOutOfIndex", func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: mode,
					TransactItems: []dynamodbtypes.TransactWriteItem{{Update: &dynamodbtypes.Update{
						TableName: aws.String(parityPokemonTable), Key: capacityKey("001"),
						UpdateExpression:         aws.String("REMOVE #t"),
						ExpressionAttributeNames: map[string]string{"#t": "type"},
					}}},
				})
				require.NoError(t, err)
				require.Len(t, out.ConsumedCapacity, 1)
				t.Logf("%s transact out of index = %#v", mode, out.ConsumedCapacity)

				return detailCapacity(&out.ConsumedCapacity[0])
			})
		})
	}
}

// TestE2E_ConsumedCapacityInvalidMode covers a ReturnConsumedCapacity value outside the enum
// ("FOO", or "total" since values are case sensitive), on every operation that accepts it.
//
// This is not a RunE2E parity case because DynamoDB Local does not validate the value: it
// accepts it and answers as if it were NONE. DynamoDB itself rejects it with a
// ValidationException, so minidyn follows the service and this test pins both sides: minidyn
// must return the service's validation error, and DynamoDB Local must keep accepting the
// value (when that stops being true, Local started validating and this should become a
// RunE2E parity case).
func TestE2E_ConsumedCapacityInvalidMode(t *testing.T) {
	ops := map[string]func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error{
		"GetItem": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(parityPokemonTable), Key: capacityKey("1"), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"PutItem": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(parityPokemonTable), Item: capacityKey("1"), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"UpdateItem": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(parityPokemonTable), Key: capacityKey("1"), ReturnConsumedCapacity: mode,
				UpdateExpression:          aws.String("SET #n = :n"),
				ExpressionAttributeNames:  map[string]string{"#n": "name"},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": capacityStrValue("s")},
			})

			return err
		},
		"DeleteItem": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(parityPokemonTable), Key: capacityKey("1"), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"Query": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.Query(ctx, &dynamodb.QueryInput{
				TableName: aws.String(parityPokemonTable), ReturnConsumedCapacity: mode,
				KeyConditionExpression:    aws.String("id = :id"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":id": capacityStrValue("1")},
			})

			return err
		},
		"Scan": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.Scan(ctx, &dynamodb.ScanInput{
				TableName: aws.String(parityPokemonTable), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"BatchGetItem": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
					parityPokemonTable: {Keys: []map[string]dynamodbtypes.AttributeValue{capacityKey("1")}},
				},
			})

			return err
		},
		"BatchWriteItem": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems: map[string][]dynamodbtypes.WriteRequest{
					parityPokemonTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: capacityKey("1")}}},
				},
			})

			return err
		},
		"TransactGetItems": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
				ReturnConsumedCapacity: mode,
				TransactItems: []dynamodbtypes.TransactGetItem{
					{Get: &dynamodbtypes.Get{TableName: aws.String(parityPokemonTable), Key: capacityKey("1")}},
				},
			})

			return err
		},
		"TransactWriteItems": func(ctx context.Context, client *dynamodb.Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				ReturnConsumedCapacity: mode,
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{Put: &dynamodbtypes.Put{TableName: aws.String(parityPokemonTable), Item: capacityKey("1")}},
				},
			})

			return err
		},
	}

	const wantFormat = "1 validation error detected: Value '%s' at 'returnConsumedCapacity' failed to " +
		"satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"

	for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{"FOO", "total"} {
		for name, op := range ops {
			t.Run(fmt.Sprintf("%s/%s", name, mode), func(t *testing.T) {
				t.Run("minidyn", func(t *testing.T) {
					ctx := context.Background()
					client := newTracedSDKClient(t, setupMinidynEndpoint(t), &traceRecorder{})
					parityCreatePokemonTable(ctx, t, client)

					err := op(ctx, client, mode)
					require.Error(t, err)

					var apiErr smithy.APIError

					require.ErrorAs(t, err, &apiErr)
					require.Equal(t, "ValidationException", apiErr.ErrorCode())
					require.Equal(t, fmt.Sprintf(wantFormat, mode), apiErr.ErrorMessage())
				})

				t.Run("dynamodb-local", func(t *testing.T) {
					ctx := context.Background()
					client := newTracedSDKClient(t, setupDynamoDBLocalEndpoint(t), &traceRecorder{})
					parityCreatePokemonTable(ctx, t, client)

					// DynamoDB Local does not validate the value.
					require.NoError(t, op(ctx, client, mode))
				})
			})
		}
	}
}

// TestE2E_ConsumedCapacityEmptyContainers pins that an empty list or map is sized as a 3 byte
// container, not as NULL (1 byte): the item below is exactly 1024 bytes with a NULL attribute
// (1 WCU) and 1026 with an empty list or map (2 WCU).
func TestE2E_ConsumedCapacityEmptyContainers(t *testing.T) {
	attrName := strings.Repeat("a", 1020)

	values := map[string]dynamodbtypes.AttributeValue{
		"EmptyList": &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{}},
		"EmptyMap":  &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{}},
		"Null":      &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:              aws.String(parityPokemonTable),
					Item:                   map[string]dynamodbtypes.AttributeValue{"id": capacityStrValue("x"), attrName: value},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)
				t.Logf("%s = %#v", name, out.ConsumedCapacity)

				return exactCapacity(out.ConsumedCapacity)
			})
		})
	}
}

// TestE2E_ConsumedCapacityIndexEntryUnchanged pins that a write is billed to a secondary index
// only when the index entry changes. The DynamoDB developer guide ("Write capacity units" in
// "Using Global Secondary Indexes") says that when a table update "changes only attributes that
// are neither index key attributes nor projected into the index, the index entry is unchanged, so
// no write capacity is consumed for that index. The base table is still charged for the write."
// DynamoDB Local agrees, so this is plain parity.
func TestE2E_ConsumedCapacityIndexEntryUnchanged(t *testing.T) {
	seed := func(ctx context.Context, t *testing.T, client *dynamodb.Client) {
		t.Helper()
		parityCreateIndexProjectionTable(ctx, t, client)

		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(parityIndexProjTable),
			Item:      indexProjectionItem("o", "t"),
		})
		require.NoError(t, err)
	}

	update := func(attr string) func(context.Context, *dynamodb.Client) (*dynamodbtypes.ConsumedCapacity, error) {
		return func(ctx context.Context, client *dynamodb.Client) (*dynamodbtypes.ConsumedCapacity, error) {
			out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(parityIndexProjTable), Key: capacityKey("a"),
				UpdateExpression:          aws.String("SET #a = :v"),
				ExpressionAttributeNames:  map[string]string{"#a": attr},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": capacityStrValue("new")},
				ReturnConsumedCapacity:    dynamodbtypes.ReturnConsumedCapacityIndexes,
			})
			if err != nil {
				return nil, err
			}

			return out.ConsumedCapacity, nil
		}
	}

	put := func(other, title string) func(context.Context, *dynamodb.Client) (*dynamodbtypes.ConsumedCapacity, error) {
		return func(ctx context.Context, client *dynamodb.Client) (*dynamodbtypes.ConsumedCapacity, error) {
			out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(parityIndexProjTable), Item: indexProjectionItem(other, title),
				ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
			})
			if err != nil {
				return nil, err
			}

			return out.ConsumedCapacity, nil
		}
	}

	cases := map[string]func(context.Context, *dynamodb.Client) (*dynamodbtypes.ConsumedCapacity, error){
		"UpdateNonProjectedAttribute": update("other"),
		"UpdateIncludedAttribute":     update("title"),
		"UpdateIndexKey":              update("gsi_pk"),
		"PutChangesNonProjected":      put("changed", "t"),
		"PutChangesIncluded":          put("o", "changed"),
		"PutIdenticalItem":            put("o", "t"),
	}

	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				seed(ctx, t, client)

				cc, err := run(ctx, client)
				require.NoError(t, err)

				return detailCapacity(cc)
			})
		})
	}
}

// indexProjectionItem is an item of the index projection table, indexed by both of its GSIs
// (keys-only-gsi projects nothing else, include-gsi projects title).
func indexProjectionItem(other, title string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"id": capacityStrValue("a"), "gsi_pk": capacityStrValue("g"),
		"title": capacityStrValue(title), "other": capacityStrValue(other),
	}
}

// TestE2E_ConsumedCapacityConditionCheck pins an intentional divergence from DynamoDB Local. A
// transactional ConditionCheck on a 20 KB item is billed by minidyn as a write of the key: the
// "Capacity management for transactions" section of the developer guide says only that "DynamoDB
// performs two underlying reads or writes of every item in the transaction", without saying which
// a ConditionCheck is, so minidyn keeps its approximation of 2 x 1 WCU for the key. DynamoDB Local
// bills 2 x the read units of the checked item (2 x 5 RCU).
func TestE2E_ConsumedCapacityConditionCheck(t *testing.T) {
	check := func(t *testing.T, client *dynamodb.Client) float64 {
		t.Helper()
		ctx := context.Background()
		parityCreatePokemonTable(ctx, t, client)
		capacityBigItem(ctx, t, client, "big", 20000)

		out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
			TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
				TableName: aws.String(parityPokemonTable), Key: capacityKey("big"),
				ConditionExpression: aws.String("attribute_exists(id)"),
			}}},
		})
		require.NoError(t, err)
		require.Len(t, out.ConsumedCapacity, 1)

		return aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits)
	}

	t.Run("minidyn", func(t *testing.T) {
		require.Equal(t, 2.0, check(t, newTracedSDKClient(t, setupMinidynEndpoint(t), &traceRecorder{})))
	})

	t.Run("dynamodb-local", func(t *testing.T) {
		require.Equal(t, 10.0, check(t, newTracedSDKClient(t, setupDynamoDBLocalEndpoint(t), &traceRecorder{})))
	})
}

// consumedFields is every number a ConsumedCapacity carries, with nil for a field the response
// leaves out, so the tests see which fields are reported and not only their values.
type consumedFields struct {
	Present bool
	Table   string
	Total   *float64
	Read    *float64
	Write   *float64
	OnTable *capacityFields
	GSI     map[string]capacityFields
}

type capacityFields struct{ Total, Read, Write *float64 }

func fieldsOf(c dynamodbtypes.Capacity) capacityFields {
	return capacityFields{c.CapacityUnits, c.ReadCapacityUnits, c.WriteCapacityUnits}
}

func consumedFieldsOf(cc *dynamodbtypes.ConsumedCapacity) consumedFields {
	if cc == nil {
		return consumedFields{}
	}

	f := consumedFields{
		Present: true, Table: aws.ToString(cc.TableName),
		Total: cc.CapacityUnits, Read: cc.ReadCapacityUnits, Write: cc.WriteCapacityUnits,
	}

	if cc.Table != nil {
		tf := fieldsOf(*cc.Table)
		f.OnTable = &tf
	}

	for name, c := range cc.GlobalSecondaryIndexes {
		if f.GSI == nil {
			f.GSI = map[string]capacityFields{}
		}

		f.GSI[name] = fieldsOf(c)
	}

	return f
}

// TestE2E_ConsumedCapacityReportedFields pins which ConsumedCapacity fields each operation
// reports. The ConsumedCapacity API reference lists ReadCapacityUnits and WriteCapacityUnits as
// optional, and the sample response in the Query reference for ReturnConsumedCapacity=TOTAL is
// {"CapacityUnits": 1, "TableName": "Reply"}. DynamoDB Local never reports them, at the top level
// or inside Table and the index entries, so neither does minidyn.
func TestE2E_ConsumedCapacityReportedFields(t *testing.T) {
	for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{
		dynamodbtypes.ReturnConsumedCapacityTotal, dynamodbtypes.ReturnConsumedCapacityIndexes,
	} {
		t.Run(string(mode), func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				table := aws.String(parityPokemonTable)
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "b"})

				put, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: table, ReturnConsumedCapacity: mode,
					Item: parityMarshalPokemon(t, parityPokemon{ID: "002", Type: "fire", Name: "c"}),
				})
				require.NoError(t, err)

				get, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: table, Key: capacityKey("001"), ReturnConsumedCapacity: mode})
				require.NoError(t, err)

				query, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName: table, IndexName: aws.String(parityByTypeIndexName), ReturnConsumedCapacity: mode,
					KeyConditionExpression:    aws.String("#t = :t"),
					ExpressionAttributeNames:  map[string]string{"#t": "type"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":t": capacityStrValue("grass")},
				})
				require.NoError(t, err)

				scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: table, ReturnConsumedCapacity: mode})
				require.NoError(t, err)

				batchWrite, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					ReturnConsumedCapacity: mode,
					RequestItems: map[string][]dynamodbtypes.WriteRequest{parityPokemonTable: {
						{PutRequest: &dynamodbtypes.PutRequest{Item: parityMarshalPokemon(t, parityPokemon{ID: "003", Type: "x", Name: "d"})}},
					}},
				})
				require.NoError(t, err)

				batchGet, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
					ReturnConsumedCapacity: mode,
					RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
						parityPokemonTable: {Keys: []map[string]dynamodbtypes.AttributeValue{capacityKey("001")}},
					},
				})
				require.NoError(t, err)

				transactGet, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					ReturnConsumedCapacity: mode,
					TransactItems:          []dynamodbtypes.TransactGetItem{{Get: &dynamodbtypes.Get{TableName: table, Key: capacityKey("001")}}},
				})
				require.NoError(t, err)

				transactWrite, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: mode,
					TransactItems: []dynamodbtypes.TransactWriteItem{{Put: &dynamodbtypes.Put{
						TableName: table, Item: parityMarshalPokemon(t, parityPokemon{ID: "004", Type: "y", Name: "e"}),
					}}},
				})
				require.NoError(t, err)

				return []consumedFields{
					consumedFieldsOf(put.ConsumedCapacity), consumedFieldsOf(get.ConsumedCapacity),
					consumedFieldsOf(query.ConsumedCapacity), consumedFieldsOf(scan.ConsumedCapacity),
					consumedFieldsOf(&batchWrite.ConsumedCapacity[0]), consumedFieldsOf(&batchGet.ConsumedCapacity[0]),
					consumedFieldsOf(&transactGet.ConsumedCapacity[0]), consumedFieldsOf(&transactWrite.ConsumedCapacity[0]),
				}
			})
		})
	}
}
