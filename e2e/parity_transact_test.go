package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestE2E_TransactWrite(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "TransactPut",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id":   &dynamodbtypes.AttributeValueMemberS{Value: "001"},
								"name": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
							},
						}},
					},
				})
				require.NoError(t, err)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
				})
				require.NoError(t, err)

				return []any{out.Item["id"], out.Item["name"]}
			},
		},
		{
			name: "TransactUpdate",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Update: &dynamodbtypes.Update{
							TableName:        aws.String(parityPokemonTable),
							Key:              map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression: aws.String("SET second_type = :stype"),
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
								":stype": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
							},
						}},
					},
				})
				require.NoError(t, err)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
				})
				require.NoError(t, err)

				return out.Item["second_type"]
			},
		},
		{
			name: "TransactDelete",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Delete: &dynamodbtypes.Delete{
							TableName: aws.String(parityPokemonTable),
							Key: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
							},
						}},
					},
				})
				require.NoError(t, err)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
				})
				require.NoError(t, err)

				return len(out.Item) == 0
			},
		},
		{
			name: "TransactConditionCheckPass",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(parityPokemonTable),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						}},
					},
				})
				require.NoError(t, err)

				return true
			},
		},
		{
			name: "TransactConditionCheckFail",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(parityPokemonTable),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "999"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						}},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "TransactRollbackOnFailure",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id":   &dynamodbtypes.AttributeValueMemberS{Value: "rollback-me"},
								"type": &dynamodbtypes.AttributeValueMemberS{Value: "fire"},
							},
						}},
						{ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(parityPokemonTable),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "non-existent"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						}},
					},
				})
				require.Error(t, err)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "rollback-me"},
					},
				})
				require.NoError(t, err)

				return len(out.Item) == 0
			},
		},
		{
			name: "TransactRollbackAcrossTables",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				const secondTable = "transact-items"

				_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String(secondTable),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
					},
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					BillingMode: dynamodbtypes.BillingModePayPerRequest,
				})
				require.NoError(t, err)

				_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(secondTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "should-rollback"},
							},
						}},
						{ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(parityPokemonTable),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "non-existent"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						}},
					},
				})
				require.Error(t, err)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(secondTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "should-rollback"},
					},
				})
				require.NoError(t, err)

				return len(out.Item) == 0
			},
		},
		{
			name: "TransactConditionCheckReturnAllOld",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:                           aws.String(parityPokemonTable),
							Key:                                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression:                 aws.String("attribute_not_exists(id)"),
							ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
						}},
					},
				})
				require.Error(t, err)

				var tce *dynamodbtypes.TransactionCanceledException
				require.ErrorAs(t, err, &tce)
				require.Len(t, tce.CancellationReasons, 1)

				reason := tce.CancellationReasons[0]
				idAttr, _ := reason.Item["id"].(*dynamodbtypes.AttributeValueMemberS)
				require.NotNil(t, idAttr, "CancellationReasons[0].Item must contain the old item")

				return idAttr.Value
			},
		},
		{
			name: "TransactDuplicateItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
							},
						}},
						{Update: &dynamodbtypes.Update{
							TableName:                 aws.String(parityPokemonTable),
							Key:                       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression:          aws.String("SET #n = :n"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}},
						}},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "TransactPutConditionFail",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName:           aws.String(parityPokemonTable),
							Item:                map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("attribute_not_exists(id)"),
						}},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "TransactDeleteConditionFail",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Delete: &dynamodbtypes.Delete{
							TableName:           aws.String(parityPokemonTable),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("attribute_not_exists(id)"),
						}},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "TransactUnusedExpressionAttribute",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
							},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
								":unused": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
							},
						}},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}

func TestE2E_TransactGet(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "TransactGetSingleItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{
							TableName: aws.String(parityPokemonTable),
							Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
						}},
					},
				})
				require.NoError(t, err)

				return out.Responses[0].Item
			},
		},
		{
			name: "TransactGetPreservesOrder",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "002", Type: "fire", Name: "Charmander"})

				out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{TableName: aws.String(parityPokemonTable), Key: map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "002"}}}},
						{Get: &dynamodbtypes.Get{TableName: aws.String(parityPokemonTable), Key: map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}}},
					},
				})
				require.NoError(t, err)

				return []any{out.Responses[0].Item["id"], out.Responses[1].Item["id"]}
			},
		},
		{
			name: "TransactGetMissingItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{TableName: aws.String(parityPokemonTable), Key: map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "999"}}}},
					},
				})
				require.NoError(t, err)

				return []any{len(out.Responses), len(out.Responses[0].Item)}
			},
		},
		{
			name: "TransactGetProjectionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{
							TableName:                aws.String(parityPokemonTable),
							Key:                      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ProjectionExpression:     aws.String("#n"),
							ExpressionAttributeNames: map[string]string{"#n": "name"},
						}},
					},
				})
				require.NoError(t, err)

				return out.Responses[0].Item
			},
		},
		{
			name: "TransactGetDuplicateItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{TableName: aws.String(parityPokemonTable), Key: map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}}},
						{Get: &dynamodbtypes.Get{TableName: aws.String(parityPokemonTable), Key: map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}}},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "TransactGetCrossTables",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				const ordersTable = "orders"
				const inventoryTable = "inventory"

				for _, spec := range []struct {
					name, hashKey string
				}{
					{ordersTable, "id"},
					{inventoryTable, "id"},
				} {
					_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
						TableName: aws.String(spec.name),
						KeySchema: []dynamodbtypes.KeySchemaElement{
							{AttributeName: aws.String(spec.hashKey), KeyType: dynamodbtypes.KeyTypeHash},
						},
						AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
							{AttributeName: aws.String(spec.hashKey), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
						},
						BillingMode: dynamodbtypes.BillingModePayPerRequest,
					})
					require.NoError(t, err)
				}

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(ordersTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id":     &dynamodbtypes.AttributeValueMemberS{Value: "1"},
						"status": &dynamodbtypes.AttributeValueMemberS{Value: "shipped"},
					},
				})
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(inventoryTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id":  &dynamodbtypes.AttributeValueMemberS{Value: "1"},
						"qty": &dynamodbtypes.AttributeValueMemberN{Value: "10"},
					},
				})
				require.NoError(t, err)

				out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{
							TableName: aws.String(ordersTable),
							Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
						}},
						{Get: &dynamodbtypes.Get{
							TableName: aws.String(inventoryTable),
							Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
						}},
					},
				})
				require.NoError(t, err)

				return []any{
					out.Responses[0].Item["status"],
					out.Responses[1].Item["qty"],
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
