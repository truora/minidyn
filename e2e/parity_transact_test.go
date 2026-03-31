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
