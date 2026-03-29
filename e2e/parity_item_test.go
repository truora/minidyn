package e2e

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestE2E_Item(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "PutAndGetItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				item := parityMarshalPokemon(t, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      item,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
				})
				require.NoError(t, err)

				_, err = client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key:       map[string]dynamodbtypes.AttributeValue{},
				})
				require.Error(t, err)

				return []any{
					out.Item["id"],
					out.Item["name"],
					out.Item["type"],
					normalizeSDKErrorString(err.Error()),
				}
			},
		},
		{
			name: "PutAndGetBatchItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				t.Skip("skipping PutAndGetBatchItem test until we implement it")
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				opt := func(o *attributevalue.EncoderOptions) {
					o.TagKey = "json"
				}

				item, err := attributevalue.MarshalMapWithOptions(parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				}, opt)
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      item,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				item, err = attributevalue.MarshalMapWithOptions(parityPokemon{
					ID:   "002",
					Type: "fire",
					Name: "Sharmander",
				}, opt)
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      item,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
					RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
						parityPokemonTable: {
							Keys: []map[string]dynamodbtypes.AttributeValue{
								{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
								{"id": &dynamodbtypes.AttributeValueMemberS{Value: "002"}},
								{"t1": &dynamodbtypes.AttributeValueMemberS{Value: "003"}},
								{"id": &dynamodbtypes.AttributeValueMemberS{Value: "004"}},
							},
						},
					},
				})
				require.NoError(t, err)

				return []any{
					paritySortedBatchGetIDs(out, parityPokemonTable),
					paritySortedUnprocessedKeySignatures(out, parityPokemonTable),
				}
			},
		},
		{
			name: "PutWithGSI",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				var steps []string

				item := map[string]dynamodbtypes.AttributeValue{
					"id":   &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					"name": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
					"type": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
				}

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      item,
					TableName: aws.String(parityPokemonTable),
				})
				require.Error(t, err)

				steps = append(steps, normalizeSDKErrorString(err.Error()))

				delete(item, "type")

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      item,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)
				steps = append(steps, "put_without_type_ok")

				parityAddSortBySecondTypeIndex(ctx, t, client)

				item2 := parityMarshalPokemon(t, parityPokemon{
					ID:         "002",
					Name:       "Ivysaur",
					Type:       "grass",
					SecondType: "poison",
				})

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      item2,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)
				steps = append(steps, "put_ivysaur_ok")

				return steps
			},
		},
		{
			name: "GetItemWithUnusedAttributes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ExpressionAttributeNames: map[string]string{
						"#name": "name",
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "GetItemWithInvalidExpressionAttributeNames",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ProjectionExpression: aws.String("#name-1"),
					ExpressionAttributeNames: map[string]string{
						"#name-1": "name",
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "GetItemWithProjectionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ProjectionExpression: aws.String("#n"),
					ExpressionAttributeNames: map[string]string{
						"#n": "name",
					},
				})
				require.NoError(t, err)
				require.Len(t, out.Item, 1)

				name, ok := out.Item["name"].(*dynamodbtypes.AttributeValueMemberS)
				require.True(t, ok)

				return name.Value
			},
		},
		{
			name: "GetItemWithProjectionInvalidExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ProjectionExpression: aws.String("("),
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "PutItemConditionExpressionFirstPut",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				item := parityMarshalPokemon(t, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:                item,
					TableName:           aws.String(parityPokemonTable),
					ConditionExpression: aws.String("attribute_not_exists(#type)"),
					ExpressionAttributeNames: map[string]string{
						"#type": "type",
					},
				})
				require.NoError(t, err)

				return "ok"
			},
		},
		{
			name: "PutItemUnusedExpressionAttributeValues",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				p := parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:      parityMarshalPokemon(t, p),
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					Item:                parityMarshalPokemon(t, p),
					TableName:           aws.String(parityPokemonTable),
					ConditionExpression: aws.String("attribute_exists(id)"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":not_used": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "PutItemConditionInvalidAttributeName",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					Item: parityMarshalPokemon(t, parityPokemon{
						ID:   "001",
						Type: "grass",
						Name: "Bulbasaur",
					}),
					TableName:           aws.String(parityPokemonTable),
					ConditionExpression: aws.String("attribute_not_exists(#invalid-name)"),
					ExpressionAttributeNames: map[string]string{
						"#invalid-name": "hello",
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "PutItemConditionInvalidAttributeValue",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					Item: parityMarshalPokemon(t, parityPokemon{
						ID:   "001",
						Type: "grass",
						Name: "Bulbasaur",
					}),
					TableName:           aws.String(parityPokemonTable),
					ConditionExpression: aws.String("#valid_name = :invalid-value"),
					ExpressionAttributeNames: map[string]string{
						"#valid_name": "hello",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "UpdateItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				expr := map[string]dynamodbtypes.AttributeValue{
					":ntype": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
				}
				input := &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression:          aws.String("SET second_type = :ntype"),
					ExpressionAttributeValues: expr,
				}

				_, err := client.UpdateItem(ctx, input)
				require.NoError(t, err)

				item := parityGetPokemon(ctx, t, client, "001")
				st := item["second_type"]

				input.Key["id"] = &dynamodbtypes.AttributeValueMemberS{Value: "404"}

				_, err = client.UpdateItem(ctx, input)
				require.NoError(t, err)

				item404 := parityGetPokemon(ctx, t, client, "404")
				st404 := item404["second_type"]

				return []any{st, st404}
			},
		},
		{
			name: "UpdateItemWithConditionalExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "002",
					Type: "grass",
					Name: "Ivysaur",
				})

				uexpr := "SET second_type = :ntype"
				expr := map[string]dynamodbtypes.AttributeValue{
					":ntype": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
				}

				input := &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "404"},
					},
					ConditionExpression:       aws.String("attribute_exists(id)"),
					ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression:          aws.String(uexpr),
					ExpressionAttributeValues: expr,
					ExpressionAttributeNames: map[string]string{
						"#id": "id",
					},
				}

				_, err := client.UpdateItem(ctx, input)
				require.Error(t, err)
				msgs := []string{normalizeSDKErrorString(err.Error())}

				input.ConditionExpression = aws.String("attribute_exists(#invalid-name)")
				input.ExpressionAttributeNames = map[string]string{
					"#invalid-name": "type",
				}

				_, err = client.UpdateItem(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				input.ConditionExpression = aws.String("#t = :invalid-value")
				input.ExpressionAttributeNames = map[string]string{
					"#t": "type",
				}
				input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
					":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
				}

				_, err = client.UpdateItem(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				input = &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "404"},
					},
					ConditionExpression:       aws.String("attribute_exists(#id)"),
					ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression:          aws.String(uexpr),
					ExpressionAttributeValues: expr,
					ExpressionAttributeNames: map[string]string{
						"#id": "id",
					},
				}

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				_, err = client.UpdateItem(ctx, input)
				require.True(t, errors.As(err, &ccf))

				input = &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ConditionExpression:                 aws.String("attribute_not_exists(#id)"),
					ReturnValues:                        dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression:                    aws.String(uexpr),
					ExpressionAttributeValues:           expr,
					ExpressionAttributeNames:            map[string]string{"#id": "id"},
					ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
				}

				ccf = &dynamodbtypes.ConditionalCheckFailedException{}

				_, err = client.UpdateItem(ctx, input)
				require.True(t, errors.As(err, &ccf))
				require.NotEmpty(t, ccf.Item)

				return []any{
					msgs,
					ccf.Item["id"],
					ccf.Item["name"],
				}
			},
		},
		{
			name: "UpdateItemWithGSI",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				items := parityGetPokemonsByType(ctx, t, client, "grass")
				before := len(items)
				require.Len(t, items, 1)

				input := &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression: aws.String("SET #type = :ntype"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":ntype": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
					},
					ExpressionAttributeNames: map[string]string{"#type": "type"},
				}

				_, err := client.UpdateItem(ctx, input)
				require.NoError(t, err)

				grass := len(parityGetPokemonsByType(ctx, t, client, "grass"))
				poison := len(parityGetPokemonsByType(ctx, t, client, "poison"))

				return []int{before, grass, poison}
			},
		},
		{
			name: "UpdateItemError",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"foo": &dynamodbtypes.AttributeValueMemberS{Value: "a"},
					},
					ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression: aws.String("SET second_type = :second_type"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":second_type": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "UpdateExpressions_add",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID: "001", Type: "grass", Name: "Bulbasaur", SecondType: "type",
					Moves: []string{"Growl", "Tackle", "Vine Whip", "Growth"},
					Local: []string{"001 (Red/Blue/Yellow)"},
				})

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression: aws.String("ADD lvl :one"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":one": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				})
				require.NoError(t, err)

				item := parityGetPokemon(ctx, t, client, "001")
				lvl, ok := item["lvl"].(*dynamodbtypes.AttributeValueMemberN)
				require.True(t, ok)

				return lvl.Value
			},
		},
		{
			name: "UpdateExpressions_remove",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID: "001", Type: "grass", Name: "Bulbasaur", SecondType: "type",
					Moves: []string{"Growl", "Tackle", "Vine Whip", "Growth"},
					Local: []string{
						"001 (Red/Blue/Yellow)",
						"226 (Gold/Silver/Crystal)",
						"001 (FireRed/LeafGreen)",
						"001 (Let's Go Pikachu/Let's Go Eevee)",
					},
				})

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression: aws.String("REMOVE #l[0],#l[1],#l[2],#l[3]"),
					ExpressionAttributeNames: map[string]string{
						"#l": "local",
					},
				})
				require.NoError(t, err)

				item := parityGetPokemon(ctx, t, client, "001")
				local, ok := item["local"].(*dynamodbtypes.AttributeValueMemberNULL)

				return ok && local.Value
			},
		},
		{
			name: "UpdateExpressions_delete",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID: "001", Type: "grass", Name: "Bulbasaur", SecondType: "type",
					Moves: []string{"Growl", "Tackle", "Vine Whip", "Growth"},
					Local: []string{"001 (Red/Blue/Yellow)"},
				})

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
					UpdateExpression: aws.String("DELETE moves :move"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":move": &dynamodbtypes.AttributeValueMemberSS{Value: []string{"Growl"}},
					},
				})
				require.NoError(t, err)

				item := parityGetPokemon(ctx, t, client, "001")
				moves, ok := item["moves"].(*dynamodbtypes.AttributeValueMemberSS)
				require.True(t, ok)

				cp := append([]string(nil), moves.Value...)
				sort.Strings(cp)

				return strings.Join(cp, ",")
			},
		},
		{
			name: "DeleteItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "002", Type: "grass", Name: "Ivysaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "003", Type: "grass", Name: "Venusaur"})

				key := map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{Value: "003"},
				}

				items := parityGetPokemonsByType(ctx, t, client, "grass")
				require.Len(t, items, 3)

				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					Key:       key,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				gone := parityGetPokemon(ctx, t, client, "003")
				afterFirst := len(parityGetPokemonsByType(ctx, t, client, "grass"))

				_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					Key:       key,
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				afterSecond := len(parityGetPokemonsByType(ctx, t, client, "grass"))

				return []any{len(gone), afterFirst, afterSecond}
			},
		},
		{
			name: "DeleteItemWithConditions",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				input := &dynamodb.DeleteItemInput{
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					TableName:           aws.String(parityPokemonTable),
					ConditionExpression: aws.String("attribute_exists(id)"),
				}

				_, err := client.DeleteItem(ctx, input)
				require.NoError(t, err)

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				_, err = client.DeleteItem(ctx, input)
				require.True(t, errors.As(err, &ccf))

				msgs := []string{"ccf_after_delete"}

				input.ExpressionAttributeNames = map[string]string{
					"#not_used": "hello",
				}

				_, err = client.DeleteItem(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				input.ConditionExpression = aws.String("#invalid-name = Squirtle")
				input.ExpressionAttributeNames = map[string]string{
					"#invalid-name": "hello",
				}

				_, err = client.DeleteItem(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				input.ConditionExpression = aws.String("#t = :invalid-value")
				input.ExpressionAttributeNames = map[string]string{
					"#t": "type",
				}
				input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
					":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
				}

				_, err = client.DeleteItem(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				return msgs
			},
		},
		{
			name: "DeleteItemWithReturnValues",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				})

				output, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					TableName:    aws.String(parityPokemonTable),
					ReturnValues: dynamodbtypes.ReturnValueAllOld,
				})
				require.NoError(t, err)

				name, ok := output.Attributes["name"].(*dynamodbtypes.AttributeValueMemberS)
				require.True(t, ok)

				return name
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
