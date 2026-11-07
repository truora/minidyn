package e2e

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// paritySortedIDs returns sorted "id" values from Scan/Query items (Scan order is undefined).
func paritySortedIDs(items []map[string]dynamodbtypes.AttributeValue) []string {
	ids := make([]string, 0, len(items))

	for _, it := range items {
		if v, ok := it["id"].(*dynamodbtypes.AttributeValueMemberS); ok {
			ids = append(ids, v.Value)
		}
	}

	sort.Strings(ids)

	return ids
}

func paritySeedStarters(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	parityCreatePokemonTable(ctx, t, client)
	parityCreatePokemon(ctx, t, client, parityPokemon{
		ID: "001", Type: "grass", SecondType: "poison", Name: "Bulbasaur", Level: 5,
		Moves: []string{"tackle", "vine whip"}, Local: []string{"kanto"},
	})
	parityCreatePokemon(ctx, t, client, parityPokemon{
		ID: "004", Type: "fire", Name: "Charmander", Level: 12,
		Moves: []string{"scratch", "ember"}, Local: []string{"kanto", "johto"},
	})
	parityCreatePokemon(ctx, t, client, parityPokemon{
		ID: "007", Type: "water", Name: "Squirtle", Level: 30,
		Moves: []string{"tackle"},
	})
}

func TestE2E_ItemValidation(t *testing.T) {
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "PutItemMissingKey",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"name": &dynamodbtypes.AttributeValueMemberS{Value: "Missingno"},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "PutItemWrongKeyType",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "GetItemWrongKeyType",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "PutItemInvalidReturnValues",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				errs := map[string]string{}

				for _, rv := range []dynamodbtypes.ReturnValue{
					dynamodbtypes.ReturnValueAllNew,
					dynamodbtypes.ReturnValueUpdatedOld,
					"BOGUS",
				} {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(parityPokemonTable),
						Item: map[string]dynamodbtypes.AttributeValue{
							"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
						},
						ReturnValues: rv,
					})
					require.Error(t, err)

					errs[string(rv)] = normalizeSDKErrorString(err.Error())
				}

				return errs
			},
		},
		{
			name: "DeleteItemInvalidReturnValues",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				errs := map[string]string{}

				for _, rv := range []dynamodbtypes.ReturnValue{
					dynamodbtypes.ReturnValueAllNew,
					dynamodbtypes.ReturnValueUpdatedOld,
					dynamodbtypes.ReturnValueUpdatedNew,
					"BOGUS",
				} {
					_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
						TableName: aws.String(parityPokemonTable),
						Key: map[string]dynamodbtypes.AttributeValue{
							"id": &dynamodbtypes.AttributeValueMemberS{Value: "007"},
						},
						ReturnValues: rv,
					})
					require.Error(t, err)

					errs[string(rv)] = normalizeSDKErrorString(err.Error())
				}

				return []any{errs, len(parityGetPokemon(ctx, t, client, "007")) > 0}
			},
		},
		{
			name: "DeleteItemInvalidReturnValuesMissingItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "does-not-exist"},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "DeleteItemInvalidReturnValuesFailingCondition",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "007"},
					},
					ConditionExpression: aws.String("#t = :t"),
					ExpressionAttributeNames: map[string]string{
						"#t": "type",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":t": &dynamodbtypes.AttributeValueMemberS{Value: "fire"},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.Error(t, err)

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				return []any{errors.As(err, &ccf), normalizeSDKErrorString(err.Error()), len(parityGetPokemon(ctx, t, client, "007")) > 0}
			},
		},
		{
			name: "DeleteItemInvalidReturnValuesInvalidKey",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"nope": &dynamodbtypes.AttributeValueMemberS{Value: "007"},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "GetItemTableNotFound",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String("does-not-exist"),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
				})
				require.Error(t, err)

				var notFound *dynamodbtypes.ResourceNotFoundException

				return errors.As(err, &notFound)
			},
		},
		{
			name: "PutItemConditionalCheckFailed",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:           aws.String(parityPokemonTable),
					Item:                parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "grass", Name: "Clone"}),
					ConditionExpression: aws.String("attribute_not_exists(id)"),
				})
				require.Error(t, err)

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				return []any{errors.As(err, &ccf), normalizeSDKErrorString(err.Error()), parityGetPokemon(ctx, t, client, "001")}
			},
		},
		{
			name: "PutItemReturnValuesAllOld",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				first, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:    aws.String(parityPokemonTable),
					Item:         parityMarshalPokemon(t, parityPokemon{ID: "025", Type: "electric", Name: "Pikachu"}),
					ReturnValues: dynamodbtypes.ReturnValueAllOld,
				})
				require.NoError(t, err)

				second, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:    aws.String(parityPokemonTable),
					Item:         parityMarshalPokemon(t, parityPokemon{ID: "004", Type: "fire", Name: "Charmeleon", Level: 16}),
					ReturnValues: dynamodbtypes.ReturnValueAllOld,
				})
				require.NoError(t, err)

				return []any{len(first.Attributes), parityNormalizeItemSets(second.Attributes)}
			},
		},
		{
			name: "UpdateItemArithmeticListAppendIfNotExists",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "004"},
					},
					UpdateExpression: aws.String(
						"SET lvl = lvl + :inc, #local = list_append(#local, :regions), " +
							"evolves_at = if_not_exists(evolves_at, :evo), #n = if_not_exists(#n, :name)",
					),
					ExpressionAttributeNames: map[string]string{
						"#local": "local",
						"#n":     "name",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":inc": &dynamodbtypes.AttributeValueMemberN{Value: "4"},
						":regions": &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{
							&dynamodbtypes.AttributeValueMemberS{Value: "hoenn"},
						}},
						":evo":  &dynamodbtypes.AttributeValueMemberN{Value: "16"},
						":name": &dynamodbtypes.AttributeValueMemberS{Value: "Ignored"},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.NoError(t, err)

				return parityNormalizeItemSets(out.Attributes)
			},
		},
		{
			name: "UpdateItemReturnValuesUpdatedOldAndNew",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				key := map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{Value: "007"},
				}

				oldOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(parityPokemonTable),
					Key:              key,
					UpdateExpression: aws.String("SET lvl = :lvl, nickname = :nick"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":lvl":  &dynamodbtypes.AttributeValueMemberN{Value: "31"},
						":nick": &dynamodbtypes.AttributeValueMemberS{Value: "Squirt"},
					},
					ReturnValues: dynamodbtypes.ReturnValueUpdatedOld,
				})
				require.NoError(t, err)

				newOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(parityPokemonTable),
					Key:              key,
					UpdateExpression: aws.String("SET lvl = lvl + :one REMOVE nickname"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":one": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
					ReturnValues: dynamodbtypes.ReturnValueUpdatedNew,
				})
				require.NoError(t, err)

				return []any{oldOut.Attributes, newOut.Attributes}
			},
		},
		{
			name: "UpdateItemRemoveTopLevelAttributes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				key := map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
				}

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(parityPokemonTable),
					Key:              key,
					UpdateExpression: aws.String("SET lease_owner = :o, lease_until = :u"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":o": &dynamodbtypes.AttributeValueMemberS{Value: "worker-1"},
						":u": &dynamodbtypes.AttributeValueMemberN{Value: "1700000000"},
					},
				})
				require.NoError(t, err)

				removeOnly, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(parityPokemonTable),
					Key:              key,
					UpdateExpression: aws.String("REMOVE lease_owner, lease_until"),
					ReturnValues:     dynamodbtypes.ReturnValueAllNew,
				})
				require.NoError(t, err)

				mixed, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(parityPokemonTable),
					Key:              key,
					UpdateExpression: aws.String("SET lvl = :lvl REMOVE second_type, #n"),
					ExpressionAttributeNames: map[string]string{
						"#n": "name",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":lvl": &dynamodbtypes.AttributeValueMemberN{Value: "6"},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.NoError(t, err)

				return []any{
					parityNormalizeItemSets(removeOnly.Attributes),
					parityNormalizeItemSets(mixed.Attributes),
					parityNormalizeItemSets(parityGetPokemon(ctx, t, client, "001")),
				}
			},
		},
		{
			name: "UpdateItemConditionalCheckFailed",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					UpdateExpression:    aws.String("SET lvl = :lvl"),
					ConditionExpression: aws.String("lvl > :min"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":lvl": &dynamodbtypes.AttributeValueMemberN{Value: "100"},
						":min": &dynamodbtypes.AttributeValueMemberN{Value: "10"},
					},
				})
				require.Error(t, err)

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				return []any{errors.As(err, &ccf), normalizeSDKErrorString(err.Error()), parityGetPokemon(ctx, t, client, "001")}
			},
		},
		{
			name:       "ScanFilterFunctions",
			knownIssue: "#143",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				filters := []struct {
					expr   string
					values map[string]dynamodbtypes.AttributeValue
				}{
					{"contains(moves, :v)", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "tackle"},
					}},
					{"contains(#local, :v)", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "johto"},
					}},
					{"contains(#n, :v)", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "rt"},
					}},
					{"begins_with(#n, :v)", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "Char"},
					}},
					{"#t IN (:a, :b)", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "fire"},
						":b": &dynamodbtypes.AttributeValueMemberS{Value: "water"},
					}},
					{"lvl BETWEEN :a AND :b", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberN{Value: "5"},
						":b": &dynamodbtypes.AttributeValueMemberN{Value: "12"},
					}},
					{"size(moves) > :v", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					}},
					{"attribute_type(#local, :v)", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "L"},
					}},
					{"NOT (#t = :a) AND (lvl < :b OR #n = :c)", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
						":b": &dynamodbtypes.AttributeValueMemberN{Value: "20"},
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "Squirtle"},
					}},
					{"second_type <> :v", map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
					}},
				}

				results := make([][]string, 0, len(filters))

				for _, f := range filters {
					names := map[string]string{}
					for _, alias := range []string{"#local", "#n", "#t"} {
						if strings.Contains(f.expr, alias) {
							names[alias] = map[string]string{"#local": "local", "#n": "name", "#t": "type"}[alias]
						}
					}

					input := &dynamodb.ScanInput{
						TableName:                 aws.String(parityPokemonTable),
						FilterExpression:          aws.String(f.expr),
						ExpressionAttributeValues: f.values,
					}
					if len(names) > 0 {
						input.ExpressionAttributeNames = names
					}

					out, err := client.Scan(ctx, input)
					require.NoError(t, err, f.expr)

					results = append(results, paritySortedIDs(out.Items))
				}

				return results
			},
		},
		{
			name: "ScanSelectCount",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(parityPokemonTable),
					Select:           dynamodbtypes.SelectCount,
					FilterExpression: aws.String("lvl >= :min"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":min": &dynamodbtypes.AttributeValueMemberN{Value: "10"},
					},
				})
				require.NoError(t, err)

				return []any{out.Count, out.ScannedCount, len(out.Items)}
			},
		},
		{
			name:       "ScanProjectionExpressionNested",
			knownIssue: "#143",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:            aws.String(parityPokemonTable),
					ProjectionExpression: aws.String("id, #local[0], #n"),
					ExpressionAttributeNames: map[string]string{
						"#local": "local",
						"#n":     "name",
					},
				})
				require.NoError(t, err)

				sort.Slice(out.Items, func(i, j int) bool {
					return parityAttrValueString(out.Items[i]["id"]) < parityAttrValueString(out.Items[j]["id"])
				})

				return out.Items
			},
		},
		{
			name: "DeleteItemConditionalCheckFailed",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "007"},
					},
					ConditionExpression: aws.String("#t = :t"),
					ExpressionAttributeNames: map[string]string{
						"#t": "type",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":t": &dynamodbtypes.AttributeValueMemberS{Value: "fire"},
					},
				})
				require.Error(t, err)

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				return []any{errors.As(err, &ccf), normalizeSDKErrorString(err.Error()), len(parityGetPokemon(ctx, t, client, "007")) > 0}
			},
		},
		{
			name: "UpdateItemDeleteInvalidOperand",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				deleteErr := func(attr string, v dynamodbtypes.AttributeValue) string {
					_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName: aws.String(parityPokemonTable),
						Key: map[string]dynamodbtypes.AttributeValue{
							"id": &dynamodbtypes.AttributeValueMemberS{Value: "004"},
						},
						UpdateExpression:          aws.String("DELETE " + attr + " :v"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": v},
					})
					require.Error(t, err)

					return normalizeSDKErrorString(err.Error())
				}

				return []any{
					deleteErr("moves", &dynamodbtypes.AttributeValueMemberS{Value: "ember"}),
					deleteErr("absent", &dynamodbtypes.AttributeValueMemberN{Value: "1"}),
					deleteErr("absent", &dynamodbtypes.AttributeValueMemberB{Value: []byte("b")}),
					deleteErr("moves", &dynamodbtypes.AttributeValueMemberBOOL{Value: true}),
					deleteErr("moves", &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{}}),
					deleteErr("absent", &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{}}),
					// A set of another type than the target's string set. minidyn adds debug detail
					// after " | ", which the normalizer strips.
					deleteErr("moves", &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}}),
					deleteErr("moves", &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("b")}}),
					parityNormalizeItemSets(parityGetPokemon(ctx, t, client, "004")),
				}
			},
		},
		{
			name: "UpdateItemAddInvalidOperand",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				paritySeedStarters(ctx, t, client)

				// Item 004 has lvl (N) and moves (SS).
				addErr := func(attr string, v dynamodbtypes.AttributeValue) string {
					_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName: aws.String(parityPokemonTable),
						Key: map[string]dynamodbtypes.AttributeValue{
							"id": &dynamodbtypes.AttributeValueMemberS{Value: "004"},
						},
						UpdateExpression:          aws.String("ADD " + attr + " :v"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": v},
					})
					require.Error(t, err)

					return normalizeSDKErrorString(err.Error())
				}

				return []any{
					// Operands ADD never accepts.
					addErr("lvl", &dynamodbtypes.AttributeValueMemberS{Value: "x"}),
					addErr("absent", &dynamodbtypes.AttributeValueMemberBOOL{Value: true}),
					addErr("lvl", &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{}}),
					addErr("moves", &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{}}),
					// An allowed operand type that doesn't match the target.
					addErr("moves", &dynamodbtypes.AttributeValueMemberN{Value: "1"}),
					addErr("moves", &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}}),
					parityNormalizeItemSets(parityGetPokemon(ctx, t, client, "004")),
				}
			},
		},
		{
			name: "FunctionArgumentCount",
			fn:   parityFunctionArgumentCount,
		},
		{
			// ADD on a set needs a set operand of the same type, not one element (#191).
			name: "UpdateItemAddScalarOnSet",
			fn:   parityUpdateItemAddScalarOnSet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}

// parityFunctionArgumentCount sends built-in functions with too few and too many operands
// through every expression context DynamoDB validates them in.
func parityFunctionArgumentCount(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	// The table is empty, so the count must be rejected before any item is read.
	parityCreatePokemonTable(ctx, t, client)

	vals := map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"}}
	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "004"}}
	table := aws.String(parityPokemonTable)

	// Expressions use :v only when they mention it, as DynamoDB rejects unused values.
	withVals := func(expr string) map[string]dynamodbtypes.AttributeValue {
		if strings.Contains(expr, ":v") {
			return vals
		}

		return nil
	}

	errString := func(op string, err error) string {
		require.Error(t, err, "%s accepted the expression", op)

		return normalizeSDKErrorString(err.Error())
	}

	exprs := []string{
		"contains(a)", "contains(a, :v, :v)", "begins_with(a)", "begins_with(a, :v, :v)",
		"attribute_type(a)", "attribute_type(a, :v, :v)", "attribute_exists(a, a)",
		"attribute_not_exists(a, a)", "size(a, a) > :v",
	}

	out := []any{}

	for _, expr := range exprs {
		_, scanErr := client.Scan(ctx, &dynamodb.ScanInput{
			TableName: table, FilterExpression: aws.String(expr), ExpressionAttributeValues: withVals(expr),
		})
		_, putErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: table, Item: key, ConditionExpression: aws.String(expr),
			ExpressionAttributeValues: withVals(expr),
		})
		_, deleteErr := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: table, Key: key, ConditionExpression: aws.String(expr),
			ExpressionAttributeValues: withVals(expr),
		})
		_, updateErr := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: table, Key: key, ConditionExpression: aws.String(expr),
			UpdateExpression:          aws.String("SET b = :v"),
			ExpressionAttributeValues: vals,
		})

		out = append(out, expr,
			errString("Scan "+expr, scanErr), errString("PutItem "+expr, putErr),
			errString("DeleteItem "+expr, deleteErr), errString("UpdateItem "+expr, updateErr))
	}

	for _, expr := range []string{
		"SET a = if_not_exists(a)", "SET a = if_not_exists(a, :v, :v)",
		"SET a = list_append(a)", "SET a = list_append(a, a, a)",
	} {
		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: table, Key: key, UpdateExpression: aws.String(expr),
			ExpressionAttributeValues: withVals(expr),
		})

		out = append(out, expr, errString("UpdateItem "+expr, err))
	}

	return out
}

// parityUpdateItemAddScalarOnSet checks that ADD on a set needs a set operand of the same type (#191).
func parityUpdateItemAddScalarOnSet(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	paritySeedStarters(ctx, t, client)

	key := map[string]dynamodbtypes.AttributeValue{
		"id": &dynamodbtypes.AttributeValueMemberS{Value: "004"},
	}

	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(parityPokemonTable),
		Key:              key,
		UpdateExpression: aws.String("SET ns = :ns, bs = :bs"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":ns": &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}},
			":bs": &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
		},
	})
	require.NoError(t, err)

	add := func(attr string, v dynamodbtypes.AttributeValue) string {
		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(parityPokemonTable),
			Key:                       key,
			UpdateExpression:          aws.String("ADD " + attr + " :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": v},
		})
		if err == nil {
			return "accepted"
		}

		return normalizeSDKErrorString(err.Error())
	}

	return []any{
		// A single element of the set's type.
		add("moves", &dynamodbtypes.AttributeValueMemberS{Value: "y"}),
		add("ns", &dynamodbtypes.AttributeValueMemberN{Value: "2"}),
		add("bs", &dynamodbtypes.AttributeValueMemberB{Value: []byte("b")}),
		// A set operand on a number target.
		add("lvl", &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}}),
		add("lvl", &dynamodbtypes.AttributeValueMemberSS{Value: []string{"1"}}),
		// A set of the right type still works.
		add("moves", &dynamodbtypes.AttributeValueMemberSS{Value: []string{"y"}}),
		add("ns", &dynamodbtypes.AttributeValueMemberNS{Value: []string{"2"}}),
		add("bs", &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("b")}}),
		parityNormalizeItemSets(parityGetPokemon(ctx, t, client, "004")),
	}
}
