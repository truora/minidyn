package e2e

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func boolStr(b bool) string {
	if b {
		return "1"
	}

	return "0"
}

func TestE2E_QueryBatch(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "Query",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "004", Type: "fire", Name: "Charmander"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "007", Type: "water", Name: "Squirtle"})

				input := &dynamodb.QueryInput{
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":id": &dynamodbtypes.AttributeValueMemberS{Value: "004"},
					},
					ExpressionAttributeNames: map[string]string{
						"#id": "id",
					},
					KeyConditionExpression: aws.String("#id = :id"),
					TableName:              aws.String(parityPokemonTable),
				}

				out, err := client.Query(ctx, input)
				require.NoError(t, err)
				counts := []int{len(out.Items)}
				require.Empty(t, out.LastEvaluatedKey)

				input.FilterExpression = aws.String("#type = :type")
				input.ExpressionAttributeNames["#type"] = "type"
				input.ExpressionAttributeValues[":type"] = &dynamodbtypes.AttributeValueMemberS{Value: "fire"}

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				counts = append(counts, len(out.Items))

				input.ExpressionAttributeValues[":type"] = &dynamodbtypes.AttributeValueMemberS{Value: "grass"}

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				counts = append(counts, len(out.Items))

				input.ExpressionAttributeNames["#not_used"] = "hello"

				_, err = client.Query(ctx, input)
				require.Error(t, err)
				errs := []string{normalizeSDKErrorString(err.Error())}

				input.KeyConditionExpression = aws.String("#invalid-name = :id")
				input.ExpressionAttributeNames = map[string]string{
					"#invalid-name": "id",
				}

				_, err = client.Query(ctx, input)
				require.Error(t, err)
				errs = append(errs, normalizeSDKErrorString(err.Error()))

				input.KeyConditionExpression = aws.String("#t = :invalid-value")

				input.ExpressionAttributeNames = map[string]string{
					"#t": "type",
				}

				input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
					":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
				}

				_, err = client.Query(ctx, input)
				require.Error(t, err)
				errs = append(errs, normalizeSDKErrorString(err.Error()))

				return []any{counts, errs}
			},
		},
		{
			name: "QueryPagination",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "002", Type: "grass", Name: "Ivysaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "003", Type: "grass", Name: "Venusaur"})

				input := &dynamodb.QueryInput{
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":type": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
					},
					ExpressionAttributeNames: map[string]string{
						"#type": "type",
					},
					KeyConditionExpression: aws.String("#type = :type"),
					TableName:              aws.String(parityPokemonTable),
					IndexName:              aws.String(parityByTypeIndexName),
					Limit:                  aws.Int32(1),
				}

				var lines []string

				appendPage := func(out *dynamodb.QueryOutput) {
					id := ""
					if len(out.Items) > 0 {
						if s, ok := out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS); ok {
							id = s.Value
						}
					}

					lines = append(lines, strings.Join([]string{
						id,
						boolStr(len(out.Items) > 0),
						boolStr(len(out.LastEvaluatedKey) > 0),
					}, "|"))
				}

				out, err := client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 1)
				require.Equal(t, "001", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)

				input.ExclusiveStartKey = out.LastEvaluatedKey
				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 1)
				require.Equal(t, "002", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)

				input.ExclusiveStartKey = out.LastEvaluatedKey
				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 1)
				require.Equal(t, "003", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
				require.NotEmpty(t, out.LastEvaluatedKey)

				input.ExclusiveStartKey = out.LastEvaluatedKey
				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Empty(t, out.Items)
				require.Empty(t, out.LastEvaluatedKey)

				input.Limit = aws.Int32(4)
				input.ExclusiveStartKey = nil

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 3)
				require.Empty(t, out.LastEvaluatedKey)

				input.Limit = aws.Int32(2)
				input.ExclusiveStartKey = nil

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 2)
				require.NotEmpty(t, out.LastEvaluatedKey)
				input.ExclusiveStartKey = out.LastEvaluatedKey

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 1)
				require.Empty(t, out.LastEvaluatedKey)

				input.Limit = nil
				input.ExclusiveStartKey = nil

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 3)
				require.Empty(t, out.LastEvaluatedKey)

				input.Limit = aws.Int32(4)
				input.ExclusiveStartKey = nil

				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "004", Type: "fire", Name: "Charmander"})

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				appendPage(out)
				require.Len(t, out.Items, 3)
				require.Empty(t, out.LastEvaluatedKey)

				input.ScanIndexForward = aws.Bool(false)

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				require.Equal(t, "003", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
				lines = append(lines, "scan_forward_false_first="+out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)

				input.ScanIndexForward = nil
				input.ExclusiveStartKey = nil
				input.FilterExpression = aws.String("begins_with(#name, :letter)")
				input.Limit = aws.Int32(2)
				input.ExpressionAttributeValues[":letter"] = &dynamodbtypes.AttributeValueMemberS{Value: "V"}
				input.ExpressionAttributeNames["#name"] = "name"

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				lines = append(lines, "filter_p1_items="+strconv.Itoa(len(out.Items))+",lek="+boolStr(len(out.LastEvaluatedKey) > 0))
				require.Empty(t, out.Items)
				require.NotEmpty(t, out.LastEvaluatedKey)

				input.ExclusiveStartKey = out.LastEvaluatedKey
				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				require.Len(t, out.Items, 1)
				require.Equal(t, "003", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
				lines = append(lines, "filter_p2_id=003")

				return lines
			},
		},
		{
			name: "QuerySyntaxError",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":partition": &dynamodbtypes.AttributeValueMemberS{Value: "a"},
					},
					ExpressionAttributeNames: map[string]string{
						"#partition": "partition",
					},
					KeyConditionExpression: aws.String("#partition != :partition"),
					TableName:              aws.String(parityPokemonTable),
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "Scan",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "002", Type: "grass", Name: "Ivysaur"})
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "003", Type: "grass", Name: "Venusaur"})

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)
				counts := []int{len(out.Items)}

				out, err = client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(parityPokemonTable),
					Limit:     aws.Int32(1),
				})
				require.NoError(t, err)
				counts = append(counts, len(out.Items))
				le1 := len(out.LastEvaluatedKey) > 0

				_, err = client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(parityPokemonTable),
					FilterExpression: aws.String("#invalid-name = Raichu"),
					ExpressionAttributeNames: map[string]string{
						"#invalid-name": "Name",
					},
				})
				require.Error(t, err)
				errs := []string{normalizeSDKErrorString(err.Error())}

				_, err = client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(parityPokemonTable),
					FilterExpression: aws.String("#t = :invalid-value"),
					ExpressionAttributeNames: map[string]string{
						"#t": "type",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
					},
				})
				require.Error(t, err)
				errs = append(errs, normalizeSDKErrorString(err.Error()))

				out, err = client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(parityPokemonTable),
					FilterExpression: aws.String("#name = :name"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":name": &dynamodbtypes.AttributeValueMemberS{Value: "Venusaur"},
					},
					ExpressionAttributeNames: map[string]string{
						"#name": "name",
					},
				})
				require.NoError(t, err)
				counts = append(counts, len(out.Items))

				_, err = client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(parityPokemonTable),
					FilterExpression: aws.String("#name = :name"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":name": &dynamodbtypes.AttributeValueMemberS{Value: "Venusaur"},
					},
					ExpressionAttributeNames: map[string]string{
						"#name":     "name",
						"#not_used": "hello",
					},
				})
				require.Error(t, err)
				errs = append(errs, normalizeSDKErrorString(err.Error()))

				return []any{counts, le1, errs}
			},
		},
		{
			name: "BatchWriteItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				opt := func(o *attributevalue.EncoderOptions) {
					o.TagKey = "json"
				}

				m := parityPokemon{
					ID:   "001",
					Type: "grass",
					Name: "Bulbasaur",
				}

				item, err := attributevalue.MarshalMapWithOptions(m, opt)
				require.NoError(t, err)

				requests := []dynamodbtypes.WriteRequest{
					{PutRequest: &dynamodbtypes.PutRequest{Item: item}},
				}

				input := &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: requests,
					},
				}

				_, err = client.BatchWriteItem(ctx, input)
				require.NoError(t, err)

				require.NotEmpty(t, parityGetPokemon(ctx, t, client, "001"))

				_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {
							{
								DeleteRequest: &dynamodbtypes.DeleteRequest{
									Key: map[string]dynamodbtypes.AttributeValue{
										"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
									},
								},
							},
						},
					},
				})
				require.NoError(t, err)

				require.Empty(t, parityGetPokemon(ctx, t, client, "001"))

				delete(item, "id")

				_, err = client.BatchWriteItem(ctx, input)
				require.Error(t, err)

				var lines []string
				lines = append(lines, normalizeSDKErrorString(err.Error()))

				_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {
							{
								DeleteRequest: &dynamodbtypes.DeleteRequest{
									Key: map[string]dynamodbtypes.AttributeValue{
										"id":   &dynamodbtypes.AttributeValueMemberS{Value: "001"},
										"type": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
									},
								},
								PutRequest: &dynamodbtypes.PutRequest{Item: item},
							},
						},
					},
				})
				require.Error(t, err)
				lines = append(lines, normalizeSDKErrorString(err.Error()))

				_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {{}},
					},
				})
				require.Error(t, err)
				lines = append(lines, normalizeSDKErrorString(err.Error()))

				item, err = attributevalue.MarshalMap(m)
				require.NoError(t, err)

				for range e2eBatchWriteRequestsLimit {
					requests = append(requests, dynamodbtypes.WriteRequest{
						PutRequest: &dynamodbtypes.PutRequest{Item: item},
					})
				}

				_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: requests,
					},
				})
				require.Error(t, err)
				lines = append(lines, normalizeSDKErrorString(err.Error()))

				return lines
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
