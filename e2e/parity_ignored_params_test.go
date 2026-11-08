package e2e

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Request parameters that minidyn decodes but never reads, as reported by
// `go run ./tools/parity-surface`. Each case checks that the parameter has an effect.

func TestE2E_IgnoredParams(t *testing.T) {
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name:       "ParallelScanSegments",
			knownIssue: "#155",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				const total = 3

				var all []string

				for seg := range int32(total) {
					out, err := client.Scan(ctx, &dynamodb.ScanInput{
						TableName:     aws.String(paritySortKeyTable),
						Segment:       aws.Int32(seg),
						TotalSegments: aws.Int32(total),
					})
					require.NoError(t, err)

					for _, it := range out.Items {
						all = append(all, parityAttrValueString(it["customer"])+"/"+parityAttrValueString(it["sk"]))
					}
				}

				sort.Strings(all)

				// Every item exactly once across the segments.
				return all
			},
		},
		{
			name:       "LegacyExpectedOnPutItem",
			knownIssue: "#156",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable),
					Item:      parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "fire", Name: "Impostor"}),
					Expected: map[string]dynamodbtypes.ExpectedAttributeValue{
						"id": {Exists: aws.Bool(false)},
					},
				})

				var ccf *dynamodbtypes.ConditionalCheckFailedException

				return []any{errors.As(err, &ccf), parityAttrValueString(parityGetPokemon(ctx, t, client, "001")["name"])}
			},
		},
		{
			name:       "LegacyAttributeUpdatesOnUpdateItem",
			knownIssue: "#156",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					AttributeUpdates: map[string]dynamodbtypes.AttributeValueUpdate{
						"name": {
							Action: dynamodbtypes.AttributeActionPut,
							Value:  &dynamodbtypes.AttributeValueMemberS{Value: "Ivysaur"},
						},
					},
				})
				require.NoError(t, err)

				return parityAttrValueString(parityGetPokemon(ctx, t, client, "001")["name"])
			},
		},
		{
			name:       "LegacyKeyConditionsOnQuery",
			knownIssue: "#156",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName: aws.String(paritySortKeyTable),
					KeyConditions: map[string]dynamodbtypes.Condition{
						"customer": {
							ComparisonOperator: dynamodbtypes.ComparisonOperatorEq,
							AttributeValueList: []dynamodbtypes.AttributeValue{
								&dynamodbtypes.AttributeValueMemberS{Value: "bob"},
							},
						},
					},
				})
				if err != nil {
					return normalizeSDKErrorString(err.Error())
				}

				return paritySortKeys(out.Items)
			},
		},
		{
			name: "ReturnValuesOnConditionCheckFailure",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:                           aws.String(parityPokemonTable),
					Item:                                parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "fire", Name: "Impostor"}),
					ConditionExpression:                 aws.String("attribute_not_exists(id)"),
					ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
				})

				var ccf *dynamodbtypes.ConditionalCheckFailedException
				require.True(t, errors.As(err, &ccf), "%v", err)

				return parityAttrValueString(ccf.Item["name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}
