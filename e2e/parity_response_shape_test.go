package e2e

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Response-shape parity: fields a caller may read that the other parity tests don't
// return. Most of these were found with MINIDYN_E2E_TRACE (see trace_test.go).

// parityDescribeShape keeps the TableDescription fields that are deterministic across
// engines (no ARNs, ids, timestamps or counters).
func parityDescribeShape(d *dynamodbtypes.TableDescription) []any {
	if d == nil {
		return nil
	}

	defs := make([]string, 0, len(d.AttributeDefinitions))
	for _, a := range d.AttributeDefinitions {
		defs = append(defs, aws.ToString(a.AttributeName)+":"+string(a.AttributeType))
	}

	sort.Strings(defs)

	billing := ""
	if d.BillingModeSummary != nil {
		billing = string(d.BillingModeSummary.BillingMode)
	}

	var rcu, wcu int64
	if d.ProvisionedThroughput != nil {
		rcu = aws.ToInt64(d.ProvisionedThroughput.ReadCapacityUnits)
		wcu = aws.ToInt64(d.ProvisionedThroughput.WriteCapacityUnits)
	}

	gsis := make([]string, 0, len(d.GlobalSecondaryIndexes))
	for _, g := range d.GlobalSecondaryIndexes {
		gsis = append(gsis, aws.ToString(g.IndexName)+":"+string(g.IndexStatus))
	}

	sort.Strings(gsis)

	return []any{
		string(d.TableStatus), defs, billing, rcu, wcu,
		aws.ToBool(d.DeletionProtectionEnabled), gsis,
	}
}

func TestE2E_ResponseShape(t *testing.T) {
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "ListTables",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityCreateOrdersTable(ctx, t, client)

				out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
				require.NoError(t, err)

				return out.TableNames
			},
		},
		{
			name:       "TableDescriptionFields",
			knownIssue: "#151",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				created, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String("provisioned"),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
					},
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					BillingMode: dynamodbtypes.BillingModeProvisioned,
					ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(3),
						WriteCapacityUnits: aws.Int64(2),
					},
				})
				require.NoError(t, err)

				parityCreatePokemonTable(ctx, t, client)

				described, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)

				deleted, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
					TableName: aws.String("provisioned"),
				})
				require.NoError(t, err)

				return []any{
					parityDescribeShape(created.TableDescription),
					parityDescribeShape(described.Table),
					parityDescribeShape(deleted.TableDescription),
				}
			},
		},
		{
			name:       "UpdateTableDeleteIndexDescription",
			knownIssue: "#151",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				out, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
					TableName: aws.String(parityPokemonTable),
					GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
						{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(parityByTypeIndexName)}},
					},
				})
				require.NoError(t, err)

				return parityDescribeShape(out.TableDescription)
			},
		},
		{
			name: "PutItemWithoutReturnValues",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable),
					Item:      parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}),
				})
				require.NoError(t, err)

				return len(out.Attributes)
			},
		},
		{
			name: "EmptyMapAndListAttributes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityPokemonTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id":    &dynamodbtypes.AttributeValueMemberS{Value: "001"},
						"info":  &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{}},
						"local": &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{}},
						"nested": &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{
							&dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{}},
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

				types := map[string]string{}
				for k, v := range out.Item {
					types[k] = fmt.Sprintf("%T", v)
				}

				if l, ok := out.Item["nested"].(*dynamodbtypes.AttributeValueMemberL); ok && len(l.Value) == 1 {
					types["nested[0]"] = fmt.Sprintf("%T", l.Value[0])
				}

				return types
			},
		},
		{
			name: "QueryLimitWithFilterLastEvaluatedKey",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				// alice has order#001 shipped, order#002 pending, order#003 shipped,
				// order#010 cancelled, profile active.
				input := &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c"),
					FilterExpression:       aws.String("#s = :s"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "alice"},
						":s": &dynamodbtypes.AttributeValueMemberS{Value: "cancelled"},
					},
					Limit: aws.Int32(2),
				}

				var pages []any

				for range 5 {
					out, err := client.Query(ctx, input)
					require.NoError(t, err)

					pages = append(pages, []any{paritySortKeys(out.Items), parityKeySignature(out.LastEvaluatedKey)})

					if len(out.LastEvaluatedKey) == 0 {
						break
					}

					input.ExclusiveStartKey = out.LastEvaluatedKey
				}

				return pages
			},
		},
		{
			name: "TransactionCanceledReasons",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item:      parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}),
						}},
						{ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(parityPokemonTable),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "404"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						}},
					},
				})
				require.Error(t, err)

				var canceled *dynamodbtypes.TransactionCanceledException
				require.True(t, errors.As(err, &canceled), "%v", err)

				reasons := make([]string, 0, len(canceled.CancellationReasons))
				for _, r := range canceled.CancellationReasons {
					reasons = append(reasons, aws.ToString(r.Code)+"|"+aws.ToString(r.Message))
				}

				return []any{aws.ToString(canceled.Message), reasons}
			},
		},
		{
			name:       "QueryNoMatchesItemsAndCount",
			knownIssue: "#144",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "nobody"},
					},
				})
				require.NoError(t, err)

				return []any{out.Items == nil, out.Count, out.ScannedCount}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}

// TestE2E_ListTables covers ListTables ordering, pagination and Limit validation.
func TestE2E_ListTables(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "Pagination",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				for _, name := range []string{"zeta", "Beta", "alpha", "alpha-2", "alpha.1", "gamma_1"} {
					parityCreateSimpleTable(ctx, t, client, name)
				}

				input := &dynamodb.ListTablesInput{Limit: aws.Int32(2)}

				var pages []any

				for range 6 {
					out, err := client.ListTables(ctx, input)
					require.NoError(t, err)

					pages = append(pages, []any{out.TableNames, aws.ToString(out.LastEvaluatedTableName)})

					if out.LastEvaluatedTableName == nil {
						break
					}

					input.ExclusiveStartTableName = out.LastEvaluatedTableName
				}

				return pages
			},
		},
		{
			name: "ExclusiveStartTableName",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				for _, name := range []string{"aaa", "bbb", "ccc"} {
					parityCreateSimpleTable(ctx, t, client, name)
				}

				var results []any

				for _, start := range []string{"aaa", "abc", "ccc", "zzz"} {
					out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
						ExclusiveStartTableName: aws.String(start),
					})
					require.NoError(t, err)

					results = append(results, []any{start, out.TableNames, aws.ToString(out.LastEvaluatedTableName)})
				}

				out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(3)})
				require.NoError(t, err)

				results = append(results, []any{"limit=3", out.TableNames, aws.ToString(out.LastEvaluatedTableName)})

				return results
			},
		},
		{
			name: "InvalidLimit",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				var msgs []string

				for _, limit := range []int32{0, -1, 101} {
					_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(limit)})
					require.Error(t, err)

					msgs = append(msgs, normalizeSDKErrorString(err.Error()))
				}

				out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(100)})
				require.NoError(t, err)

				return []any{msgs, out.TableNames}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
