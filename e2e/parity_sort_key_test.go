package e2e

import (
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const paritySortKeyTable = "orders"

// parityCreateOrdersTable creates a table with a string partition key (customer)
// and a string sort key (sk), then seeds it with orders for two customers.
func parityCreateOrdersTable(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(paritySortKeyTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("customer"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("customer"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	seed := []struct {
		customer, sk, status string
		total                int
	}{
		{"alice", "order#001", "shipped", 10},
		{"alice", "order#002", "pending", 25},
		{"alice", "order#003", "shipped", 40},
		{"alice", "order#010", "cancelled", 5},
		{"alice", "profile", "active", 0},
		{"bob", "order#001", "pending", 99},
	}

	for _, s := range seed {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(paritySortKeyTable),
			Item: map[string]dynamodbtypes.AttributeValue{
				"customer": &dynamodbtypes.AttributeValueMemberS{Value: s.customer},
				"sk":       &dynamodbtypes.AttributeValueMemberS{Value: s.sk},
				"status":   &dynamodbtypes.AttributeValueMemberS{Value: s.status},
				"total":    &dynamodbtypes.AttributeValueMemberN{Value: strconv.Itoa(s.total)},
			},
		})
		require.NoError(t, err)
	}
}

// paritySortKeys returns the "sk" values of items in response order.
func paritySortKeys(items []map[string]dynamodbtypes.AttributeValue) []string {
	out := make([]string, 0, len(items))

	for _, it := range items {
		if v, ok := it["sk"].(*dynamodbtypes.AttributeValueMemberS); ok {
			out = append(out, v.Value)
		}
	}

	return out
}

func TestE2E_SortKey(t *testing.T) {
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "QuerySortKeyConditions",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				conditions := []struct {
					expr   string
					values map[string]dynamodbtypes.AttributeValue
				}{
					{"customer = :c AND sk = :a", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#002"},
					}},
					{"customer = :c AND sk < :a", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#003"},
					}},
					{"customer = :c AND sk <= :a", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#003"},
					}},
					{"customer = :c AND sk > :a", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#003"},
					}},
					{"customer = :c AND sk >= :a", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#003"},
					}},
					{"customer = :c AND sk BETWEEN :a AND :b", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#002"},
						":b": &dynamodbtypes.AttributeValueMemberS{Value: "order#010"},
					}},
					{"customer = :c AND begins_with(sk, :a)", map[string]dynamodbtypes.AttributeValue{
						":a": &dynamodbtypes.AttributeValueMemberS{Value: "order#"},
					}},
					{"customer = :c", map[string]dynamodbtypes.AttributeValue{}},
				}

				results := make([][]string, 0, len(conditions))

				for _, c := range conditions {
					c.values[":c"] = &dynamodbtypes.AttributeValueMemberS{Value: "alice"}

					out, err := client.Query(ctx, &dynamodb.QueryInput{
						TableName:                 aws.String(paritySortKeyTable),
						KeyConditionExpression:    aws.String(c.expr),
						ExpressionAttributeValues: c.values,
					})
					require.NoError(t, err, c.expr)

					results = append(results, paritySortKeys(out.Items))
				}

				return results
			},
		},
		{
			name: "QueryScanIndexForwardFalse",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c AND begins_with(sk, :p)"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "alice"},
						":p": &dynamodbtypes.AttributeValueMemberS{Value: "order#"},
					},
					ScanIndexForward: aws.Bool(false),
					Limit:            aws.Int32(2),
				})
				require.NoError(t, err)

				return []any{paritySortKeys(out.Items), parityKeySignature(out.LastEvaluatedKey)}
			},
		},
		{
			name: "QueryFilterCountAndScannedCount",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				input := &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c AND begins_with(sk, :p)"),
					FilterExpression:       aws.String("#s = :s"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "alice"},
						":p": &dynamodbtypes.AttributeValueMemberS{Value: "order#"},
						":s": &dynamodbtypes.AttributeValueMemberS{Value: "shipped"},
					},
				}

				out, err := client.Query(ctx, input)
				require.NoError(t, err)
				full := []any{out.Count, out.ScannedCount, paritySortKeys(out.Items)}

				input.Limit = aws.Int32(2)

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				limited := []any{out.Count, out.ScannedCount, paritySortKeys(out.Items), parityKeySignature(out.LastEvaluatedKey)}

				input.Limit = nil
				input.Select = dynamodbtypes.SelectCount

				out, err = client.Query(ctx, input)
				require.NoError(t, err)
				counted := []any{out.Count, out.ScannedCount, len(out.Items)}

				return []any{full, limited, counted}
			},
		},
		{
			name: "QueryProjectionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c AND sk = :sk"),
					ProjectionExpression:   aws.String("#t, sk"),
					ExpressionAttributeNames: map[string]string{
						"#t": "total",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c":  &dynamodbtypes.AttributeValueMemberS{Value: "bob"},
						":sk": &dynamodbtypes.AttributeValueMemberS{Value: "order#001"},
					},
				})
				require.NoError(t, err)

				return out.Items
			},
		},
		{
			name: "QueryKeyConditionOnSortKeyOnly",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("sk = :sk"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":sk": &dynamodbtypes.AttributeValueMemberS{Value: "order#001"},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "QueryNonKeyAttributeInKeyCondition",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c AND #s = :s"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "alice"},
						":s": &dynamodbtypes.AttributeValueMemberS{Value: "shipped"},
					},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "QueryPaginationDescending",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateOrdersTable(ctx, t, client)

				input := &dynamodb.QueryInput{
					TableName:              aws.String(paritySortKeyTable),
					KeyConditionExpression: aws.String("customer = :c"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":c": &dynamodbtypes.AttributeValueMemberS{Value: "alice"},
					},
					ScanIndexForward: aws.Bool(false),
					Limit:            aws.Int32(2),
				}

				var pages [][]string

				for range 5 {
					out, err := client.Query(ctx, input)
					require.NoError(t, err)

					pages = append(pages, paritySortKeys(out.Items))

					if len(out.LastEvaluatedKey) == 0 {
						break
					}

					input.ExclusiveStartKey = out.LastEvaluatedKey
				}

				return pages
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}
