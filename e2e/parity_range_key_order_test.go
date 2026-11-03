package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Range key ordering parity for every scalar type DynamoDB allows as a key: N, S and B.
// Each case seeds one partition in shuffled order and stores the original spelling in a
// "label" attribute, so comparisons do not depend on how each engine canonicalizes values.

const (
	parityRangeOrderTable = "range_order"
	parityRangeOrderGSI   = "by-rk"
	parityRangeOrderPK    = "series"
)

type parityRangeValue struct {
	label string
	value dynamodbtypes.AttributeValue
}

func parityN(v string) parityRangeValue {
	return parityRangeValue{label: v, value: &dynamodbtypes.AttributeValueMemberN{Value: v}}
}

func parityS(v string) parityRangeValue {
	return parityRangeValue{label: v, value: &dynamodbtypes.AttributeValueMemberS{Value: v}}
}

func parityB(label string, v ...byte) parityRangeValue {
	return parityRangeValue{label: label, value: &dynamodbtypes.AttributeValueMemberB{Value: v}}
}

// Values where lexicographic text order and the DynamoDB order disagree.
var (
	parityNumberRangeValues = []parityRangeValue{
		parityN("10"), parityN("2"), parityN("-5"), parityN("100"), parityN("0"), parityN("1.5"),
		parityN("-10.25"), parityN("9"), parityN("1E3"), parityN("0.001"), parityN("-0.5"),
		parityN("99999999999"), parityN("20"),
	}

	// DynamoDB orders strings by their UTF-8 bytes.
	parityStringRangeValues = []parityRangeValue{
		parityS("b"), parityS("a"), parityS("B"), parityS("A"), parityS("aa"), parityS("a b"),
		parityS("a.b"), parityS("10"), parityS("9"), parityS("é"), parityS("z"), parityS("~"),
		parityS("_"), parityS("日本"), parityS("😀"), parityS("�"),
	}

	// DynamoDB orders binary values as unsigned bytes.
	parityBinaryRangeValues = []parityRangeValue{
		parityB("02", 0x02), parityB("00", 0x00), parityB("ff", 0xff), parityB("01", 0x01),
		parityB("0100", 0x01, 0x00), parityB("7f", 0x7f), parityB("80", 0x80), parityB("10", 0x10),
		parityB("ff00", 0xff, 0x00), parityB("0a", 0x0a), parityB("0102", 0x01, 0x02),
	}
)

// parityCreateRangeOrderTable creates a table with hash key pk (S) and range key rk of
// rangeType, plus a GSI (grp, rk2) whose range key has the same type, and seeds vals.
func parityCreateRangeOrderTable(
	ctx context.Context,
	t *testing.T,
	client *dynamodb.Client,
	rangeType dynamodbtypes.ScalarAttributeType,
	vals []parityRangeValue,
) {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(parityRangeOrderTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("rk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("rk"), AttributeType: rangeType},
			{AttributeName: aws.String("grp"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("rk2"), AttributeType: rangeType},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String(parityRangeOrderGSI),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("grp"), KeyType: dynamodbtypes.KeyTypeHash},
					{AttributeName: aws.String("rk2"), KeyType: dynamodbtypes.KeyTypeRange},
				},
				Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	for _, v := range vals {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(parityRangeOrderTable),
			Item: map[string]dynamodbtypes.AttributeValue{
				"pk":    &dynamodbtypes.AttributeValueMemberS{Value: parityRangeOrderPK},
				"rk":    v.value,
				"grp":   &dynamodbtypes.AttributeValueMemberS{Value: "all"},
				"rk2":   v.value,
				"label": &dynamodbtypes.AttributeValueMemberS{Value: v.label},
			},
		})
		require.NoError(t, err, v.label)
	}
}

// parityQueryLabels runs input to completion (following LastEvaluatedKey) and returns the
// "label" of every item in response order.
func parityQueryLabels(ctx context.Context, t *testing.T, client *dynamodb.Client, input *dynamodb.QueryInput) []string {
	t.Helper()

	var labels []string

	for range 50 {
		out, err := client.Query(ctx, input)
		require.NoError(t, err, aws.ToString(input.KeyConditionExpression))

		for _, it := range out.Items {
			if v, ok := it["label"].(*dynamodbtypes.AttributeValueMemberS); ok {
				labels = append(labels, v.Value)
			}
		}

		if len(out.LastEvaluatedKey) == 0 {
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	return labels
}

// parityRangeQuery builds a base-table query on the seeded partition.
func parityRangeQuery(cond string, values map[string]dynamodbtypes.AttributeValue, forward bool, limit int32) *dynamodb.QueryInput {
	if values == nil {
		values = map[string]dynamodbtypes.AttributeValue{}
	}

	values[":p"] = &dynamodbtypes.AttributeValueMemberS{Value: parityRangeOrderPK}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(parityRangeOrderTable),
		KeyConditionExpression:    aws.String(cond),
		ExpressionAttributeValues: values,
		ScanIndexForward:          aws.Bool(forward),
	}
	if limit > 0 {
		input.Limit = aws.Int32(limit)
	}

	return input
}

// parityRangeOrderBasics returns full-partition orderings on the base table (ascending,
// descending, and paginated both ways) and on the GSI (ascending and descending).
func parityRangeOrderBasics(ctx context.Context, t *testing.T, client *dynamodb.Client) [][]string {
	t.Helper()

	results := [][]string{
		parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p", nil, true, 0)),
		parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p", nil, false, 0)),
		parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p", nil, true, 3)),
		parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p", nil, false, 3)),
	}

	for _, forward := range []bool{true, false} {
		results = append(results, parityQueryLabels(ctx, t, client, &dynamodb.QueryInput{
			TableName:              aws.String(parityRangeOrderTable),
			IndexName:              aws.String(parityRangeOrderGSI),
			KeyConditionExpression: aws.String("grp = :g"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":g": &dynamodbtypes.AttributeValueMemberS{Value: "all"},
			},
			ScanIndexForward: aws.Bool(forward),
		}))
	}

	return results
}

func TestE2E_RangeKeyOrder(t *testing.T) {
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name:       "NumberOrder",
			knownIssue: "#147",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateRangeOrderTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeN, parityNumberRangeValues)

				return parityRangeOrderBasics(ctx, t, client)
			},
		},
		{
			name:       "NumberConditionsOrder",
			knownIssue: "#147",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateRangeOrderTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeN, parityNumberRangeValues)

				n := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

				return [][]string{
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk BETWEEN :a AND :b",
						map[string]dynamodbtypes.AttributeValue{":a": n("-1"), ":b": n("10")}, true, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk > :a",
						map[string]dynamodbtypes.AttributeValue{":a": n("9")}, true, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk <= :a",
						map[string]dynamodbtypes.AttributeValue{":a": n("0.5")}, false, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk = :a",
						map[string]dynamodbtypes.AttributeValue{":a": n("1000")}, true, 0)),
				}
			},
		},
		{
			name: "StringOrder",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateRangeOrderTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, parityStringRangeValues)

				return parityRangeOrderBasics(ctx, t, client)
			},
		},
		{
			name: "StringConditionsOrder",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateRangeOrderTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, parityStringRangeValues)

				s := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

				return [][]string{
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk BETWEEN :a AND :b",
						map[string]dynamodbtypes.AttributeValue{":a": s("A"), ":b": s("a.b")}, true, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND begins_with(rk, :a)",
						map[string]dynamodbtypes.AttributeValue{":a": s("a")}, false, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk > :a",
						map[string]dynamodbtypes.AttributeValue{":a": s("z")}, true, 0)),
				}
			},
		},
		{
			name:       "BinaryOrder",
			knownIssue: "#148",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateRangeOrderTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeB, parityBinaryRangeValues)

				return parityRangeOrderBasics(ctx, t, client)
			},
		},
		{
			name:       "BinaryConditionsOrder",
			knownIssue: "#148",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateRangeOrderTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeB, parityBinaryRangeValues)

				b := func(v ...byte) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberB{Value: v} }

				return [][]string{
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk BETWEEN :a AND :b",
						map[string]dynamodbtypes.AttributeValue{":a": b(0x01), ":b": b(0x7f)}, true, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND begins_with(rk, :a)",
						map[string]dynamodbtypes.AttributeValue{":a": b(0x01)}, true, 0)),
					parityQueryLabels(ctx, t, client, parityRangeQuery("pk = :p AND rk >= :a",
						map[string]dynamodbtypes.AttributeValue{":a": b(0x80)}, false, 0)),
				}
			},
		},
		{
			name:       "KeysContainingSeparator",
			knownIssue: "#149",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String(parityRangeOrderTable),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("rk"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
						{AttributeName: aws.String("rk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					BillingMode: dynamodbtypes.BillingModePayPerRequest,
				})
				require.NoError(t, err)

				// ("a", "b.c") and ("a.b", "c") are distinct primary keys in DynamoDB.
				for _, k := range [][2]string{{"a", "b.c"}, {"a.b", "c"}, {"a", "a"}} {
					_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(parityRangeOrderTable),
						Item: map[string]dynamodbtypes.AttributeValue{
							"pk":    &dynamodbtypes.AttributeValueMemberS{Value: k[0]},
							"rk":    &dynamodbtypes.AttributeValueMemberS{Value: k[1]},
							"label": &dynamodbtypes.AttributeValueMemberS{Value: k[0] + "|" + k[1]},
						},
					})
					require.NoError(t, err)
				}

				query := func(pk string) []string {
					return parityQueryLabels(ctx, t, client, &dynamodb.QueryInput{
						TableName:              aws.String(parityRangeOrderTable),
						KeyConditionExpression: aws.String("pk = :p"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
							":p": &dynamodbtypes.AttributeValueMemberS{Value: pk},
						},
					})
				}

				scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(parityRangeOrderTable)})
				require.NoError(t, err)

				return []any{query("a"), query("a.b"), scan.Count}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}
