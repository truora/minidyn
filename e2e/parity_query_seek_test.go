package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Query edge cases around finding the queried partition: number spellings, partition
// values that are prefixes of each other, empty partitions and start keys of another
// partition.

const paritySeekTable = "seek"

// parityCreateSeekTable creates table "seek" with hash key pk of type pkType, an optional
// string range key sk, and an optional hash-only GSI "by-g" on g.
func parityCreateSeekTable(ctx context.Context, t *testing.T, client *dynamodb.Client,
	pkType dynamodbtypes.ScalarAttributeType, withRange, withGSI bool,
) {
	t.Helper()

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(paritySeekTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: pkType},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	}

	if withRange {
		input.KeySchema = append(input.KeySchema, dynamodbtypes.KeySchemaElement{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange})
		input.AttributeDefinitions = append(input.AttributeDefinitions, dynamodbtypes.AttributeDefinition{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS})
	}

	if withGSI {
		input.AttributeDefinitions = append(input.AttributeDefinitions, dynamodbtypes.AttributeDefinition{AttributeName: aws.String("g"), AttributeType: dynamodbtypes.ScalarAttributeTypeS})
		input.GlobalSecondaryIndexes = []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-g"),
			KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("g"), KeyType: dynamodbtypes.KeyTypeHash}},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}}
	}

	_, err := client.CreateTable(ctx, input)
	require.NoError(t, err)

	if withGSI {
		parityWaitUntilGSIActive(ctx, t, client, paritySeekTable, "by-g")
	}
}

func paritySeekPut(ctx context.Context, t *testing.T, client *dynamodb.Client, item map[string]dynamodbtypes.AttributeValue) {
	t.Helper()

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(paritySeekTable), Item: item})
	require.NoError(t, err)
}

// paritySeekPages pages through a query and returns, per page, the item keys and the
// LastEvaluatedKey.
func paritySeekPages(ctx context.Context, t *testing.T, client *dynamodb.Client, input *dynamodb.QueryInput) []any {
	t.Helper()

	var pages []any

	for range 6 {
		out, err := client.Query(ctx, input)
		require.NoError(t, err)

		keys := make([]string, 0, len(out.Items))
		for _, it := range out.Items {
			keys = append(keys, parityKeySignature(map[string]dynamodbtypes.AttributeValue{"pk": it["pk"], "sk": it["sk"]}))
		}

		pages = append(pages, []any{keys, parityKeySignature(out.LastEvaluatedKey)})

		if len(out.LastEvaluatedKey) == 0 {
			break
		}

		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	return pages
}

func TestE2E_QuerySeek(t *testing.T) {
	s := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	n := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "NumericHashKeySpellings",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSeekTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeN, true, false)
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": n("7"), "sk": s("a")})
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": n("7"), "sk": s("b")})
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": n("70"), "sk": s("a")})

				var out []any

				for _, v := range []string{"7", "7.0", "07", "7e0"} {
					pages := paritySeekPages(ctx, t, client, &dynamodb.QueryInput{
						TableName:                 aws.String(paritySeekTable),
						KeyConditionExpression:    aws.String("pk = :p"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": n(v)},
					})
					out = append(out, v, pages)
				}

				return out
			},
		},
		{
			name: "HashOnlyPrefixPartitionsWithFilterAndLimit",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSeekTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, false, false)

				for _, pk := range []string{"alice", "alicex", "alicez", "bob"} {
					paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s(pk), "f": s("yes")})
				}

				return paritySeekPages(ctx, t, client, &dynamodb.QueryInput{
					TableName:                 aws.String(paritySeekTable),
					KeyConditionExpression:    aws.String("pk = :p"),
					FilterExpression:          aws.String("f = :f"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": s("alice"), ":f": s("yes")},
					Limit:                     aws.Int32(2),
				})
			},
		},
		{
			name: "HashOnlyGSIPrefixPartitionsWithLimit",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSeekTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, false, true)
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s("ann"), "g": s("x")})
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s("bob"), "g": s("xb")})

				return paritySeekPages(ctx, t, client, &dynamodb.QueryInput{
					TableName:                 aws.String(paritySeekTable),
					IndexName:                 aws.String("by-g"),
					KeyConditionExpression:    aws.String("g = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":g": s("x")},
					Limit:                     aws.Int32(1),
				})
			},
		},
		{
			name: "CompositeHashWithSeparatorAndLimit",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSeekTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, true, false)

				for _, k := range [][2]string{{"a", "1"}, {"a", "2"}, {"a.b", "1"}, {"a.b", "2"}} {
					paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s(k[0]), "sk": s(k[1]), "f": s("yes")})
				}

				return paritySeekPages(ctx, t, client, &dynamodb.QueryInput{
					TableName:                 aws.String(paritySeekTable),
					KeyConditionExpression:    aws.String("pk = :p"),
					FilterExpression:          aws.String("f = :f"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": s("a"), ":f": s("yes")},
					Limit:                     aws.Int32(3),
				})
			},
		},
		{
			// minidyn rejects it while evaluating ("right operand is not a string"); DynamoDB
			// checks placeholder types against the key schema up front.
			name:       "InvalidConditionOnEmptyPartition",
			knownIssue: "#145",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSeekTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, true, false)
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s("a"), "sk": s("1"), "f": s("yes")})

				var out []string

				for _, q := range []struct{ key, filter string }{
					{"pk = :p AND sk > :n", ""},
					{"pk = :p", "f > :n"},
				} {
					input := &dynamodb.QueryInput{
						TableName:                 aws.String(paritySeekTable),
						KeyConditionExpression:    aws.String(q.key),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": s("zzz"), ":n": n("1")},
					}
					if q.filter != "" {
						input.FilterExpression = aws.String(q.filter)
					}

					_, err := client.Query(ctx, input)
					out = append(out, q.key+" | "+q.filter+": "+fuzzErrString(err))
				}

				return out
			},
		},
		{
			name: "ExclusiveStartKeyFromAnotherPartition",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSeekTable(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS, true, false)
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s("a"), "sk": s("1")})
				paritySeekPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{"pk": s("b"), "sk": s("1")})

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(paritySeekTable),
					KeyConditionExpression:    aws.String("pk = :p"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": s("b")},
					ExclusiveStartKey:         map[string]dynamodbtypes.AttributeValue{"pk": s("a"), "sk": s("1")},
				})

				return fuzzErrString(err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}
