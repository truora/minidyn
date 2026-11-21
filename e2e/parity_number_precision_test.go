package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// N values keep up to 38 significant digits wherever minidyn reads them into the interpreter
// and writes them back (#259): projections, UpdateItem (the attributes it does not change, copies,
// list_append, if_not_exists, ReturnValues), conditions and arithmetic. The numbers here are in the
// form DynamoDB stores, so the text compares as it is.

const (
	numPrecDigits38 = "12345678901234567890123456789012345678"
	numPrecNext     = "12345678901234567890123456789012345679"
)

func numPrecKey() map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"pk": fzS("a"), "sk": fzN(numPrecDigits38)}
}

func numPrecItem() map[string]dynamodbtypes.AttributeValue {
	item := numPrecKey()
	item["n"] = fzN(numPrecDigits38)
	item["l"] = &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{fzN(numPrecDigits38)}}
	item["m"] = &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"x": fzN(numPrecDigits38)}}
	item["ns"] = &dynamodbtypes.AttributeValueMemberNS{Value: []string{numPrecDigits38}}

	return item
}

func numPrecSetup(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	fuzzSetupFor(ctx, t, client, fuzzSchemaN, numPrecItem())
}

func TestE2E_NumberPrecision(t *testing.T) {
	const projection = "sk, n, l, m, ns"

	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "GetItemProjection",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(fuzzTable), Key: numPrecKey(), ProjectionExpression: aws.String(projection),
				})
				require.NoError(t, err)

				return parityNormalizeItemSets(out.Item)
			},
		},
		{
			name: "QueryScanProjection",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				query, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(fuzzTable),
					KeyConditionExpression:    aws.String("pk = :p"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": fzS("a")},
					ProjectionExpression:      aws.String(projection),
				})
				require.NoError(t, err)

				scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(fuzzTable), ProjectionExpression: aws.String(projection)})
				require.NoError(t, err)

				return []any{parityNormalizeItemSets(query.Items[0]), parityNormalizeItemSets(scan.Items[0])}
			},
		},
		{
			name: "BatchGetAndTransactGetProjection",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				batch, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
					RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
						fuzzTable: {Keys: []map[string]dynamodbtypes.AttributeValue{numPrecKey()}, ProjectionExpression: aws.String(projection)},
					},
				})
				require.NoError(t, err)

				transact, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					TransactItems: []dynamodbtypes.TransactGetItem{{Get: &dynamodbtypes.Get{
						TableName: aws.String(fuzzTable), Key: numPrecKey(), ProjectionExpression: aws.String(projection),
					}}},
				})
				require.NoError(t, err)

				return []any{
					parityNormalizeItemSets(batch.Responses[fuzzTable][0]),
					parityNormalizeItemSets(transact.Responses[0].Item),
				}
			},
		},
		{
			name: "UpdateItemRewritesNumbersUntouched",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				var returned []any

				for _, rv := range []dynamodbtypes.ReturnValue{
					dynamodbtypes.ReturnValueAllOld, dynamodbtypes.ReturnValueAllNew,
					dynamodbtypes.ReturnValueUpdatedOld, dynamodbtypes.ReturnValueUpdatedNew,
				} {
					out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName:                 aws.String(fuzzTable),
						Key:                       numPrecKey(),
						ReturnValues:              rv,
						UpdateExpression:          aws.String("SET s = :s, c = n, ln = list_append(l, l), ie = if_not_exists(nope, n), mx = m.x"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":s": fzS("v")},
					})
					require.NoError(t, err)

					returned = append(returned, parityNormalizeItemSets(out.Attributes))
				}

				got, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(fuzzTable), Key: numPrecKey()})
				require.NoError(t, err)

				return append(returned, parityNormalizeItemSets(got.Item))
			},
		},
		{
			name: "ConditionCheckFailureReturnsOldItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                           aws.String(fuzzTable),
					Key:                                 numPrecKey(),
					ConditionExpression:                 aws.String("n = :other"),
					UpdateExpression:                    aws.String("SET s = :s"),
					ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
					ExpressionAttributeValues:           map[string]dynamodbtypes.AttributeValue{":s": fzS("v"), ":other": fzN(numPrecNext)},
				})

				var ccf *dynamodbtypes.ConditionalCheckFailedException
				require.ErrorAs(t, err, &ccf)

				return parityNormalizeItemSets(ccf.Item)
			},
		},
		{
			name: "ConditionsCompareExactly",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				var results []string

				for _, c := range []struct{ cond, value string }{
					{"n = :v", numPrecDigits38},
					{"n = :v", numPrecNext},
					{"n <> :v", numPrecNext},
					{"n < :v", numPrecNext},
					{"n >= :v", numPrecNext},
					{"n BETWEEN :v AND :v", numPrecDigits38},
					{"n IN (:v)", numPrecNext},
				} {
					_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName:                 aws.String(fuzzTable),
						Key:                       numPrecKey(),
						ConditionExpression:       aws.String(c.cond),
						UpdateExpression:          aws.String("SET s = :s"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":s": fzS("v"), ":v": fzN(c.value)},
					})

					results = append(results, c.cond+" "+c.value+": "+fuzzErrString(err))
				}

				return results
			},
		},
		{
			name: "ArithmeticIsExact",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				var results []string

				for _, step := range [][2]string{
					{"SET n = n + :v", "1"},
					{"SET n = n - :v", "2"},
					{"ADD n :v", "1"},
					{"SET n = :v", "99999999999999999999999999999999999999"},
					{"SET n = n + :v", "1"},
					{"SET n = :v", "0.1"},
					{"SET n = n + :v", "0.2"},
					{"SET n = :v", "1"},
					{"SET n = n + :v", "1e-37"},
					{"SET n = n + :v", "1e-38"},
					{"ADD n :v", "1e-38"},
					{"SET n = :v", numPrecDigits38},
					{"SET n = n + :v", "0.5"},
					{"SET n = n - :v", "-0.5"},
					// The largest magnitude is 9.99...E+125: a sum or difference past it is an
					// overflow. The values are plain digits, so the text compares as it is.
					{"SET n = :v", "9" + strings.Repeat("0", 125)},
					{"SET n = n + :v", "9" + strings.Repeat("0", 125)},
					{"SET n = n - :v", "-9" + strings.Repeat("0", 125)},
					{"SET n = :v", strings.Repeat("9", 38) + strings.Repeat("0", 88)},
					{"SET n = n + :v", "1" + strings.Repeat("0", 88)},
					{"SET n = n + :v", "1" + strings.Repeat("0", 87)},
				} {
					out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName:                 aws.String(fuzzTable),
						Key:                       numPrecKey(),
						UpdateExpression:          aws.String(step[0]),
						ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": fzN(step[1])},
					})

					result := step[0] + " " + step[1] + ": " + fuzzErrString(err)
					if err == nil {
						result += " " + parityAttrValueString(out.Attributes["n"])
					}

					results = append(results, result)
				}

				return results
			},
		},
		{
			name: "NumberSetOperations",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				numPrecSetup(ctx, t, client)

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(fuzzTable),
					Key:              numPrecKey(),
					UpdateExpression: aws.String("ADD ns :add"),
					ReturnValues:     dynamodbtypes.ReturnValueAllNew,
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":add": &dynamodbtypes.AttributeValueMemberNS{Value: []string{numPrecNext}},
					},
				})
				require.NoError(t, err)

				first := parityNormalizeItemSets(out.Attributes)

				out, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(fuzzTable),
					Key:              numPrecKey(),
					UpdateExpression: aws.String("DELETE ns :del"),
					ReturnValues:     dynamodbtypes.ReturnValueAllNew,
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":del": &dynamodbtypes.AttributeValueMemberNS{Value: []string{numPrecDigits38}},
					},
				})
				require.NoError(t, err)

				return []any{first, parityNormalizeItemSets(out.Attributes)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}

	t.Run("TrailingZerosAreTrimmed", func(t *testing.T) {
		// Intentional divergence (e2e/README.md, "Intentional divergences"): the developer guide
		// says "Leading and trailing zeroes are trimmed" for the Number type. minidyn trims
		// them wherever a number is written. DynamoDB Local keeps the BigDecimal scale in
		// arithmetic results and the text as sent in values UpdateItem writes and in lists
		// and maps. Only the form differs, never the value.
		const (
			minidyn = "minidyn"
			local   = "dynamodb-local"
		)

		want := map[string][]string{
			minidyn: {"N:2.3", "N:1.2", "N:1.5", "N:1", "[N:1]"},
			local:   {"N:2.30", "N:1.20", "N:1.50", "N:1.0", "[N:1.0]"},
		}

		for engine, endpoint := range map[string]func(*testing.T) string{
			minidyn: setupMinidynEndpoint,
			local:   setupDynamoDBLocalEndpoint,
		} {
			t.Run(engine, func(t *testing.T) {
				ctx := context.Background()
				client := newTracedSDKClient(t, endpoint(t), &traceRecorder{})

				item := numPrecKey()
				item["l"] = &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{fzN("1.0")}}
				fuzzSetupFor(ctx, t, client, fuzzSchemaN, item)

				// A top-level PutItem value is normalized by both engines, so x is written by an update.
				_, setErr := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                 aws.String(fuzzTable),
					Key:                       numPrecKey(),
					UpdateExpression:          aws.String("SET x = :x"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":x": fzN("1.10")},
				})
				require.NoError(t, setErr)

				var got []string

				for _, step := range []struct {
					expr, attr string
					values     map[string]dynamodbtypes.AttributeValue
				}{
					{"SET a = :a + :b", "a", map[string]dynamodbtypes.AttributeValue{":a": fzN("1.1"), ":b": fzN("1.20")}},
					{"ADD x :c", "x", map[string]dynamodbtypes.AttributeValue{":c": fzN("0.1")}},
					{"SET b = :d", "b", map[string]dynamodbtypes.AttributeValue{":d": fzN("1.50")}},
					{"SET c = l[0]", "c", nil},
				} {
					out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName:                 aws.String(fuzzTable),
						Key:                       numPrecKey(),
						UpdateExpression:          aws.String(step.expr),
						ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
						ExpressionAttributeValues: step.values,
					})
					require.NoError(t, err, step.expr)

					got = append(got, parityAttrValueString(out.Attributes[step.attr]))
				}

				read, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(fuzzTable), Key: numPrecKey()})
				require.NoError(t, err)

				list, ok := read.Item["l"].(*dynamodbtypes.AttributeValueMemberL)
				require.True(t, ok)
				require.Len(t, list.Value, 1)

				got = append(got, "["+parityAttrValueString(list.Value[0])+"]")

				require.Equal(t, want[engine], got)
			})
		}
	})
}
