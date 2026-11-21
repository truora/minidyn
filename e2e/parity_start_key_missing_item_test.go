package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An ExclusiveStartKey does not have to be an existing item (#258): the search resumes at
// the first key after it in the order the search walks, on the table and on indexes, in
// both directions.
//
// Index entries have distinct index keys here: DynamoDB Local orders entries that share an
// index key by an internal rule (not the table's sort key), so a start key that lands in
// such a run can't be compared.

const startKeyTable = "startkey"

type startKeyAV = dynamodbtypes.AttributeValue

func skS(v string) startKeyAV  { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
func skN(v string) startKeyAV  { return &dynamodbtypes.AttributeValueMemberN{Value: v} }
func skB(v ...byte) startKeyAV { return &dynamodbtypes.AttributeValueMemberB{Value: v} }

// startKeyLabel prints a key attribute, binary values included.
func startKeyLabel(av startKeyAV) string {
	switch v := av.(type) {
	case nil:
		return "-"
	case *dynamodbtypes.AttributeValueMemberS:
		return "S:" + v.Value
	case *dynamodbtypes.AttributeValueMemberN:
		return "N:" + v.Value
	case *dynamodbtypes.AttributeValueMemberB:
		return fmt.Sprintf("B:%x", v.Value)
	}

	return "?"
}

func startKeySig(key map[string]startKeyAV) string {
	parts := make([]string, 0, len(key))
	for attr, v := range key {
		parts = append(parts, attr+"="+startKeyLabel(v))
	}

	sort.Strings(parts)

	return strings.Join(parts, ",")
}

// startKeyCreate creates table "startkey": pk (S) / sk (skType), a local index "by-l" on
// pk / l (S) and a global index "by-g" on g (S) / gr (S).
func startKeyCreate(ctx context.Context, t *testing.T, client *dynamodb.Client, skType dynamodbtypes.ScalarAttributeType) {
	t.Helper()

	def := func(name string, typ dynamodbtypes.ScalarAttributeType) dynamodbtypes.AttributeDefinition {
		return dynamodbtypes.AttributeDefinition{AttributeName: aws.String(name), AttributeType: typ}
	}
	ks := func(name string, typ dynamodbtypes.KeyType) dynamodbtypes.KeySchemaElement {
		return dynamodbtypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: typ}
	}
	all := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(startKeyTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("sk", dynamodbtypes.KeyTypeRange)},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			def("pk", dynamodbtypes.ScalarAttributeTypeS), def("sk", skType), def("l", dynamodbtypes.ScalarAttributeTypeS),
			def("g", dynamodbtypes.ScalarAttributeTypeS), def("gr", dynamodbtypes.ScalarAttributeTypeS),
		},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{{
			IndexName: aws.String("by-l"), Projection: all,
			KeySchema: []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("l", dynamodbtypes.KeyTypeRange)},
		}},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("by-g"), Projection: all,
			KeySchema: []dynamodbtypes.KeySchemaElement{ks("g", dynamodbtypes.KeyTypeHash), ks("gr", dynamodbtypes.KeyTypeRange)},
		}},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, startKeyTable, "by-g")
}

func startKeyPut(ctx context.Context, t *testing.T, client *dynamodb.Client, sk startKeyAV, l, gr string) {
	t.Helper()

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(startKeyTable), Item: map[string]startKeyAV{
		"pk": skS("p"), "sk": sk, "l": skS(l), "g": skS("g"), "gr": skS(gr),
	}})
	require.NoError(t, err)
}

// startKeyWalk runs q in both directions with every start key and returns, per direction and
// key, the sort keys of the items returned (or the error).
func startKeyWalk(ctx context.Context, client *dynamodb.Client, q dynamodb.QueryInput, starts []map[string]startKeyAV) []any {
	var out []any

	for _, forward := range []bool{true, false} {
		for _, start := range starts {
			in := q
			in.ScanIndexForward = aws.Bool(forward)
			in.ExclusiveStartKey = start

			res, err := client.Query(ctx, &in)
			if err != nil {
				out = append(out, forward, startKeySig(start), fuzzErrString(err))

				continue
			}

			sigs := make([]string, 0, len(res.Items))
			for _, it := range res.Items {
				sigs = append(sigs, startKeyLabel(it["sk"]))
			}

			out = append(out, forward, startKeySig(start), sigs, startKeySig(res.LastEvaluatedKey))
		}
	}

	return out
}

func startKeyTableQuery() dynamodb.QueryInput {
	return dynamodb.QueryInput{
		TableName:                 aws.String(startKeyTable),
		KeyConditionExpression:    aws.String("pk = :p"),
		ExpressionAttributeValues: map[string]startKeyAV{":p": skS("p")},
	}
}

func TestE2E_StartKeyMissingItem(t *testing.T) { //nolint:gocognit // one table of independent parity cases
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "TableStringKeys",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

				for _, sk := range []string{"b", "d", "f"} {
					startKeyPut(ctx, t, client, skS(sk), sk, sk)
				}

				starts := []map[string]startKeyAV{}
				for _, sk := range []string{"a", "b", "c", "d", "e", "f", "g", "bb"} {
					starts = append(starts, map[string]startKeyAV{"pk": skS("p"), "sk": skS(sk)})
				}

				return startKeyWalk(ctx, client, startKeyTableQuery(), starts)
			},
		},
		{
			name: "TableDeletedItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

				for _, sk := range []string{"a", "b", "c"} {
					startKeyPut(ctx, t, client, skS(sk), sk, sk)
				}

				_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(startKeyTable),
					Key:       map[string]startKeyAV{"pk": skS("p"), "sk": skS("b")},
				})
				require.NoError(t, err)

				return startKeyWalk(ctx, client, startKeyTableQuery(),
					[]map[string]startKeyAV{{"pk": skS("p"), "sk": skS("b")}})
			},
		},
		{
			name: "TableNumberKeys",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeN)

				for _, sk := range []string{"-5", "2", "10", "100"} {
					startKeyPut(ctx, t, client, skN(sk), sk, sk)
				}

				starts := []map[string]startKeyAV{}
				for _, sk := range []string{"-6", "-5", "0", "2.5", "9.99", "10.0", "50", "1e3"} {
					starts = append(starts, map[string]startKeyAV{"pk": skS("p"), "sk": skN(sk)})
				}

				return startKeyWalk(ctx, client, startKeyTableQuery(), starts)
			},
		},
		{
			name: "TableBinaryKeys",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeB)

				for _, sk := range [][]byte{{1}, {1, 0}, {2}, {255}} {
					startKeyPut(ctx, t, client, skB(sk...), string(sk), string(sk))
				}

				starts := []map[string]startKeyAV{}
				for _, sk := range [][]byte{{0}, {1}, {1, 0, 0}, {1, 5}, {2}, {3}, {255, 0}} {
					starts = append(starts, map[string]startKeyAV{"pk": skS("p"), "sk": skB(sk...)})
				}

				return startKeyWalk(ctx, client, startKeyTableQuery(), starts)
			},
		},
		{
			name: "TableWithLimitAndFilter",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

				for _, sk := range []string{"b", "d", "f", "h"} {
					startKeyPut(ctx, t, client, skS(sk), sk, sk)
				}

				q := startKeyTableQuery()
				q.Limit = aws.Int32(2)
				q.FilterExpression = aws.String("l <> :x")
				q.ExpressionAttributeValues[":x"] = skS("f")

				return startKeyWalk(ctx, client, q, []map[string]startKeyAV{
					{"pk": skS("p"), "sk": skS("c")}, {"pk": skS("p"), "sk": skS("e")},
				})
			},
		},
		{
			name: "Scan",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

				for _, sk := range []string{"b", "d", "f"} {
					startKeyPut(ctx, t, client, skS(sk), sk, sk)
				}

				var out []any

				for _, sk := range []string{"a", "b", "c", "e", "g"} {
					res, err := client.Scan(ctx, &dynamodb.ScanInput{
						TableName:         aws.String(startKeyTable),
						ExclusiveStartKey: map[string]startKeyAV{"pk": skS("p"), "sk": skS(sk)},
					})
					if err != nil {
						out = append(out, sk, fuzzErrString(err))

						continue
					}

					keys := []string{}
					for _, it := range res.Items {
						keys = append(keys, startKeyLabel(it["sk"]))
					}

					out = append(out, sk, keys)
				}

				return out
			},
		},
		{
			name: "LocalIndex",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

				// l order: 1:b 3:a 5:c; table order: a b c.
				startKeyPut(ctx, t, client, skS("b"), "1", "x")
				startKeyPut(ctx, t, client, skS("a"), "3", "x")
				startKeyPut(ctx, t, client, skS("c"), "5", "x")

				q := startKeyTableQuery()
				q.IndexName = aws.String("by-l")

				key := func(sk, l string) map[string]startKeyAV {
					return map[string]startKeyAV{"pk": skS("p"), "sk": skS(sk), "l": skS(l)}
				}

				return startKeyWalk(ctx, client, q, []map[string]startKeyAV{
					key("a", "3"),  // existing
					key("zz", "2"), // no item, index key between
					key("zz", "0"), // before everything
					key("zz", "9"), // after everything
					key("a", "2"),  // item exists, index entry has another l
				})
			},
		},
		{
			name: "GlobalIndex",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

				startKeyPut(ctx, t, client, skS("b"), "x", "1")
				startKeyPut(ctx, t, client, skS("a"), "x", "3")
				startKeyPut(ctx, t, client, skS("c"), "x", "5")
				startKeyPut(ctx, t, client, skS("d"), "x", "7")

				q := dynamodb.QueryInput{
					TableName:                 aws.String(startKeyTable),
					IndexName:                 aws.String("by-g"),
					KeyConditionExpression:    aws.String("g = :g"),
					ExpressionAttributeValues: map[string]startKeyAV{":g": skS("g")},
				}

				key := func(sk, gr string) map[string]startKeyAV {
					return map[string]startKeyAV{"pk": skS("p"), "sk": skS(sk), "g": skS("g"), "gr": skS(gr)}
				}

				return startKeyWalk(ctx, client, q, []map[string]startKeyAV{
					key("a", "3"),
					key("zz", "4"), // no item, between
					key("zz", "0"),
					key("zz", "9"),
					key("a", "2"), // item exists, wrong gr
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}
