package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Minimal reproductions of divergences found by the differential fuzzer (fuzz_test.go).
// They use the fuzz table: pk (S) / sk (S), GSI by-g on g (S) / sk.

func fuzzSetup(ctx context.Context, t *testing.T, client *dynamodb.Client, items ...map[string]dynamodbtypes.AttributeValue) {
	t.Helper()

	require.NoError(t, fuzzCreateTable(ctx, client))
	parityWaitUntilGSIActive(ctx, t, client, fuzzTable, fuzzGSI)

	for _, it := range items {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: it})
		require.NoError(t, err)
	}
}

// fuzzSetupFor is fuzzSetup with a key layout (#225: N and B range keys).
func fuzzSetupFor(ctx context.Context, t *testing.T, client *dynamodb.Client, schema fuzzSchema, items ...map[string]dynamodbtypes.AttributeValue) {
	t.Helper()

	require.NoError(t, fuzzCreateTableFor(ctx, client, schema))
	parityWaitUntilGSIActive(ctx, t, client, fuzzTable, fuzzGSI)

	for _, it := range items {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: it})
		require.NoError(t, err)
	}
}

func fuzzKey(pk, sk string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"pk": fzS(pk), "sk": fzS(sk)}
}

// fuzzErrString returns the normalized error, or "ok".
func fuzzErrString(err error) string {
	if err == nil {
		return "ok"
	}

	return normalizeSDKErrorString(err.Error())
}

func indexKeys(items []map[string]dynamodbtypes.AttributeValue) []string {
	keys := make([]string, 0, len(items))
	for _, it := range items {
		keys = append(keys, parityAttrValueString(it["pk"])+"/"+parityAttrValueString(it["sk"]))
	}

	return keys
}

func TestE2E_FuzzFindings(t *testing.T) {
	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "UpdateItemAddsIndexKey",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, fuzzKey("a", "1"), fuzzKey("b", "2"))

				for _, k := range []map[string]dynamodbtypes.AttributeValue{fuzzKey("a", "1"), fuzzKey("b", "2")} {
					_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName:                 aws.String(fuzzTable),
						Key:                       k,
						UpdateExpression:          aws.String("SET g = :g"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":g": fzS("g1")},
					})
					require.NoError(t, err)
				}

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(fuzzTable),
					IndexName:                 aws.String(fuzzGSI),
					KeyConditionExpression:    aws.String("g = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":g": fzS("g1")},
				})
				require.NoError(t, err)

				keys := make([]string, 0, len(out.Items))
				for _, it := range out.Items {
					keys = append(keys, parityAttrValueString(it["pk"])+"/"+parityAttrValueString(it["sk"]))
				}

				return keys
			},
		},
		{
			name: "DeleteFromMissingSet",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, fuzzKey("a", "1"))

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(fuzzTable),
					Key:              fuzzKey("a", "1"),
					UpdateExpression: aws.String("DELETE ss :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x"}},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.NoError(t, err)

				_, hasSet := out.Attributes["ss"]

				return hasSet
			},
		},
		{
			name: "DeleteLastElementsRemovesSet",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				item := fuzzKey("a", "1")
				item["ss"] = &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x"}}
				fuzzSetup(ctx, t, client, item)

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(fuzzTable),
					Key:              fuzzKey("a", "1"),
					UpdateExpression: aws.String("DELETE ss :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x"}},
					},
					ReturnValues: dynamodbtypes.ReturnValueAllNew,
				})
				require.NoError(t, err)

				_, hasSet := out.Attributes["ss"]

				return hasSet
			},
		},
		{
			name: "EmptyStringKeyConditionValue",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client)

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(fuzzTable),
					IndexName:              aws.String(fuzzGSI),
					KeyConditionExpression: aws.String("g = :g AND begins_with(sk, :p)"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":g": fzS("g1"),
						":p": fzS(""),
					},
				})

				return fuzzErrString(err)
			},
		},
		{
			name:       "InvalidExpressionsRejected",
			knownIssue: "#164",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, map[string]dynamodbtypes.AttributeValue{
					"pk": fzS("a"), "sk": fzS("1"), "g": fzS("g1"), "n": fzN("1"),
				})

				var out []string

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:           aws.String(fuzzTable),
					Key:                 fuzzKey("a", "1"),
					UpdateExpression:    aws.String("SET n = :v"),
					ConditionExpression: aws.String("((attribute_exists(n)))"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": fzN("2"),
					},
				})
				out = append(out, "redundant parentheses: "+fuzzErrString(err))

				_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(fuzzTable),
					Key:              fuzzKey("a", "1"),
					UpdateExpression: aws.String("ADD n :a, n :b"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":a": fzN("1"), ":b": fzN("2"),
					},
				})
				out = append(out, "overlapping paths: "+fuzzErrString(err))

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:           aws.String(fuzzTable),
					Item:                fuzzKey("a", "2"),
					ConditionExpression: aws.String("n BETWEEN :lo AND :hi"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":lo": fzN("1"), ":hi": fzN("0"),
					},
				})
				out = append(out, "between bounds: "+fuzzErrString(err))

				_, err = client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(fuzzTable),
					IndexName:              aws.String(fuzzGSI),
					KeyConditionExpression: aws.String("g = :g"),
					FilterExpression:       aws.String("attribute_exists(g)"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":g": fzS("g1"),
					},
				})
				out = append(out, "filter on key: "+fuzzErrString(err))

				return out
			},
		},
		{
			name: "UpdatedOldIncludesUnchangedAttributes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, map[string]dynamodbtypes.AttributeValue{
					"pk": fzS("a"), "sk": fzS("1"), "n": fzN("1"), "s": fzS("foo"),
				})

				var out []any

				for _, rv := range []dynamodbtypes.ReturnValue{dynamodbtypes.ReturnValueUpdatedOld, dynamodbtypes.ReturnValueUpdatedNew} {
					res, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
						TableName:        aws.String(fuzzTable),
						Key:              fuzzKey("a", "1"),
						UpdateExpression: aws.String("SET n = if_not_exists(n, :v), s = :s"),
						ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
							":v": fzN("2"), ":s": fzS("foo"),
						},
						ReturnValues: rv,
					})
					require.NoError(t, err)

					out = append(out, parityAttrValueString(res.Attributes["n"])+" "+parityAttrValueString(res.Attributes["s"]))
				}

				return out
			},
		},
		{
			name: "ConditionFunctionOnMissingAttribute",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, fuzzKey("a", "1"))

				values := map[string]dynamodbtypes.AttributeValue{":n": fzN("0"), ":x": fzS("x")}
				conds := []string{"size(l) > :n", "contains(ss, :x)", "begins_with(s, :x)"}

				var out []string

				// a/1 exists without l, ss or s; a/9 does not exist.
				for _, sk := range []string{"1", "9"} {
					for _, cond := range conds {
						_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: aws.String(fuzzTable), Item: fuzzKey("a", sk),
							ConditionExpression: aws.String(cond), ExpressionAttributeValues: values,
						})
						out = append(out, "PutItem a/"+sk+" "+cond+": "+fuzzErrString(err))

						_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
							TableName: aws.String(fuzzTable), Key: fuzzKey("a", sk),
							UpdateExpression:    aws.String("SET n = :n"),
							ConditionExpression: aws.String(cond), ExpressionAttributeValues: values,
						})
						out = append(out, "UpdateItem a/"+sk+" "+cond+": "+fuzzErrString(err))

						_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
							TableName: aws.String(fuzzTable), Key: fuzzKey("a", sk),
							ConditionExpression: aws.String(cond), ExpressionAttributeValues: values,
						})
						out = append(out, "DeleteItem a/"+sk+" "+cond+": "+fuzzErrString(err))
					}
				}

				return out
			},
		},
		{
			// Intentional divergence (#153, closed): DynamoDB Local omits LastEvaluatedKey when a
			// full page ends on the sort key range bound; minidyn returns it, as documented for
			// DynamoDB. See "Intentional divergences" in e2e/README.md.
			name:       "QueryLimitEndsAtRangeEnd",
			knownIssue: "#153",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, fuzzKey("a", "1"), fuzzKey("a", "2"), fuzzKey("a", "3"))

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(fuzzTable),
					KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":p": fzS("a"), ":lo": fzS("1"), ":hi": fzS("2"),
					},
					Limit: aws.Int32(2),
				})
				require.NoError(t, err)

				return []any{len(out.Items), parityKeySignature(out.LastEvaluatedKey)}
			},
		},
		{
			// #177: indexing past the end of a list used to panic.
			name: "ListIndexPastTheEnd",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				item := fuzzKey("a", "1")
				item["l"] = &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{}}
				item["l1"] = &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{fzS("x")}}
				fuzzSetup(ctx, t, client, item)

				got, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName:            aws.String(fuzzTable),
					Key:                  fuzzKey("a", "1"),
					ProjectionExpression: aws.String("sk, l[0], l1[1]"),
				})
				require.NoError(t, err)

				_, condErr := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                 aws.String(fuzzTable),
					Key:                       fuzzKey("a", "1"),
					UpdateExpression:          aws.String("SET x = :v"),
					ConditionExpression:       aws.String("l[0] = :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": fzS("x")},
				})

				return []any{len(got.Item), parityAttrValueString(got.Item["sk"]), fuzzErrString(condErr)}
			},
		},
		{
			name: "PutItemDropsIndexKey",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				item := fuzzKey("a", "1")
				item["g"] = fzS("g1")
				fuzzSetup(ctx, t, client, item)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(fuzzTable),
					Item:      fuzzKey("a", "1"),
				})
				require.NoError(t, err)

				scan, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(fuzzTable),
					IndexName: aws.String(fuzzGSI),
				})
				require.NoError(t, err)

				query, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(fuzzTable),
					IndexName:                 aws.String(fuzzGSI),
					KeyConditionExpression:    aws.String("g = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":g": fzS("g1")},
				})
				require.NoError(t, err)

				return []any{scan.Count, indexKeys(scan.Items), query.Count, indexKeys(query.Items)}
			},
		},
		{
			name: "UpdateItemRemovesIndexKey",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				item := fuzzKey("a", "1")
				item["g"] = fzS("g1")
				fuzzSetup(ctx, t, client, item)

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:        aws.String(fuzzTable),
					Key:              fuzzKey("a", "1"),
					UpdateExpression: aws.String("REMOVE g"),
				})
				require.NoError(t, err)

				scan, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(fuzzTable),
					IndexName: aws.String(fuzzGSI),
				})
				require.NoError(t, err)

				query, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(fuzzTable),
					IndexName:                 aws.String(fuzzGSI),
					KeyConditionExpression:    aws.String("g = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":g": fzS("g1")},
				})
				require.NoError(t, err)

				return []any{scan.Count, indexKeys(scan.Items), query.Count, indexKeys(query.Items)}
			},
		},
		{
			// DELETE and ADD on nested document paths were silently ignored (#184).
			name: "NestedDeleteAndAdd",
			fn:   fuzzNestedDeleteAdd,
		},
		{
			// DynamoDB rejects an ExclusiveStartKey outside the range key predicate
			// ("The provided starting key does not match the range key predicate", #171).
			name: "ExclusiveStartKeyOutsideRangePredicate",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, fuzzKey("a", "1"), fuzzKey("a", "2"), fuzzKey("a", "3"))

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(fuzzTable),
					KeyConditionExpression:    aws.String("pk = :p AND sk > :s"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": fzS("a"), ":s": fzS("2")},
					ExclusiveStartKey:         fuzzKey("a", "1"),
				})

				return []any{fuzzErrString(err), out != nil && len(out.Items) > 0}
			},
		},
		{
			// An ExclusiveStartKey that is not an item in the table resumes at the next
			// larger key.
			name: "ExclusiveStartKeyOfMissingItem",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, fuzzKey("c", "1"), fuzzKey("c", "x.y"))

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(fuzzTable),
					KeyConditionExpression:    aws.String("pk = :p"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":p": fzS("c")},
					ExclusiveStartKey:         fuzzKey("c", "x"),
				})
				require.NoError(t, err)

				return indexKeys(out.Items)
			},
		},
		{
			// A 38-digit N key written with PutItem reads back with 17 significant digits
			// when projected.
			name: "ProjectedNumberKeyKeepsPrecision",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				key := map[string]dynamodbtypes.AttributeValue{"pk": fzS("c"), "sk": fzN("12345678901234567890123456789012345678")}
				fuzzSetupFor(ctx, t, client, fuzzSchemaN, key)

				full, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(fuzzTable), Key: key})
				require.NoError(t, err)

				projected, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(fuzzTable), Key: key, ProjectionExpression: aws.String("sk"),
				})
				require.NoError(t, err)

				return []string{parityAttrValueString(full.Item["sk"]), parityAttrValueString(projected.Item["sk"])}
			},
		},
		{
			// UpdateItem that creates an item writes its N key with 17 significant digits.
			name: "UpdateItemCreatedNumberKeyKeepsPrecision",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				fuzzSetupFor(ctx, t, client, fuzzSchemaN)

				key := map[string]dynamodbtypes.AttributeValue{"pk": fzS("a"), "sk": fzN("12345678901234567890123456789012345678")}
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                 aws.String(fuzzTable),
					Key:                       key,
					UpdateExpression:          aws.String("SET s = :s"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":s": fzS("v")},
				})
				require.NoError(t, err)

				out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(fuzzTable)})
				require.NoError(t, err)

				return parityAttrValueString(out.Items[0]["sk"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, tt.fn)
		})
	}
}
