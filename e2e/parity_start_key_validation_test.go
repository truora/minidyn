package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An ExclusiveStartKey is validated before any item is read (#171): its shape, its types, empty
// values and, for a Query, the key condition. Every case compares DynamoDB Local's answer, the
// items returned when the start key is accepted or the error text when it is not.
//
// Table "startkey" (see startKeyCreate) has pk (S) / sk (S), a local index "by-l" on pk / l
// and a global index "by-g" on g / gr. It holds p/a, p/b and p/c, whose l is 3, 2 and 1 and
// whose gr is 1, 2 and 3, all with g = "g", and q/a (l 1, gr 4) in another partition. Every
// index entry has its own index key: DynamoDB Local orders entries that share one by an internal
// rule. A Scan reports only whether the start key is accepted, as its order is unspecified.

type skAttrs = map[string]startKeyAV

func skSetup(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeS)

	for _, it := range []struct{ pk, sk, l, gr string }{
		{"p", "a", "3", "1"}, {"p", "b", "2", "2"}, {"p", "c", "1", "3"}, {"q", "a", "1", "4"},
	} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(startKeyTable), Item: skAttrs{
			"pk": skS(it.pk), "sk": skS(it.sk), "l": skS(it.l), "g": skS("g"), "gr": skS(it.gr),
		}})
		require.NoError(t, err)
	}
}

// skResult is what a Query or Scan returned: the keys of its items, or the error.
func skResult(items []map[string]startKeyAV, err error) string {
	if err != nil {
		return fuzzErrString(err)
	}

	keys := make([]string, 0, len(items))
	for _, it := range items {
		keys = append(keys, startKeyLabel(it["pk"])+"/"+startKeyLabel(it["sk"]))
	}

	return fmt.Sprint(keys)
}

type skProbe struct {
	name string
	in   dynamodb.QueryInput
}

// skRun runs every probe in both directions and returns what each one answered.
func skRun(ctx context.Context, client *dynamodb.Client, probes []skProbe) []any {
	var out []any

	for _, p := range probes {
		for _, forward := range []bool{true, false} {
			in := p.in
			in.TableName = aws.String(startKeyTable)
			in.ScanIndexForward = aws.Bool(forward)

			res, err := client.Query(ctx, &in)

			var items []map[string]startKeyAV
			if res != nil {
				items = res.Items
			}

			out = append(out, fmt.Sprintf("%s forward=%v", p.name, forward), skResult(items, err))
		}
	}

	return out
}

// skScan runs every start key as a Scan, on the table or on an index.
func skScan(ctx context.Context, client *dynamodb.Client, index string, starts map[string]skAttrs, order []string) []any {
	var out []any

	for _, name := range order {
		in := dynamodb.ScanInput{TableName: aws.String(startKeyTable), ExclusiveStartKey: starts[name]}
		if index != "" {
			in.IndexName = aws.String(index)
		}

		res, err := client.Scan(ctx, &in)

		var items []map[string]startKeyAV
		if res != nil {
			items = res.Items
		}

		out = append(out, name, skScanResult(items, err))
	}

	return out
}

// skScanResult is "ok" or the error of a Scan: its order, so which items follow a start key, is
// unspecified.
func skScanResult(_ []map[string]startKeyAV, err error) string {
	if err != nil {
		return fuzzErrString(err)
	}

	return "ok"
}

func skTableProbe(name string, start skAttrs, cond string, extra skAttrs) skProbe {
	values := skAttrs{":p": skS("p")}
	for k, v := range extra {
		values[k] = v
	}

	return skProbe{name, dynamodb.QueryInput{
		KeyConditionExpression: aws.String(cond), ExpressionAttributeValues: values, ExclusiveStartKey: start,
	}}
}

func skIndexProbe(name, index, cond string, hash startKeyAV, start skAttrs, extra skAttrs) skProbe {
	values := skAttrs{":p": hash}
	for k, v := range extra {
		values[k] = v
	}

	return skProbe{name, dynamodb.QueryInput{
		IndexName: aws.String(index), KeyConditionExpression: aws.String(cond),
		ExpressionAttributeValues: values, ExclusiveStartKey: start,
	}}
}

func skMerge(base skAttrs, over skAttrs) skAttrs {
	out := skAttrs{}
	for k, v := range base {
		out[k] = v
	}

	for k, v := range over {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = v
		}
	}

	return out
}

func skEmpty() startKeyAV { return skS("") }

// skRangeConditions are the key conditions on the range key attr that the probes use, the
// partition key being ":p".
func skRangeConditions(hash, attr string) []string {
	return []string{
		hash + " = :p AND " + attr + " > :s", hash + " = :p AND " + attr + " >= :s", hash + " = :p AND " + attr + " < :s", hash + " = :p AND " + attr + " <= :s",
		hash + " = :p AND " + attr + " = :s", hash + " = :p AND " + attr + " BETWEEN :s AND :t", hash + " = :p AND begins_with(" + attr + ", :s)",
		hash + " = :p AND :s >= " + attr, hash + " = :p AND :s <= " + attr,
	}
}

// skTableConditions are the key conditions TableRangePredicate probes, with the bounds ":s" = b
// and ":t" = d.
var skTableConditions = []string{
	"pk = :p AND sk > :s", "pk = :p AND sk >= :s", "pk = :p AND sk < :s", "pk = :p AND sk <= :s",
	"pk = :p AND sk = :s", "pk = :p AND sk BETWEEN :s AND :t", "pk = :p AND begins_with(sk, :s)",
	"pk = :p AND :s >= sk", "pk = :p AND :s <= sk",
}

// skAtLastBound reports whether sk is, in one of the two directions, the inclusive bound a walk
// of cond (see skTableConditions) ends at: the upper one going forward, the lower one going
// backward.
func skAtLastBound(cond, sk string) bool {
	switch cond {
	case "pk = :p AND sk >= :s", "pk = :p AND sk <= :s", "pk = :p AND :s >= sk", "pk = :p AND :s <= sk":
		return sk == "b"
	case "pk = :p AND sk BETWEEN :s AND :t":
		return sk == "b" || sk == "d"
	}

	return false
}

func TestE2E_StartKeyValidation(t *testing.T) { //nolint:gocognit // one table of independent parity cases
	tableKey := skAttrs{"pk": skS("p"), "sk": skS("a")}
	lsiKey := skAttrs{"pk": skS("p"), "sk": skS("a"), "l": skS("3")}
	gsiKey := skAttrs{"pk": skS("p"), "sk": skS("a"), "g": skS("g"), "gr": skS("1")}

	tests := []struct {
		name       string
		knownIssue string
		fn         func(ctx context.Context, t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "TableShape",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				q := func(name string, start skAttrs) skProbe { return skTableProbe(name, start, "pk = :p", nil) }

				return skRun(ctx, client, []skProbe{
					q("existing item", tableKey),
					q("no such item", skAttrs{"pk": skS("p"), "sk": skS("zz")}),
					q("another partition", skAttrs{"pk": skS("q"), "sk": skS("a")}),
					q("missing range key", skAttrs{"pk": skS("p")}),
					q("missing hash key", skAttrs{"sk": skS("a")}),
					q("placeholder", skAttrs{"lastkey": skS("value")}),
					q("extra attribute", skMerge(tableKey, skAttrs{"x": skS("1")})),
					q("index key attribute", skMerge(tableKey, skAttrs{"l": skS("1")})),
					q("same size, range key missing", skAttrs{"pk": skS("p"), "x": skS("1")}),
					q("same size, hash key missing", skAttrs{"x": skS("p"), "sk": skS("a")}),
					q("same size, hash key missing and empty", skAttrs{"pk": skEmpty(), "x": skS("1")}),
				})
			},
		},
		{
			name: "TableTypes",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				q := func(name string, start skAttrs) skProbe { return skTableProbe(name, start, "pk = :p", nil) }

				return skRun(ctx, client, []skProbe{
					q("range key N", skAttrs{"pk": skS("p"), "sk": skN("1")}),
					q("hash key N", skAttrs{"pk": skN("1"), "sk": skS("a")}),
					q("range key B", skAttrs{"pk": skS("p"), "sk": skB(1)}),
					q("range key BOOL", skAttrs{"pk": skS("p"), "sk": &dynamodbtypes.AttributeValueMemberBOOL{Value: true}}),
					q("range key NULL", skAttrs{"pk": skS("p"), "sk": &dynamodbtypes.AttributeValueMemberNULL{Value: true}}),
					q("range key L", skAttrs{"pk": skS("p"), "sk": &dynamodbtypes.AttributeValueMemberL{Value: []startKeyAV{}}}),
					q("range key SS", skAttrs{"pk": skS("p"), "sk": &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a"}}}),
					q("wrong type in another partition", skAttrs{"pk": skS("q"), "sk": skN("1")}),
					q("both wrong", skAttrs{"pk": skN("1"), "sk": skN("1")}),
					q("wrong type and a missing attribute", skAttrs{"pk": skN("1")}),
					q("wrong type and an extra attribute", skMerge(skAttrs{"pk": skS("p"), "sk": skN("1")}, skAttrs{"x": skS("1")})),
					// The attributes are checked in turn: the first problem found is reported.
					q("hash key wrong, range key missing", skAttrs{"pk": skN("1"), "x": skS("1")}),
					q("hash key missing, range key wrong", skAttrs{"x": skS("1"), "sk": skN("1")}),
					q("hash key empty, range key wrong", skAttrs{"pk": skEmpty(), "sk": skN("1")}),
					q("hash key wrong, range key empty", skAttrs{"pk": skN("1"), "sk": skEmpty()}),
					q("number out of range", skAttrs{"pk": skN("1e999"), "sk": skS("a")}),
					q("number out of range in an extra attribute", skAttrs{"lastkey": skN("1e999")}),
				})
			},
		},
		{
			name: "TableEmptyValues",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				q := func(name string, start skAttrs) skProbe { return skTableProbe(name, start, "pk = :p", nil) }

				return skRun(ctx, client, []skProbe{
					q("empty range key", skAttrs{"pk": skS("p"), "sk": skEmpty()}),
					q("empty hash key", skAttrs{"pk": skEmpty(), "sk": skS("a")}),
					q("both empty", skAttrs{"pk": skEmpty(), "sk": skEmpty()}),
					q("empty range key, another partition", skAttrs{"pk": skS("q"), "sk": skEmpty()}),
					q("empty range key and a missing attribute", skAttrs{"sk": skEmpty()}),
					q("empty range key and an extra attribute", skMerge(skAttrs{"pk": skS("p"), "sk": skEmpty()}, skAttrs{"x": skS("1")})),
				})
			},
		},
		{
			name: "TableRangePredicate",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				var probes []skProbe

				bounds := skAttrs{":s": skS("b"), ":t": skS("d")}

				for _, cond := range skTableConditions {
					for _, sk := range []string{"a", "b", "bb", "c", "d", "e"} {
						if skAtLastBound(cond, sk) {
							continue
						}

						probes = append(probes, skTableProbe(cond+" start "+sk,
							skAttrs{"pk": skS("p"), "sk": skS(sk)}, cond, bounds))
					}
				}

				q := "pk = :p AND sk > :s"
				probes = append(probes,
					skTableProbe("another partition above the range", skAttrs{"pk": skS("q"), "sk": skS("c")}, q, bounds),
					skTableProbe("another partition below the range", skAttrs{"pk": skS("q"), "sk": skS("a")}, q, bounds),
					skTableProbe("range and a missing range key", skAttrs{"pk": skS("p")}, q, bounds),
					skTableProbe("range and an extra attribute", skMerge(tableKey, skAttrs{"x": skS("1")}), q, bounds),
					skTableProbe("range and a wrong type", skAttrs{"pk": skS("p"), "sk": skN("1")}, q, bounds),
					skTableProbe("range and an empty value", skAttrs{"pk": skS("p"), "sk": skEmpty()}, q, bounds),
					skTableProbe("range and a placeholder", skAttrs{"lastkey": skS("v")}, q, bounds),
				)

				return skRun(ctx, client, probes)
			},
		},
		{
			// Local rejects a start key equal to the inclusive bound the walk ends at, as nothing
			// is left to read after it. It never returns one: it leaves out the LastEvaluatedKey
			// of a page that ends there. minidyn does return it (#153), so it accepts it back.
			name:       "TableRangePredicateAtBound",
			knownIssue: "#153",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				var probes []skProbe

				bounds := skAttrs{":s": skS("b"), ":t": skS("d")}

				for _, cond := range skTableConditions {
					for _, sk := range []string{"b", "d"} {
						if skAtLastBound(cond, sk) {
							probes = append(probes, skTableProbe(cond+" start "+sk,
								skAttrs{"pk": skS("p"), "sk": skS(sk)}, cond, bounds))
						}
					}
				}

				return skRun(ctx, client, probes)
			},
		},
		{
			name: "BinaryKeyTable",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeB)

				for i, sk := range []byte{1, 2, 3} {
					startKeyPut(ctx, t, client, skB(sk), fmt.Sprint(3-i), fmt.Sprint(i+1))
				}

				q := func(name string, start skAttrs) skProbe { return skTableProbe(name, start, "pk = :p", nil) }
				r := func(name, cond string, start skAttrs) skProbe {
					return skTableProbe(name, start, cond, skAttrs{":s": skB(2), ":t": skB(3)})
				}
				key := func(sk startKeyAV) skAttrs { return skAttrs{"pk": skS("p"), "sk": sk} }

				return skRun(ctx, client, []skProbe{
					q("existing item", key(skB(1))),
					q("no such item", key(skB(9))),
					q("another partition", skAttrs{"pk": skS("q"), "sk": skB(1)}),
					q("missing range key", skAttrs{"pk": skS("p")}),
					q("range key S", key(skS("a"))),
					q("range key N", key(skN("1"))),
					q("hash key B", skAttrs{"pk": skB(1), "sk": skB(1)}),
					q("extra attribute", skMerge(key(skB(1)), skAttrs{"x": skS("1")})),
					r("range, start below", "pk = :p AND sk > :s", key(skB(1))),
					r("range, start at the bound", "pk = :p AND sk > :s", key(skB(2))),
					r("range, start above", "pk = :p AND sk > :s", key(skB(3))),
					r("between, start inside", "pk = :p AND sk BETWEEN :s AND :t", key(skB(2, 5))),
					r("between, start above", "pk = :p AND sk BETWEEN :s AND :t", key(skB(4))),
					r("begins_with, start inside", "pk = :p AND begins_with(sk, :s)", key(skB(2, 1))),
					r("begins_with, start outside", "pk = :p AND begins_with(sk, :s)", key(skB(1))),
					r("range, wrong type", "pk = :p AND sk > :s", key(skS("a"))),
				})
			},
		},
		{
			// The index key is the table's, swapped: the start key names pk and sk once each, so
			// it has two attributes, not four.
			name: "InvertedGlobalIndex",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				def := func(name string) dynamodbtypes.AttributeDefinition {
					return dynamodbtypes.AttributeDefinition{AttributeName: aws.String(name), AttributeType: dynamodbtypes.ScalarAttributeTypeS}
				}
				ks := func(name string, typ dynamodbtypes.KeyType) dynamodbtypes.KeySchemaElement {
					return dynamodbtypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: typ}
				}

				_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName:            aws.String(startKeyTable),
					KeySchema:            []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("sk", dynamodbtypes.KeyTypeRange)},
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{def("pk"), def("sk")},
					GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
						IndexName:  aws.String("by-inv"),
						Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
						KeySchema:  []dynamodbtypes.KeySchemaElement{ks("sk", dynamodbtypes.KeyTypeHash), ks("pk", dynamodbtypes.KeyTypeRange)},
					}},
					BillingMode: dynamodbtypes.BillingModePayPerRequest,
				})
				require.NoError(t, err)

				parityWaitUntilGSIActive(ctx, t, client, startKeyTable, "by-inv")

				for _, it := range [][2]string{{"p", "a"}, {"q", "a"}, {"r", "a"}, {"p", "b"}} {
					_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(startKeyTable), Item: skAttrs{
						"pk": skS(it[0]), "sk": skS(it[1]),
					}})
					require.NoError(t, err)
				}

				q := func(name string, start skAttrs) skProbe {
					return skIndexProbe(name, "by-inv", "sk = :p", skS("a"), start, nil)
				}
				rq := func(name string, start skAttrs) skProbe {
					return skIndexProbe(name, "by-inv", "sk = :p AND pk > :s", skS("a"), start, skAttrs{":s": skS("p")})
				}

				return skRun(ctx, client, []skProbe{
					q("existing entry", skAttrs{"pk": skS("q"), "sk": skS("a")}),
					q("no such item", skAttrs{"pk": skS("zz"), "sk": skS("a")}),
					q("another index partition", skAttrs{"pk": skS("p"), "sk": skS("b")}),
					q("missing pk", skAttrs{"sk": skS("a")}),
					q("missing sk", skAttrs{"pk": skS("q")}),
					q("placeholder", skAttrs{"lastkey": skS("v")}),
					q("same size, pk missing", skAttrs{"x": skS("q"), "sk": skS("a")}),
					q("same size, sk missing", skAttrs{"pk": skS("q"), "x": skS("a")}),
					q("extra attribute", skAttrs{"pk": skS("q"), "sk": skS("a"), "x": skS("1")}),
					q("pk wrong type", skAttrs{"pk": skN("1"), "sk": skS("a")}),
					q("sk wrong type", skAttrs{"pk": skS("q"), "sk": skN("1")}),
					q("empty pk", skAttrs{"pk": skEmpty(), "sk": skS("a")}),
					q("empty sk", skAttrs{"pk": skS("q"), "sk": skEmpty()}),
					rq("range, start below", skAttrs{"pk": skS("p"), "sk": skS("a")}),
					rq("range, start above", skAttrs{"pk": skS("q"), "sk": skS("a")}),
					rq("range, another index partition", skAttrs{"pk": skS("r"), "sk": skS("b")}),
				})
			},
		},
		{
			name: "HashOnlyTable",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String(startKeyTable),
					KeySchema: []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					BillingMode: dynamodbtypes.BillingModePayPerRequest,
				})
				require.NoError(t, err)

				for _, pk := range []string{"p", "q"} {
					_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(startKeyTable), Item: skAttrs{"pk": skS(pk)}})
					require.NoError(t, err)
				}

				var out []any

				for _, start := range []skAttrs{
					{"pk": skS("p")},
					{"pk": skS("q")},
					{"pk": skS("p"), "sk": skS("a")},
					{"x": skS("p")},
					{"pk": skN("1")},
					{"pk": skEmpty()},
					{"pk": skS("p"), "x": skEmpty()},
				} {
					res, qerr := client.Query(ctx, &dynamodb.QueryInput{
						TableName: aws.String(startKeyTable), KeyConditionExpression: aws.String("pk = :p"),
						ExpressionAttributeValues: skAttrs{":p": skS("p")}, ExclusiveStartKey: start,
					})

					var items []map[string]startKeyAV
					if res != nil {
						items = res.Items
					}

					out = append(out, "query "+startKeySig(start), skResult(items, qerr))

					sres, serr := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(startKeyTable), ExclusiveStartKey: start})

					items = nil
					if sres != nil {
						items = sres.Items
					}

					out = append(out, "scan "+startKeySig(start), skScanResult(items, serr))
				}

				return out
			},
		},
		{
			name: "LocalIndex",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				q := func(name string, start skAttrs) skProbe {
					return skIndexProbe(name, "by-l", "pk = :p", skS("p"), start, nil)
				}
				lq := func(name string, start skAttrs, cond string, v string) skProbe {
					return skIndexProbe(name, "by-l", cond, skS("p"), start, skAttrs{":s": skS(v)})
				}

				return skRun(ctx, client, []skProbe{
					q("existing entry", lsiKey),
					q("no such item", skAttrs{"pk": skS("p"), "sk": skS("zz"), "l": skS("25")}),
					q("another partition", skAttrs{"pk": skS("q"), "sk": skS("a"), "l": skS("1")}),
					q("missing index key", tableKey),
					q("missing range key", skAttrs{"pk": skS("p"), "l": skS("3")}),
					q("only the hash key", skAttrs{"pk": skS("p")}),
					q("placeholder", skAttrs{"lastkey": skS("v")}),
					q("extra attribute", skMerge(lsiKey, skAttrs{"x": skS("1")})),
					q("global index key attribute", skMerge(lsiKey, skAttrs{"g": skS("g")})),
					q("same size, index key missing", skAttrs{"pk": skS("p"), "sk": skS("a"), "x": skS("1")}),
					q("same size, range key missing", skAttrs{"pk": skS("p"), "l": skS("3"), "x": skS("1")}),
					q("same size, hash key missing", skAttrs{"x": skS("p"), "sk": skS("a"), "l": skS("3")}),
					q("index key wrong type", skMerge(lsiKey, skAttrs{"l": skN("3")})),
					q("range key wrong type", skMerge(lsiKey, skAttrs{"sk": skN("1")})),
					q("range key wrong, index key missing", skAttrs{"pk": skS("p"), "sk": skN("1"), "x": skS("1")}),
					q("range key missing, index key wrong", skAttrs{"pk": skS("p"), "x": skS("1"), "l": skN("1")}),
					q("index key empty", skMerge(lsiKey, skAttrs{"l": skEmpty()})),
					q("range key empty", skMerge(lsiKey, skAttrs{"sk": skEmpty()})),
					q("hash key empty", skMerge(lsiKey, skAttrs{"pk": skEmpty()})),
					q("range and index keys empty", skMerge(lsiKey, skAttrs{"sk": skEmpty(), "l": skEmpty()})),
					q("range key empty, index key wrong", skMerge(lsiKey, skAttrs{"sk": skEmpty(), "l": skN("1")})),
					q("index key empty and a missing attribute", skAttrs{"pk": skS("p"), "x": skS("1"), "l": skEmpty()}),
					lq("index range, start below", skAttrs{"pk": skS("p"), "sk": skS("c"), "l": skS("1")}, "pk = :p AND l > :s", "2"),
					lq("index range, start inside", lsiKey, "pk = :p AND l > :s", "2"),
					lq("index range, start at the bound", skAttrs{"pk": skS("p"), "sk": skS("b"), "l": skS("2")}, "pk = :p AND l >= :s", "2"),
					lq("index range, upper bound", skAttrs{"pk": skS("p"), "sk": skS("b"), "l": skS("2")}, "pk = :p AND l <= :s", "2"),
					lq("index range, another partition", skAttrs{"pk": skS("q"), "sk": skS("a"), "l": skS("3")}, "pk = :p AND l > :s", "2"),
					lq("index range, missing index key", tableKey, "pk = :p AND l > :s", "2"),
					lq("index begins_with, outside", skAttrs{"pk": skS("p"), "sk": skS("c"), "l": skS("1")}, "pk = :p AND begins_with(l, :s)", "3"),
				})
			},
		},
		{
			name: "GlobalIndex",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				q := func(name string, start skAttrs) skProbe {
					return skIndexProbe(name, "by-g", "g = :p", skS("g"), start, nil)
				}
				gq := func(name string, start skAttrs, cond string, v string) skProbe {
					return skIndexProbe(name, "by-g", cond, skS("g"), start, skAttrs{":s": skS(v)})
				}
				without := func(attrs ...string) skAttrs {
					over := skAttrs{}
					for _, a := range attrs {
						over[a] = nil
					}

					return skMerge(gsiKey, over)
				}

				return skRun(ctx, client, []skProbe{
					q("existing entry", gsiKey),
					q("no such item", skAttrs{"pk": skS("p"), "sk": skS("zz"), "g": skS("g"), "gr": skS("25")}),
					q("table key of another item", skAttrs{"pk": skS("q"), "sk": skS("a"), "g": skS("g"), "gr": skS("4")}),
					q("another index partition", skMerge(gsiKey, skAttrs{"g": skS("g2")})),
					q("missing table hash key", without("pk")),
					q("missing table range key", without("sk")),
					q("missing table key", without("pk", "sk")),
					q("missing index hash key", without("g")),
					q("missing index range key", without("gr")),
					q("table key only", tableKey),
					q("placeholder", skAttrs{"lastkey": skS("v")}),
					q("extra attribute", skMerge(gsiKey, skAttrs{"x": skS("1")})),
					q("local index key attribute", skMerge(gsiKey, skAttrs{"l": skS("1")})),
					q("extra attribute and a wrong type", skMerge(gsiKey, skAttrs{"gr": skN("1"), "x": skS("1")})),
					q("extra attribute and an empty value", skMerge(gsiKey, skAttrs{"gr": skEmpty(), "x": skS("1")})),
					q("extra attribute and another partition", skMerge(gsiKey, skAttrs{"g": skS("g2"), "x": skS("1")})),
					q("index hash key wrong type", skMerge(gsiKey, skAttrs{"g": skN("1")})),
					q("index range key wrong type", skMerge(gsiKey, skAttrs{"gr": skN("1")})),
					q("table hash key wrong type", skMerge(gsiKey, skAttrs{"pk": skN("1")})),
					q("table range key wrong type", skMerge(gsiKey, skAttrs{"sk": skN("1")})),
					// The table's key attributes are checked before the index's, one at a time.
					q("table hash key wrong, index hash key missing", skAttrs{"pk": skN("1"), "sk": skS("a"), "x": skS("1"), "gr": skS("1")}),
					q("table hash key missing, index hash key wrong", skAttrs{"sk": skS("a"), "g": skN("1"), "gr": skS("1"), "x": skS("1")}),
					q("table range key wrong, index range key missing", skAttrs{"pk": skS("p"), "sk": skN("1"), "g": skS("g"), "x": skS("1")}),
					q("table range key missing, index range key wrong", skAttrs{"pk": skS("p"), "g": skS("g"), "gr": skN("1"), "x": skS("1")}),
					q("wrong type in another partition", skMerge(gsiKey, skAttrs{"g": skS("g2"), "gr": skN("1")})),
					q("empty index range key", skMerge(gsiKey, skAttrs{"gr": skEmpty()})),
					q("empty index hash key", skMerge(gsiKey, skAttrs{"g": skEmpty()})),
					q("empty table hash key", skMerge(gsiKey, skAttrs{"pk": skEmpty()})),
					q("empty table range key", skMerge(gsiKey, skAttrs{"sk": skEmpty()})),
					q("empty table range key and index hash key", skMerge(gsiKey, skAttrs{"sk": skEmpty(), "g": skEmpty()})),
					q("empty table hash key and index hash key", skMerge(gsiKey, skAttrs{"pk": skEmpty(), "g": skEmpty()})),
					q("empty index range key, another partition", skMerge(gsiKey, skAttrs{"gr": skEmpty(), "g": skS("g2")})),
					q("empty table range key, wrong index range key", skMerge(gsiKey, skAttrs{"sk": skEmpty(), "gr": skN("1")})),
					q("wrong table range key, empty index range key", skMerge(gsiKey, skAttrs{"sk": skN("1"), "gr": skEmpty()})),
					gq("range, start at the bound", gsiKey, "g = :p AND gr > :s", "1"),
					gq("range, start inside", gsiKey, "g = :p AND gr > :s", "0"),
					gq("range, missing index range key", without("gr"), "g = :p AND gr > :s", "2"),
					gq("range, upper bound", skMerge(gsiKey, skAttrs{"sk": skS("b"), "gr": skS("2")}), "g = :p AND gr <= :s", "2"),
				})
			},
		},
		{
			// Unlike the table, an index accepts a start key at the bound its walk ends at.
			name: "LocalIndexRangeMatrix",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				var probes []skProbe

				for _, cond := range skRangeConditions("pk", "l") {
					// The start key names the item of its index key, when there is one.
					for _, start := range [][2]string{{"c", "1"}, {"b", "2"}, {"x", "25"}, {"a", "3"}, {"x", "4"}} {
						probes = append(probes, skIndexProbe(cond+" start l="+start[1], "by-l", cond, skS("p"),
							skAttrs{"pk": skS("p"), "sk": skS(start[0]), "l": skS(start[1])}, skAttrs{":s": skS("2"), ":t": skS("3")}))
					}
				}

				return skRun(ctx, client, probes)
			},
		},
		{
			name: "GlobalIndexRangeMatrix",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				var probes []skProbe

				for _, cond := range skRangeConditions("g", "gr") {
					for _, start := range [][2]string{{"a", "1"}, {"b", "2"}, {"x", "25"}, {"c", "3"}, {"x", "5"}} {
						probes = append(probes, skIndexProbe(cond+" start gr="+start[1], "by-g", cond, skS("g"),
							skMerge(gsiKey, skAttrs{"sk": skS(start[0]), "gr": skS(start[1])}), skAttrs{":s": skS("2"), ":t": skS("3")}))
					}
				}

				return skRun(ctx, client, probes)
			},
		},
		{
			name: "ScanTable",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				starts := map[string]skAttrs{
					"existing item":         tableKey,
					"no such item":          {"pk": skS("p"), "sk": skS("zz")},
					"another partition":     {"pk": skS("zz"), "sk": skS("a")},
					"missing range key":     {"pk": skS("p")},
					"missing hash key":      {"sk": skS("a")},
					"placeholder":           {"lastkey": skS("v")},
					"extra attribute":       skMerge(tableKey, skAttrs{"x": skS("1")}),
					"index key given":       gsiKey,
					"range key wrong type":  {"pk": skS("p"), "sk": skN("1")},
					"hash key wrong type":   {"pk": skN("1"), "sk": skS("a")},
					"wrong type, missing":   {"pk": skN("1")},
					"empty range key":       {"pk": skS("p"), "sk": skEmpty()},
					"empty hash key":        {"pk": skEmpty(), "sk": skS("a")},
					"number out of range":   {"pk": skS("p"), "sk": skN("1e999")},
					"empty value, extra":    skMerge(tableKey, skAttrs{"sk": skEmpty(), "x": skS("1")}),
					"same size, wrong name": {"pk": skS("p"), "x": skS("1")},
				}
				order := []string{
					"existing item", "no such item", "another partition", "missing range key", "missing hash key",
					"placeholder", "extra attribute", "index key given", "range key wrong type", "hash key wrong type",
					"wrong type, missing", "empty range key", "empty hash key", "number out of range",
					"empty value, extra", "same size, wrong name",
				}

				return skScan(ctx, client, "", starts, order)
			},
		},
		{
			name: "ScanIndexes",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				gsi := map[string]skAttrs{
					"existing entry":              gsiKey,
					"no such item":                {"pk": skS("zz"), "sk": skS("a"), "g": skS("g"), "gr": skS("1")},
					"table key only":              tableKey,
					"index key only":              {"g": skS("g"), "gr": skS("1")},
					"missing table hash key":      skMerge(gsiKey, skAttrs{"pk": nil}),
					"missing index range key":     skMerge(gsiKey, skAttrs{"gr": nil}),
					"placeholder":                 {"lastkey": skS("v")},
					"extra attribute":             skMerge(gsiKey, skAttrs{"x": skS("1")}),
					"index range key wrong type":  skMerge(gsiKey, skAttrs{"gr": skN("1")}),
					"table range key wrong type":  skMerge(gsiKey, skAttrs{"sk": skN("1")}),
					"empty index range key":       skMerge(gsiKey, skAttrs{"gr": skEmpty()}),
					"empty index hash key":        skMerge(gsiKey, skAttrs{"g": skEmpty()}),
					"empty table range key":       skMerge(gsiKey, skAttrs{"sk": skEmpty()}),
					"extra attribute, wrong type": skMerge(gsiKey, skAttrs{"gr": skN("1"), "x": skS("1")}),
				}
				gsiOrder := []string{
					"existing entry", "no such item", "table key only", "index key only", "missing table hash key",
					"missing index range key", "placeholder", "extra attribute", "index range key wrong type",
					"table range key wrong type", "empty index range key", "empty index hash key",
					"empty table range key", "extra attribute, wrong type",
				}

				lsi := map[string]skAttrs{
					"existing entry":       lsiKey,
					"missing index key":    tableKey,
					"missing range key":    {"pk": skS("p"), "l": skS("3")},
					"placeholder":          {"lastkey": skS("v")},
					"extra attribute":      skMerge(lsiKey, skAttrs{"x": skS("1")}),
					"same size, wrong":     {"pk": skS("p"), "sk": skS("a"), "x": skS("1")},
					"index key wrong type": skMerge(lsiKey, skAttrs{"l": skN("3")}),
					"empty index key":      skMerge(lsiKey, skAttrs{"l": skEmpty()}),
					"empty hash key":       skMerge(lsiKey, skAttrs{"pk": skEmpty()}),
				}
				lsiOrder := []string{
					"existing entry", "missing index key", "missing range key", "placeholder", "extra attribute",
					"same size, wrong", "index key wrong type", "empty index key", "empty hash key",
				}

				return []any{
					skScan(ctx, client, "by-g", gsi, gsiOrder),
					skScan(ctx, client, "by-l", lsi, lsiOrder),
				}
			},
		},
		{
			name: "NumberKeys",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				startKeyCreate(ctx, t, client, dynamodbtypes.ScalarAttributeTypeN)

				for _, sk := range []string{"2", "10", "100"} {
					startKeyPut(ctx, t, client, skN(sk), sk, sk)
				}

				bound := skAttrs{":s": skN("9")}
				cond := "pk = :p AND sk > :s"

				return skRun(ctx, client, []skProbe{
					skTableProbe("above by value", skAttrs{"pk": skS("p"), "sk": skN("10")}, cond, bound),
					skTableProbe("above, spelled with an exponent", skAttrs{"pk": skS("p"), "sk": skN("1e1")}, cond, bound),
					skTableProbe("above as text only", skAttrs{"pk": skS("p"), "sk": skN("2")}, cond, bound),
					skTableProbe("below, spelled with a decimal point", skAttrs{"pk": skS("p"), "sk": skN("8.5")}, cond, bound),
					skTableProbe("a string", skAttrs{"pk": skS("p"), "sk": skS("10")}, cond, bound),
				})
			},
		},
		{
			name: "ValidationOrder",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				skSetup(ctx, t, client)

				// The start key is invalid in each probe; another part of the request is not.
				bad := skAttrs{"lastkey": skS("v")}
				badGSI := skAttrs{"lastkey": skS("v")}
				mod := func(name string, f func(in *dynamodb.QueryInput)) skProbe {
					p := skTableProbe(name, bad, "pk = :p", nil)
					f(&p.in)

					return p
				}
				gsiMod := func(name string, index string, f func(in *dynamodb.QueryInput)) skProbe {
					p := skIndexProbe(name, index, "g = :p", skS("g"), badGSI, nil)
					f(&p.in)

					return p
				}
				none := func(*dynamodb.QueryInput) {}

				probes := []skProbe{
					mod("alone", none),
					mod("nonexistent index", func(in *dynamodb.QueryInput) { in.IndexName = aws.String("nope") }),
					mod("unused value", func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues[":u"] = skS("1") }),
					mod("unused name", func(in *dynamodb.QueryInput) { in.ExpressionAttributeNames = map[string]string{"#u": "x"} }),
					mod("projection syntax", func(in *dynamodb.QueryInput) { in.ProjectionExpression = aws.String("(") }),
					mod("projection undeclared name", func(in *dynamodb.QueryInput) { in.ProjectionExpression = aws.String("#x") }),
					mod("filter undeclared name", func(in *dynamodb.QueryInput) { in.FilterExpression = aws.String("#x = :p") }),
					mod("filter undefined value", func(in *dynamodb.QueryInput) { in.FilterExpression = aws.String("l = :nope") }),
					mod("key condition without the hash key", func(in *dynamodb.QueryInput) { in.KeyConditionExpression = aws.String("sk = :p") }),
					mod("key condition on another attribute", func(in *dynamodb.QueryInput) { in.KeyConditionExpression = aws.String("l = :p") }),
					mod("key condition with two ranges", func(in *dynamodb.QueryInput) {
						in.KeyConditionExpression = aws.String("pk = :p AND sk > :s AND sk < :t")
						in.ExpressionAttributeValues[":s"], in.ExpressionAttributeValues[":t"] = skS("a"), skS("z")
					}),
					mod("key condition BETWEEN out of order", func(in *dynamodb.QueryInput) {
						in.KeyConditionExpression = aws.String("pk = :p AND sk BETWEEN :s AND :t")
						in.ExpressionAttributeValues[":s"], in.ExpressionAttributeValues[":t"] = skS("z"), skS("a")
					}),
					mod("key condition undefined value", func(in *dynamodb.QueryInput) { in.KeyConditionExpression = aws.String("pk = :nope") }),
					mod("key condition range of the wrong type", func(in *dynamodb.QueryInput) {
						in.KeyConditionExpression = aws.String("pk = :p AND sk > :s")
						in.ExpressionAttributeValues[":s"] = skN("1")
					}),
					mod("key condition hash of the wrong type", func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues[":p"] = skN("1") }),
					mod("key condition empty hash value", func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues[":p"] = skEmpty() }),
					mod("key condition empty range value", func(in *dynamodb.QueryInput) {
						in.KeyConditionExpression = aws.String("pk = :p AND sk = :e")
						in.ExpressionAttributeValues[":e"] = skEmpty()
					}),
					mod("Select ALL_PROJECTED_ATTRIBUTES on the table", func(in *dynamodb.QueryInput) { in.Select = dynamodbtypes.SelectAllProjectedAttributes }),
					mod("Select SPECIFIC_ATTRIBUTES without a projection", func(in *dynamodb.QueryInput) { in.Select = dynamodbtypes.SelectSpecificAttributes }),
					mod("Select COUNT with a projection", func(in *dynamodb.QueryInput) {
						in.Select = dynamodbtypes.SelectCount
						in.ProjectionExpression = aws.String("l")
					}),
					mod("ConsistentRead", func(in *dynamodb.QueryInput) { in.ConsistentRead = aws.Bool(true) }),
					gsiMod("GSI Select ALL_ATTRIBUTES", "by-g", func(in *dynamodb.QueryInput) { in.Select = dynamodbtypes.SelectAllAttributes }),
				}

				out := skRun(ctx, client, probes)

				// A valid start key does reach the later checks.
				out = append(out, skRun(ctx, client, []skProbe{
					{"valid start key, Select", func() dynamodb.QueryInput {
						in := skTableProbe("", tableKey, "pk = :p", nil).in
						in.Select = dynamodbtypes.SelectAllProjectedAttributes

						return in
					}()},
				})...)

				return out
			},
		},
		{
			name: "ValidationOrderKeysOnlyIndex",
			fn: func(ctx context.Context, t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				skSetupKeysOnly(ctx, t, client)

				gsi := func(name string, start skAttrs, f func(in *dynamodb.QueryInput)) skProbe {
					p := skIndexProbe(name, "by-k", "g = :p", skS("g"), start, nil)
					f(&p.in)

					return p
				}
				bad := skAttrs{"lastkey": skS("v")}
				other := skAttrs{"pk": skS("p"), "sk": skS("a"), "g": skS("g2"), "gr": skS("1")}
				none := func(*dynamodb.QueryInput) {}
				filter := func(in *dynamodb.QueryInput) { in.FilterExpression = aws.String("l = :p") }
				projection := func(in *dynamodb.QueryInput) { in.ProjectionExpression = aws.String("l") }
				selectAll := func(in *dynamodb.QueryInput) { in.Select = dynamodbtypes.SelectAllAttributes }

				return skRun(ctx, client, []skProbe{
					gsi("valid", gsiKey, none),
					gsi("valid, filter on a non-projected attribute", gsiKey, filter),
					gsi("valid, projection of a non-projected attribute", gsiKey, projection),
					gsi("valid, Select ALL_ATTRIBUTES", gsiKey, selectAll),
					gsi("bad start key, filter", bad, filter),
					gsi("bad start key, projection", bad, projection),
					gsi("bad start key, Select ALL_ATTRIBUTES", bad, selectAll),
					gsi("another partition, filter", other, filter),
					gsi("another partition, projection", other, projection),
					gsi("another partition, Select ALL_ATTRIBUTES", other, selectAll),
					gsi("wrong type, filter", skMerge(gsiKey, skAttrs{"gr": skN("1")}), filter),
					gsi("empty value, Select ALL_ATTRIBUTES", skMerge(gsiKey, skAttrs{"gr": skEmpty()}), selectAll),
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2EKnown(t, tt.knownIssue, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				return tt.fn(context.Background(), t, client)
			})
		})
	}
}

// skSetupKeysOnly creates table "startkey" with a global index "by-k" on g / gr that projects
// only the keys, holding p/a with gr 1.
func skSetupKeysOnly(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	def := func(name string) dynamodbtypes.AttributeDefinition {
		return dynamodbtypes.AttributeDefinition{AttributeName: aws.String(name), AttributeType: dynamodbtypes.ScalarAttributeTypeS}
	}
	ks := func(name string, typ dynamodbtypes.KeyType) dynamodbtypes.KeySchemaElement {
		return dynamodbtypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: typ}
	}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(startKeyTable),
		KeySchema:            []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("sk", dynamodbtypes.KeyTypeRange)},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{def("pk"), def("sk"), def("g"), def("gr")},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-k"),
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeKeysOnly},
			KeySchema:  []dynamodbtypes.KeySchemaElement{ks("g", dynamodbtypes.KeyTypeHash), ks("gr", dynamodbtypes.KeyTypeRange)},
		}},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, startKeyTable, "by-k")

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(startKeyTable), Item: skAttrs{
		"pk": skS("p"), "sk": skS("a"), "g": skS("g"), "gr": skS("1"), "l": skS("3"),
	}})
	require.NoError(t, err)
}
