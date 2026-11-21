package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Paging a Query with Limit, feeding each LastEvaluatedKey back as the ExclusiveStartKey, must
// not fail and must read the same items as the unpaged Query, on DynamoDB Local and on minidyn.
// Only the union of the items and the absence of errors are compared, not the page boundaries:
// Local leaves out the LastEvaluatedKey of a page that ends on the inclusive end of the range
// and minidyn returns it (#153), so minidyn may read one more, empty, page.

func TestE2E_StartKeyPagingRoundTrip(t *testing.T) {
	for _, typ := range []dynamodbtypes.ScalarAttributeType{
		dynamodbtypes.ScalarAttributeTypeS, dynamodbtypes.ScalarAttributeTypeN,
	} {
		t.Run(string(typ), func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				return skPagingRoundTrip(context.Background(), t, client, typ)
			})
		})
	}
}

func skPagingRoundTrip(ctx context.Context, t *testing.T, client *dynamodb.Client, typ dynamodbtypes.ScalarAttributeType) any {
	t.Helper()

	startKeyCreate(ctx, t, client, typ)

	// Distinct sort keys and index keys, in different orders, so that no walk ties.
	idxKeys := []string{"c", "bb", "a", "b", "d"}
	sks := []startKeyAV{skS("a"), skS("b"), skS("bb"), skS("c"), skS("d")}

	if typ == dynamodbtypes.ScalarAttributeTypeN {
		sks = []startKeyAV{skN("1"), skN("2"), skN("3"), skN("4"), skN("5")}
	}

	for i, sk := range sks {
		tag := "keep"
		if i == 3 {
			tag = "skip" // the item the filter drops
		}

		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(startKeyTable), Item: skAttrs{
			"pk": skS("p"), "sk": sk, "l": skS(idxKeys[i]), "g": skS("g"), "gr": skS(idxKeys[i]), "tag": skS(tag),
		}})
		require.NoError(t, err)
	}

	var out []any

	for _, c := range skRoundTripQueries(typ) {
		unpaged, err := skPages(ctx, client, c.in, 0)
		require.NoError(t, err, c.name)

		out = append(out, c.name, unpaged)

		for _, limit := range []int32{1, 2} {
			paged, err := skPages(ctx, client, c.in, limit)
			require.NoError(t, err, "%s limit %d", c.name, limit)
			require.Equal(t, unpaged, paged, "%s limit %d", c.name, limit)
		}
	}

	return out
}

type skRoundTripQuery struct {
	name string
	in   dynamodb.QueryInput
}

type skRoundTripCond struct {
	name        string
	expr        func(attr string) string
	stringsOnly bool
}

var skRoundTripConds = []skRoundTripCond{
	{"=", func(a string) string { return a + " = :lo" }, false},
	{"<", func(a string) string { return a + " < :hi" }, false},
	{"<=", func(a string) string { return a + " <= :hi" }, false},
	{">", func(a string) string { return a + " > :lo" }, false},
	{">=", func(a string) string { return a + " >= :lo" }, false},
	{"BETWEEN", func(a string) string { return a + " BETWEEN :lo AND :hi" }, false},
	{"begins_with", func(a string) string { return "begins_with(" + a + ", :pre)" }, true},
}

// skRoundTripQueries is every Query the round trip pages: each target (the table by its sort
// key of type typ, then the two indexes), condition, filter and direction.
func skRoundTripQueries(typ dynamodbtypes.ScalarAttributeType) []skRoundTripQuery {
	lo, hi := skS("b"), skS("c")
	if typ == dynamodbtypes.ScalarAttributeTypeN {
		lo, hi = skN("2"), skN("4")
	}

	targets := []struct {
		name, index, hashAttr, hash, attr string
		isString                          bool
		lo, hi                            startKeyAV
	}{
		{"table", "", "pk", "p", "sk", typ == dynamodbtypes.ScalarAttributeTypeS, lo, hi},
		{"LSI", "by-l", "pk", "p", "l", true, skS("b"), skS("c")},
		{"GSI", "by-g", "g", "g", "gr", true, skS("b"), skS("c")},
	}

	var queries []skRoundTripQuery

	for _, target := range targets {
		for _, cond := range skRoundTripConds {
			if cond.stringsOnly && !target.isString {
				continue
			}

			for _, filtered := range []bool{false, true} {
				for _, forward := range []bool{true, false} {
					in := skRoundTripInput(target.index, target.hashAttr+" = :h AND "+cond.expr(target.attr),
						skAttrs{":h": skS(target.hash), ":lo": target.lo, ":hi": target.hi, ":pre": skS("b")}, filtered, forward)

					queries = append(queries, skRoundTripQuery{
						fmt.Sprintf("%s %s filter=%v forward=%v", target.name, cond.name, filtered, forward), in,
					})
				}
			}
		}
	}

	return queries
}

// skRoundTripInput is a Query of expr on index ("" for the table) that names only those of
// values it uses, as DynamoDB rejects the others.
func skRoundTripInput(index, expr string, all skAttrs, filtered, forward bool) dynamodb.QueryInput {
	values := skAttrs{}

	for name, v := range all {
		if strings.Contains(expr, name) {
			values[name] = v
		}
	}

	in := dynamodb.QueryInput{
		TableName:                 aws.String(startKeyTable),
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: values,
		ScanIndexForward:          aws.Bool(forward),
	}

	if index != "" {
		in.IndexName = aws.String(index)
	}

	if filtered {
		in.FilterExpression = aws.String("tag <> :skip")
		values[":skip"] = skS("skip")
	}

	return in
}

// skPages reads every page of in with limit (all at once when it is 0) and returns the keys of
// the items read, in order.
func skPages(ctx context.Context, client *dynamodb.Client, in dynamodb.QueryInput, limit int32) ([]string, error) {
	if limit > 0 {
		in.Limit = aws.Int32(limit)
	}

	keys := []string{}

	for range 50 {
		res, err := client.Query(ctx, &in)
		if err != nil {
			return nil, err
		}

		for _, it := range res.Items {
			keys = append(keys, startKeyLabel(it["pk"])+"/"+startKeyLabel(it["sk"]))
		}

		if len(res.LastEvaluatedKey) == 0 {
			return keys, nil
		}

		in.ExclusiveStartKey = res.LastEvaluatedKey
	}

	return nil, fmt.Errorf("paging did not finish")
}
