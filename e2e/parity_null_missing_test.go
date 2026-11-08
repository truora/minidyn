package e2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Parity for expressions that meet a stored value of an unexpected type (NULL, a string
// where a list is expected, ...) or a missing one (#143). DynamoDB only rejects expressions
// for static problems: in conditions and filters a mismatch on one item is just false, in
// projections the path is left out, and update expressions answer with their own errors.

const nullMissingTable = fuzzTable

func nmItems() []map[string]dynamodbtypes.AttributeValue {
	nul := &dynamodbtypes.AttributeValueMemberNULL{Value: true}

	full := map[string]dynamodbtypes.AttributeValue{
		"pk": fzS("p"), "sk": fzS("full"),
		"s": fzS("abc"), "n": fzN("5"),
		"b": &dynamodbtypes.AttributeValueMemberB{Value: []byte("abc")},
		"l": &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{fzS("a"), fzN("1")}},
		"m": &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{
			"k": fzS("abc"), "nul": nul,
		}},
		"ss":   &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
		"ns":   &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
		"bs":   &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
		"bool": &dynamodbtypes.AttributeValueMemberBOOL{Value: true},
		"nul":  nul,
	}

	nulls := map[string]dynamodbtypes.AttributeValue{"pk": fzS("p"), "sk": fzS("nulls")}
	for _, a := range []string{"s", "n", "b", "l", "m", "ss", "ns", "bs", "bool", "nul"} {
		nulls[a] = nul
	}

	return []map[string]dynamodbtypes.AttributeValue{full, nulls, fuzzKey("p", "empty")}
}

func nmItemFor(sk string) map[string]dynamodbtypes.AttributeValue {
	for _, it := range nmItems() {
		if it["sk"].(*dynamodbtypes.AttributeValueMemberS).Value == sk {
			return it
		}
	}

	return fuzzKey("p", sk)
}

func nmRoots() []string {
	return []string{"s", "n", "b", "l", "m", "ss", "ns", "bs", "bool", "nul", "nope"}
}

func nmPaths() []string {
	return []string{"#r", "#r.k", "#r.nul", "#r.k.x", "#r[0]", "#r[5]"}
}

func nmFilterTemplates() []string {
	return []string{
		"contains(P, :s)", "contains(P, :n)", "contains(P, :b)",
		"begins_with(P, :s)", "begins_with(P, :b)",
		"size(P) > :n", "size(P) = :n", "size(P) <> :n", "attribute_type(P, :t)",
		"attribute_exists(P)", "attribute_not_exists(P)",
		"P = :s", "P <> :s", "P = :n", "P < :s", "P >= :n",
		"P BETWEEN :s AND :s2", "P BETWEEN :n AND :n2", "P IN (:s, :n)",
		"NOT contains(P, :s)", "attribute_exists(P) AND begins_with(P, :s)",
		"contains(P, :s) OR attribute_not_exists(P)",
	}
}

func nmValues() map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		":s": fzS("a"), ":s2": fzS("b"), ":n": fzN("1"), ":n2": fzN("9"), ":t": fzS("S"),
		":b": &dynamodbtypes.AttributeValueMemberB{Value: []byte("a")}, ":x": fzS("X"),
		":l":    &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{fzS("z")}},
		":m":    &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"a": fzS("z")}},
		":bool": &dynamodbtypes.AttributeValueMemberBOOL{Value: true},
		":null": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
		":ss":   &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a"}},
	}
}

// nmUsed returns only the placeholders expr mentions: DynamoDB rejects unused values.
func nmUsed(expr string, values map[string]dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
	out := map[string]dynamodbtypes.AttributeValue{}

	for k, v := range values {
		if strings.Contains(expr, k+")") || strings.Contains(expr, k+" ") || strings.Contains(expr, k+",") ||
			strings.HasSuffix(expr, k) {
			out[k] = v
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func nmScanIDs(items []map[string]dynamodbtypes.AttributeValue) string {
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it["sk"].(*dynamodbtypes.AttributeValueMemberS).Value)
	}

	sort.Strings(ids)

	return strings.Join(ids, ",")
}

func nmItemString(it map[string]dynamodbtypes.AttributeValue) string {
	if it == nil {
		return "<none>"
	}

	keys := make([]string, 0, len(it))
	for k := range it {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+nmAttrString(it[k]))
	}

	return strings.Join(parts, " ")
}

func nmAttrString(av dynamodbtypes.AttributeValue) string {
	switch v := av.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		return "S:" + v.Value
	case *dynamodbtypes.AttributeValueMemberN:
		return "N:" + v.Value
	case *dynamodbtypes.AttributeValueMemberB:
		return "B:" + string(v.Value)
	case *dynamodbtypes.AttributeValueMemberBOOL:
		if v.Value {
			return "BOOL:true"
		}

		return "BOOL:false"
	case *dynamodbtypes.AttributeValueMemberNULL:
		return "NULL"
	case *dynamodbtypes.AttributeValueMemberL:
		parts := make([]string, 0, len(v.Value))
		for _, e := range v.Value {
			parts = append(parts, nmAttrString(e))
		}

		return "L[" + strings.Join(parts, ",") + "]"
	case *dynamodbtypes.AttributeValueMemberM:
		return "M{" + nmItemString(v.Value) + "}"
	case *dynamodbtypes.AttributeValueMemberSS:
		s := append([]string{}, v.Value...)
		sort.Strings(s)

		return "SS" + strings.Join(s, ",")
	case *dynamodbtypes.AttributeValueMemberNS:
		s := append([]string{}, v.Value...)
		sort.Strings(s)

		return "NS" + strings.Join(s, ",")
	case *dynamodbtypes.AttributeValueMemberBS:
		parts := make([]string, 0, len(v.Value))
		for _, e := range v.Value {
			parts = append(parts, string(e))
		}

		sort.Strings(parts)

		return "BS" + strings.Join(parts, ",")
	}

	return "?"
}

// A filter that meets a stored value of the wrong type, or a missing one, is
// false for that item; only static problems fail the request.
func nmScanFilterOnMismatchedValues(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client, nmItems()...)

	out := map[string]string{}

	for _, root := range nmRoots() {
		for _, p := range nmPaths() {
			for _, tpl := range nmFilterTemplates() {
				expr := strings.ReplaceAll(tpl, "P", p)
				key := root + " " + expr

				res, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:                 aws.String(nullMissingTable),
					FilterExpression:          aws.String(expr),
					ExpressionAttributeNames:  map[string]string{"#r": root},
					ExpressionAttributeValues: nmUsed(expr, nmValues()),
				})
				if err != nil {
					out[key] = "ERR " + fuzzErrString(err)

					continue
				}

				out[key] = nmScanIDs(res.Items)
			}
		}
	}

	return out
}

// Problems DynamoDB can see without reading the item, from the types of the
// values in the expression, still fail the request.
func nmScanFilterStaticOperandTypes(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client, nmItems()...)

	out := map[string]string{}
	exprs := []string{
		"begins_with(:n, :s)", "begins_with(:s, :n)", "begins_with(s, :l)", "begins_with(s, :m)",
		"begins_with(s, :bool)", "begins_with(s, :null)", "begins_with(s, :ss)", "begins_with(:s, s)",
		"begins_with(:b, s)", "begins_with(s, :b)",
		"contains(s, :l)", "contains(s, :m)", "contains(s, :bool)", "contains(s, :null)",
		"contains(s, :ss)", "contains(:s, :s2)", "contains(:n, :s)", "contains(:ss, :s)",
		"size(:s) > :n", "size(:l) > :n", "attribute_type(s, :n)", "attribute_type(s, :x)",
		"attribute_type(:s, :t)",
		"s < :l", "s = :l", "s = :m", "s BETWEEN :s AND :n",
		"s BETWEEN :l AND :l", "s IN (:s, :l)", "s IN (:n)",
		"size(s) = :s", "size(s) > :l", "size(s) > :null",
		// a missing path does not hide a value that cannot be ordered
		"nope < :bool", "nope <= :m", "nope > :l", "nope >= :null", ":bool < nope",
		"nope < :n", "nope >= :s", "n.x < :m", "nul < :l",
	}

	for _, expr := range exprs {
		res, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:                 aws.String(nullMissingTable),
			FilterExpression:          aws.String(expr),
			ExpressionAttributeValues: nmUsed(expr, nmValues()),
		})
		if err != nil {
			// Only whether it fails: the wording of these static errors is not
			// ported yet.
			out[expr] = "ERR"

			continue
		}

		out[expr] = nmScanIDs(res.Items)
	}

	return out
}

// A projected path that does not resolve is left out of the item.
func nmScanProjectionOnMismatchedValues(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client, nmItems()...)

	out := map[string]string{}

	for _, root := range nmRoots() {
		for _, p := range nmPaths() {
			res, err := client.Scan(ctx, &dynamodb.ScanInput{
				TableName:                aws.String(nullMissingTable),
				ProjectionExpression:     aws.String("sk, " + p),
				ExpressionAttributeNames: map[string]string{"#r": root},
			})
			if err != nil {
				out[root+" "+p] = "ERR " + fuzzErrString(err)

				continue
			}

			items := make([]string, 0, len(res.Items))
			for _, it := range res.Items {
				items = append(items, nmItemString(it))
			}

			sort.Strings(items)

			out[root+" "+p] = strings.Join(items, " | ")
		}
	}

	return out
}

// Write conditions with functions on existing, attribute-less and missing items.
func nmWriteConditionOnMismatchedValues(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client, nmItems()...)

	out := map[string]string{}
	conds := []string{
		"contains(ss, :s)", "contains(s, :s)", "contains(nul, :s)", "begins_with(s, :s)",
		"begins_with(nul, :s)", "size(l) > :n", "size(nul) > :n", "size(nope) > :n",
		"attribute_exists(s) AND begins_with(s, :s)", "NOT (begins_with(s, :s))",
		"l[0] = :s", "m.k = :s", "nul.x = :s", "begins_with(m.k, :s)", "contains(l, :n)",
		"s BETWEEN :s AND :s2", "n IN (:n, :n2)",
	}

	for _, sk := range []string{"full", "nulls", "empty", "gone"} {
		for _, cond := range conds {
			values := nmUsed(cond, nmValues())

			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(nullMissingTable), Item: fuzzKey("p", sk),
				ConditionExpression:       aws.String(cond),
				ExpressionAttributeValues: values,
			})
			out["Put "+sk+" "+cond] = fuzzErrString(err)

			_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(nullMissingTable), Key: fuzzKey("p", sk),
				UpdateExpression:          aws.String("SET zz = :s"),
				ConditionExpression:       aws.String(cond),
				ExpressionAttributeValues: withValue(values, ":s", fzS("a")),
			})
			out["Update "+sk+" "+cond] = fuzzErrString(err)

			_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(nullMissingTable), Key: fuzzKey("p", sk),
				ConditionExpression:       aws.String(cond),
				ExpressionAttributeValues: values,
			})
			out["Delete "+sk+" "+cond] = fuzzErrString(err)

			// Restore whatever the previous attempts removed or changed.
			if sk != "gone" {
				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(nullMissingTable), Item: nmItemFor(sk),
				})
				require.NoError(t, err)
			} else {
				_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName: aws.String(nullMissingTable), Key: fuzzKey("p", sk),
				})
				require.NoError(t, err)
			}
		}
	}

	return out
}

// Update expressions reading missing or NULL attributes have their own errors.
func nmUpdateExpressionOnMissingOrNullValues(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client, nmItems()...)

	out := map[string]string{}
	updates := []string{
		"SET n = n + :n", "SET n = :n + n", "SET zz = s + :n", "SET n = n - :n", "SET n = :n - n",
		"SET zz = nope + :n", "SET zz = nul + :n", "SET zz = n + nope",
		"SET l = list_append(l, :l)", "SET l = list_append(:l, l)",
		"SET l = list_append(nope, :l)", "SET zz = list_append(:l, nope)",
		"SET zz = list_append(nul, :l)", "SET zz = list_append(s, :l)",
		"SET zz = list_append(:l, :n)", "SET zz = list_append(:n, :l)",
		"SET zz = :s + :n", "SET zz = :n + :s", "SET zz = :l + :n", "SET zz = :n - :null",
		"SET zz = nope + n", "SET zz = n + nul", "SET zz = nul + nope",
		"SET zz = list_append(nope, nul)", "SET zz = list_append(nul, nope)",
		"SET zz = if_not_exists(m.k, m.k)", "SET zz = if_not_exists(l[0], l[0])",
		"SET zz = if_not_exists(n, n)", "SET zz = if_not_exists(n, :n) + if_not_exists(n, :n)",
		"SET zz = if_not_exists(nope, :n) + nope", "SET zz = if_not_exists(n, nope)",
		"SET zz = if_not_exists(nope, nope2)", "SET zz = if_not_exists(nope, l[9])",

		"SET zz = if_not_exists(nope, :n)", "SET zz = if_not_exists(nul, :n)",
		"SET zz = if_not_exists(n, :n)", "SET n = if_not_exists(n, :n) + :n",
		"SET zz = if_not_exists(nope, nope)", "SET zz = if_not_exists(nope.y, :n)",
		"SET zz = if_not_exists(m.zz, :n)", "SET zz = if_not_exists(nul.y, :n)",
		"SET m.k = :s", "SET m.zz = :s", "SET m.k.x = :s", "SET m.nul.x = :s",
		"SET l[0] = :s", "SET l[9] = :s", "SET nul.x = :s", "SET nul[0] = :s",
		"SET nope.y = :s", "SET nope[0] = :s", "SET s.x = :s", "SET s[0] = :s",
		"SET zz = m.k", "SET zz = nope", "SET zz = l[0]", "SET zz = l[9]", "SET zz = nul",
		"SET zz = nope.y", "SET zz = nul.y", "SET zz = nul[0]", "SET zz = s.x",
		"SET zz = m.k.x", "SET zz = m.zz", "SET zz = nope[0]",
		"REMOVE m.k", "REMOVE m.zz", "REMOVE nul.x", "REMOVE l[0]", "REMOVE l[9]",
		"REMOVE nope.y", "REMOVE nope[0]", "REMOVE s.x", "REMOVE nul[0]",
		"ADD m.k :n", "ADD nope.y :n", "ADD nul :n", "ADD nul.x :n", "ADD nope :n",
		"DELETE nope.y :ss", "DELETE nul :ss", "DELETE ss :ss", "DELETE nul.x :ss",
	}

	values := nmValues()

	for _, sk := range []string{"full", "nulls", "empty"} {
		for _, upd := range updates {
			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(nullMissingTable), Item: nmItemFor(sk),
			})
			require.NoError(t, err)

			_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(nullMissingTable), Key: fuzzKey("p", sk),
				UpdateExpression:          aws.String(upd),
				ExpressionAttributeValues: nmUsed(upd, values),
			})

			got, gerr := client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(nullMissingTable), Key: fuzzKey("p", sk), ConsistentRead: aws.Bool(true),
			})
			require.NoError(t, gerr)

			out[sk+" "+upd] = fuzzErrString(err) + " => " + nmItemString(got.Item)
		}
	}

	return out
}

func TestE2E_NullAndMissingValues(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{"ScanFilterOnMismatchedValues", nmScanFilterOnMismatchedValues},
		{"ScanFilterStaticOperandTypes", nmScanFilterStaticOperandTypes},
		{"ScanProjectionOnMismatchedValues", nmScanProjectionOnMismatchedValues},
		{"WriteConditionOnMismatchedValues", nmWriteConditionOnMismatchedValues},
		{"UpdateExpressionOnMissingOrNullValues", nmUpdateExpressionOnMissingOrNullValues},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}

func withValue(
	values map[string]dynamodbtypes.AttributeValue, k string, v dynamodbtypes.AttributeValue,
) map[string]dynamodbtypes.AttributeValue {
	out := map[string]dynamodbtypes.AttributeValue{k: v}

	for kk, vv := range values {
		out[kk] = vv
	}

	return out
}
