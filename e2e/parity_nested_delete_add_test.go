package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// fuzzNestedDeleteAddItem is the item the nested DELETE/ADD cases start from.
func fuzzNestedDeleteAddItem() map[string]dynamodbtypes.AttributeValue {
	item := fuzzKey("a", "1")
	item["m"] = &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{
		"ss": &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x", "y"}},
		"ns": &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
		"n":  fzN("1"),
		"s":  fzS("a"),
	}}
	item["l"] = &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{
		&dynamodbtypes.AttributeValueMemberSS{Value: []string{"x", "y"}},
		fzN("1"),
		&dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
		fzS("a"),
	}}

	return item
}

var fuzzNestedDeleteAddPlaceholder = regexp.MustCompile(`[#:]\w+`)

var fuzzNestedDeleteAddNames = map[string]string{
	"#m": "m", "#ss": "ss", "#ns": "ns", "#n": "n", "#s": "s", "#l": "l",
	"#new": "new", "#nope": "nope", "#x": "x",
}

func fuzzNestedDeleteAddValues() map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		":sx":   &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x"}},
		":sxy":  &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x", "y"}},
		":sz":   &dynamodbtypes.AttributeValueMemberSS{Value: []string{"z"}},
		":n1":   &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}},
		":n3":   &dynamodbtypes.AttributeValueMemberNS{Value: []string{"3"}},
		":one":  fzN("1"),
		":str":  fzS("x"),
		":bool": &dynamodbtypes.AttributeValueMemberBOOL{Value: true},
	}
}

// fuzzNestedDeleteAddInput builds an UpdateItem for expr declaring only the placeholders it uses.
func fuzzNestedDeleteAddInput(expr string) *dynamodb.UpdateItemInput {
	in := &dynamodb.UpdateItemInput{
		TableName:        aws.String(fuzzTable),
		Key:              fuzzKey("a", "1"),
		UpdateExpression: aws.String(expr),
		ReturnValues:     dynamodbtypes.ReturnValueAllNew,
	}

	used := map[string]bool{}
	for _, tok := range fuzzNestedDeleteAddPlaceholder.FindAllString(expr, -1) {
		used[tok] = true
	}

	for name, attr := range fuzzNestedDeleteAddNames {
		if used[name] {
			if in.ExpressionAttributeNames == nil {
				in.ExpressionAttributeNames = map[string]string{}
			}

			in.ExpressionAttributeNames[name] = attr
		}
	}

	for name, v := range fuzzNestedDeleteAddValues() {
		if used[name] {
			if in.ExpressionAttributeValues == nil {
				in.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{}
			}

			in.ExpressionAttributeValues[name] = v
		}
	}

	return in
}

// fuzzNestedDeleteAdd runs DELETE and ADD actions on nested document paths (#184). Each
// expression starts from a fresh copy of fuzzNestedDeleteAddItem and reports either the
// normalized error or the resulting item.
func fuzzNestedDeleteAdd(t *testing.T, client *dynamodb.Client) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client)

	exprs := []string{
		// DELETE on a nested set.
		"DELETE #m.#ss :sx",
		"DELETE #m.#ss :sxy",
		"DELETE #m.#ss :sz",
		"DELETE #m.#ns :n1",
		"DELETE #m.#ns :sx",
		"DELETE #m.#ss :one",
		"DELETE #m.#n :sx",
		"DELETE #m.#s :sx",
		// DELETE with a missing parent or key.
		"DELETE #nope.#x :sx",
		"DELETE #nope.#x :one",
		"DELETE #m.#nope :sx",
		"DELETE #m.#nope :one",
		"DELETE #m.#nope.#x :sx",
		"DELETE #m.#s.#x :sx",
		"DELETE #m.#ss.#x :sx",
		"DELETE #s.#x :sx",
		// DELETE on list elements.
		"DELETE #l[0] :sx",
		"DELETE #l[0] :sxy",
		"DELETE #l[2] :n1",
		"DELETE #l[1] :sx",
		"DELETE #l[0] :n1",
		"DELETE #l[5] :sx",
		"DELETE #l[5] :one",
		"DELETE #nope[0] :sx",
		"DELETE #m[0] :sx",
		// ADD to nested sets and numbers.
		"ADD #m.#ss :sz",
		"ADD #m.#ns :n3",
		"ADD #m.#n :one",
		"ADD #m.#ss :n1",
		"ADD #m.#ns :sx",
		"ADD #m.#n :sx",
		"ADD #m.#ss :one",
		"ADD #m.#s :one",
		"ADD #m.#s :sx",
		"ADD #m.#ss :str",
		"ADD #m.#ss :bool",
		// ADD creating the key under an existing map.
		"ADD #m.#new :one",
		"ADD #m.#new :sz",
		"ADD #m.#new :n3",
		"ADD #m.#new :str",
		"ADD #m.#new :bool",
		// ADD with a missing parent.
		"ADD #nope.#x :one",
		"ADD #nope.#x :sz",
		"ADD #nope.#x :str",
		"ADD #m.#nope.#x :one",
		"ADD #m.#s.#x :one",
		"ADD #m.#ss.#x :one",
		"ADD #s.#x :one",
		// ADD on list elements.
		"ADD #l[0] :sz",
		"ADD #l[2] :n3",
		"ADD #l[1] :one",
		"ADD #l[1] :sx",
		"ADD #l[0] :one",
		"ADD #l[3] :one",
		"ADD #l[5] :one",
		"ADD #l[5] :sz",
		"ADD #nope[0] :one",
		"ADD #m[0] :one",
		// Nested actions next to each other.
		"DELETE #m.#ss :sx ADD #m.#n :one",
		"ADD #m.#n :one, #m.#new :sz",
	}

	out := make([]string, 0, len(exprs))

	for _, expr := range exprs {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: fuzzNestedDeleteAddItem()})
		require.NoError(t, err)

		res, err := client.UpdateItem(ctx, fuzzNestedDeleteAddInput(expr))
		if err != nil {
			out = append(out, expr+" => "+fuzzErrString(err))

			continue
		}

		item := parityNormalizeItemSets(res.Attributes)
		out = append(out, fmt.Sprintf("%s => m=%s l=%s", expr, fuzzAttrJSON(item["m"]), fuzzAttrJSON(item["l"])))
	}

	return out
}

// fuzzAttrJSON renders an attribute value as stable JSON (nil as "<absent>").
func fuzzAttrJSON(av dynamodbtypes.AttributeValue) string {
	if av == nil {
		return "<absent>"
	}

	var sb strings.Builder

	switch v := av.(type) {
	case *dynamodbtypes.AttributeValueMemberM:
		keys := make([]string, 0, len(v.Value))
		for k := range v.Value {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		sb.WriteString("{")

		for i, k := range keys {
			if i > 0 {
				sb.WriteString(",")
			}

			sb.WriteString(k + ":" + fuzzAttrJSON(v.Value[k]))
		}

		sb.WriteString("}")
	case *dynamodbtypes.AttributeValueMemberL:
		sb.WriteString("[")

		for i, e := range v.Value {
			if i > 0 {
				sb.WriteString(",")
			}

			sb.WriteString(fuzzAttrJSON(e))
		}

		sb.WriteString("]")
	case *dynamodbtypes.AttributeValueMemberS:
		sb.WriteString("S(" + v.Value + ")")
	case *dynamodbtypes.AttributeValueMemberN:
		sb.WriteString("N(" + v.Value + ")")
	default:
		b, err := json.Marshal(av)
		if err != nil {
			return "<unmarshalable>"
		}

		return fmt.Sprintf("%T%s", av, b)
	}

	return sb.String()
}
