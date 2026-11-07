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

// updatedValuesItem is the item the UPDATED_OLD / UPDATED_NEW cases start from.
func updatedValuesItem() map[string]dynamodbtypes.AttributeValue {
	item := fuzzNestedDeleteAddItem()
	item["n"] = fzN("1")
	item["s"] = fzS("a")
	item["ss"] = &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x", "y", "w"}}
	item["ns"] = &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}}
	item["es"] = &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x", "y"}}

	return item
}

type updatedValuesCase struct {
	expr   string
	upsert bool
}

// updatedValuesCases are update expressions run on updatedValuesItem (or on a missing item
// when upsert is set). Every expression writes each path once, so the cases never hit #164.
var updatedValuesCases = []updatedValuesCase{
	// Written attributes are returned even when the value does not change (#165).
	{expr: "SET #n = if_not_exists(#n, :one), #s = :str"},
	{expr: "SET #s = #s"},
	{expr: "SET #n = :one"},
	{expr: "SET #ss = :sxy"},
	// Untouched attributes, sets included, are never returned.
	{expr: "SET nickname = :str"},
	{expr: "SET #n = #n + :one"},
	// REMOVE, including a missing attribute.
	{expr: "REMOVE #s"},
	{expr: "REMOVE #nope"},
	{expr: "REMOVE #s, #n"},
	// ADD creating or growing an attribute.
	{expr: "ADD #new :one"},
	{expr: "ADD #n :one"},
	{expr: "ADD #new :sz"},
	{expr: "ADD #ss :sz"},
	{expr: "ADD #ss :sx"},
	// DELETE shrinking, emptying or missing a set.
	{expr: "DELETE #ss :sx"},
	{expr: "DELETE #ss :sz"},
	{expr: "DELETE #nope :sx"},
	{expr: "DELETE #ns :n1"},
	{expr: "DELETE es :sxy"},
	{expr: "DELETE #m.#ss :sxy"},
	// Nested paths.
	{expr: "SET #m.#n = :one"},
	{expr: "SET #m.#n = #m.#n"},
	{expr: "SET #m.#new = :str"},
	{expr: "SET #m.#n = :one, #n = :one"},
	{expr: "REMOVE #m.#s"},
	{expr: "REMOVE #m.#nope"},
	{expr: "ADD #m.#n :one"},
	{expr: "ADD #m.#new :one"},
	{expr: "DELETE #m.#ss :sx"},
	{expr: "SET #l[1] = :one"},
	{expr: "SET #l[1] = :str"},
	{expr: "SET #l[4] = :str"},
	{expr: "SET #l[9] = :str"},
	{expr: "REMOVE #l[0]"},
	{expr: "REMOVE #l[0], #l[1]"},
	{expr: "REMOVE #l[9]"},
	{expr: "REMOVE #l[3]"},
	{expr: "REMOVE #l[2], #l[3]"},
	{expr: "ADD #l[1] :one"},
	{expr: "DELETE #l[0] :sx"},
	// Upsert of a missing item.
	{expr: "SET #n = :one, #s = :str", upsert: true},
	{expr: "REMOVE #n", upsert: true},
	{expr: "ADD #n :one", upsert: true},
	{expr: "DELETE #ss :sx", upsert: true},
	{expr: "SET #m.#n = :one", upsert: true},
}

// updatedValuesRun runs every case with UPDATED_OLD and UPDATED_NEW and reports the returned
// attributes (sets sorted), or the normalized error.
func updatedValuesRun(cases []updatedValuesCase) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()

		return updatedValuesRunCases(t, client, cases)
	}
}

func updatedValuesRunCases(t *testing.T, client *dynamodb.Client, cases []updatedValuesCase) any {
	t.Helper()
	ctx := context.Background()

	fuzzSetup(ctx, t, client)

	var out []string

	for _, c := range cases {
		for _, rv := range []dynamodbtypes.ReturnValue{dynamodbtypes.ReturnValueUpdatedOld, dynamodbtypes.ReturnValueUpdatedNew} {
			_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(fuzzTable), Key: fuzzKey("a", "1")})
			require.NoError(t, err)

			if !c.upsert {
				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: updatedValuesItem()})
				require.NoError(t, err)
			}

			in := fuzzNestedDeleteAddInput(c.expr)
			in.ReturnValues = rv

			res, err := client.UpdateItem(ctx, in)
			if err != nil {
				out = append(out, fmt.Sprintf("%s [%s] => %s", c.expr, rv, fuzzErrString(err)))

				continue
			}

			out = append(out, fmt.Sprintf("%s [%s] => %s", c.expr, rv,
				fuzzAttrJSON(&dynamodbtypes.AttributeValueMemberM{Value: parityNormalizeItemSets(res.Attributes)})))
		}
	}

	return out
}

// TestE2E_UpdatedValues pins UPDATED_OLD / UPDATED_NEW (#165): they return the attributes the
// update expression writes, whether or not the value changed.
func TestE2E_UpdatedValues(t *testing.T) {
	t.Run("WrittenAttributes", func(t *testing.T) {
		RunE2E(t, updatedValuesRun(updatedValuesCases))
	})

	// DynamoDB rejects a path written twice; minidyn accepts it (#164).
	t.Run("PathWrittenTwice", func(t *testing.T) {
		RunE2EKnown(t, "#164", updatedValuesRun([]updatedValuesCase{
			{expr: "SET #n = :one, #n = :one"},
			{expr: "SET #m.#n = :one, #m = :str"},
		}))
	})
}
