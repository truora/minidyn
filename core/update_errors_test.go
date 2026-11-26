package core

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// Update returns every expression error as a types.Error ValidationException, which does not
// unwrap: the front ends map it with mapKnownError and need no interpreter.ErrSyntaxError check.
func TestUpdateExpressionErrorsAreValidationExceptions(t *testing.T) {
	tests := map[string]struct {
		update    string
		condition *string
	}{
		"update syntax":    {update: "SET = :one"},
		"condition syntax": {update: "SET n = :one", condition: aws.String("n = = :one")},
		"operand type":     {update: "SET n = s + :one"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			table, err := createPokemonTable()
			require.NoError(t, err)

			key := map[string]*types.Item{"id": {S: aws.String("a")}, "name": {S: aws.String("b")}}
			item := map[string]*types.Item{"id": key["id"], "name": key["name"], "s": {S: aws.String("x")}}

			_, err = table.Put(&types.PutItemInput{Item: item, TableName: &table.Name})
			require.NoError(t, err)

			_, err = table.Update(&types.UpdateItemInput{
				TableName:                 &table.Name,
				Key:                       key,
				UpdateExpression:          tt.update,
				ConditionExpression:       tt.condition,
				ExpressionAttributeValues: map[string]*types.Item{":one": {N: aws.String("1")}},
			})
			require.Error(t, err)

			var typed types.Error
			require.True(t, errors.As(err, &typed), "%T: %v", err, err)
			require.Equal(t, "ValidationException", typed.Code())
			require.False(t, errors.Is(err, interpreter.ErrSyntaxError))
		})
	}
}

// listIndexCase is one request that carries a list index, and the label of the expression the
// index is in.
type listIndexCase struct {
	run   func(*Table, map[string]*types.Item) error
	label string
}

// listIndexCases returns a request for every kind of expression and position that carries the
// list index text index: the same positions DynamoDB Local was probed in (#164).
func listIndexCases(index string, values map[string]*types.Item) map[string]listIndexCase {
	update := func(expression string, values map[string]*types.Item) func(*Table, map[string]*types.Item) error {
		return func(table *Table, key map[string]*types.Item) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: expression, ExpressionAttributeValues: values,
			})

			return err
		}
	}
	at := "[" + index + "]"

	return map[string]listIndexCase{
		"SET":               {update("SET l"+at+" = :v", values), "UpdateExpression"},
		"SET right side":    {update("SET x = l"+at, values), "UpdateExpression"},
		"SET nested after":  {update("SET l"+at+".m = :v", values), "UpdateExpression"},
		"SET nested before": {update("SET m.l"+at+" = :v", values), "UpdateExpression"},
		"SET mixed":         {update("SET l[0]"+at+" = :v", values), "UpdateExpression"},
		"REMOVE":            {update("REMOVE l"+at, values), "UpdateExpression"},
		"ADD":               {update("ADD l"+at+" :one", values), "UpdateExpression"},
		"DELETE":            {update("DELETE l"+at+" :ss", values), "UpdateExpression"},
		"second clause":     {update("SET n = :one, l"+at+" = :v", values), "UpdateExpression"},
		"undefined value":   {update("SET l"+at+" = :v", map[string]*types.Item{":v": values[":v"]}), "UpdateExpression"},
		"no values":         {update("REMOVE l"+at, nil), "UpdateExpression"},
		"condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: "SET n = :one", ConditionExpression: new("l" + at + " = :v"),
				ExpressionAttributeValues: values,
			})

			return err
		}, "ConditionExpression"},
		"update before condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: "SET l" + at + " = :v", ConditionExpression: new("l" + at + " = :v"),
				ExpressionAttributeValues: values,
			})

			return err
		}, "UpdateExpression"},
		"put condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Put(&types.PutItemInput{
				TableName: &table.Name, Item: key, ConditionExpression: new("l" + at + " = :v"), ExpressionAttributeValues: values,
			})

			return err
		}, "ConditionExpression"},
		"delete condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Delete(&types.DeleteItemInput{
				TableName: &table.Name, Key: key, ConditionExpression: new("l" + at + " = :v"), ExpressionAttributeValues: values,
			})

			return err
		}, "ConditionExpression"},
		"scan filter": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{Scan: true, FilterExpression: "m.l" + at + " = :v", ExpressionAttributeValues: values})

			return err
		}, "FilterExpression"},
		"query filter": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression: "id = :v", FilterExpression: "l" + at + " = :v", ExpressionAttributeValues: values,
			})

			return err
		}, "FilterExpression"},
		"query key condition": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression: "id = :v AND l" + at + " = :v", ExpressionAttributeValues: values,
			})

			return err
		}, "KeyConditionExpression"},
		"query projection": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression: "id = :v", ProjectionExpression: "l" + at, ExpressionAttributeValues: values,
			})

			return err
		}, "ProjectionExpression"},
		"get projection": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.GetItem(key, "m.l"+at, nil)

			return err
		}, "ProjectionExpression"},
	}
}

func listIndexValues() map[string]*types.Item {
	return map[string]*types.Item{
		":i":   {N: new("1")},
		":v":   {S: new("x")},
		":one": {N: new("1")},
		":ss":  {SS: []*string{new("a")}},
		":z":   {S: new("unused")},
	}
}

// A list index is a literal. DynamoDB Local rejects a placeholder between the brackets as a
// syntax error that names the token, in every expression and before it checks the placeholders
// (#164, #267): an undefined :i or an unused value does not change the answer. The item is left
// as it was.
func TestTablePlaceholderListIndexIsASyntaxError(t *testing.T) {
	values := listIndexValues()
	tests := listIndexCases(":i", values)

	tests["name as index"] = listIndexCase{func(table *Table, key map[string]*types.Item) error {
		_, err := table.Update(&types.UpdateItemInput{
			TableName: &table.Name, Key: key, UpdateExpression: "SET l[#n] = :v", ExpressionAttributeValues: values,
		})

		return err
	}, "UpdateExpression"}

	// A keyword is a word between the brackets like any other, and keeps its case in the token.
	for _, keyword := range []string{"OR", "and", "set", "in", "between"} {
		for name, tt := range listIndexCases(keyword, values) {
			tests["keyword "+keyword+"/"+name] = tt
		}
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			table, key := listIndexTable(t)

			token := ":i"
			if name == "name as index" {
				token = "#n"
			}

			if k, _, found := strings.Cut(strings.TrimPrefix(name, "keyword "), "/"); found && strings.HasPrefix(name, "keyword ") {
				token = k
			}

			want := "Invalid " + tt.label + ": " + fmt.Sprintf(`Syntax error; token: %q, near: "[%s]"`, token, token)
			requireValidationError(t, tt.run(table, key), want)

			got, err := table.GetItem(key, "l", nil)
			require.NoError(t, err)
			require.Len(t, got.Projected["l"].L, 3)
		})
	}
}

// A literal index is 0 to 2147483647 with no leading zero, as in DynamoDB Local, in every
// expression and position (#164, #267). A literal above the range is "List index is not within
// the allowable range; index: [N]" whatever its size, a leading zero is a syntax error that
// names the token, and both come before the placeholder checks: an undefined value does not
// change the answer. The item is left as it was.
func TestTableLiteralListIndexLikeDynamoDBLocal(t *testing.T) {
	values := listIndexValues()

	rejected := map[string]string{
		"2147483648":                 "List index is not within the allowable range; index: [2147483648]",
		"4294967296":                 "List index is not within the allowable range; index: [4294967296]",
		"9223372036854775807":        "List index is not within the allowable range; index: [9223372036854775807]",
		"9223372036854775808":        "List index is not within the allowable range; index: [9223372036854775808]",
		"99999999999999999999999999": "List index is not within the allowable range; index: [99999999999999999999999999]",
		"007":                        `Syntax error; token: "0", near: "007"`,
		"00":                         `Syntax error; token: "0", near: "00]"`,
		"01":                         `Syntax error; token: "1", near: "01]"`,
	}

	for index, text := range rejected {
		for name, tt := range listIndexCases(index, values) {
			t.Run(index+"/"+name, func(t *testing.T) {
				table, key := listIndexTable(t)

				requireValidationError(t, tt.run(table, key), "Invalid "+tt.label+": "+text)

				got, err := table.GetItem(key, "l", nil)
				require.NoError(t, err)
				require.Len(t, got.Projected["l"].L, 3)
			})
		}
	}
}

// The boundary index and a zero alone are valid literals: an update there succeeds, and a
// condition on an index beyond the list is just false.
func TestTableUpdateLiteralListIndex(t *testing.T) {
	table, key := listIndexTable(t)
	values := map[string]*types.Item{":v": {S: new("x")}, ":one": {N: new("1")}}

	for _, expression := range []string{
		"SET l[1] = :v", "SET l[0] = :v, l[2] = :v", "REMOVE l[2]", "REMOVE l[2147483647]", "SET l[0] = :v REMOVE l[2147483647]",
	} {
		_, err := table.Update(&types.UpdateItemInput{
			TableName: &table.Name, Key: key, UpdateExpression: expression, ExpressionAttributeValues: values,
		})
		require.NoError(t, err, expression)
	}

	_, err := table.Update(&types.UpdateItemInput{
		TableName: &table.Name, Key: key, UpdateExpression: "SET n = :one", ConditionExpression: new("attribute_not_exists(l[2147483647])"),
		ExpressionAttributeValues: values,
	})
	require.NoError(t, err)
}
