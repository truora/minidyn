package core

import (
	"errors"
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

// A list index is a literal. DynamoDB Local rejects a placeholder between the brackets as a
// syntax error that names the token, in every expression and before it checks the placeholders
// (#164, #267): an undefined :i or an unused value does not change the answer. The item is left
// as it was.
func TestTablePlaceholderListIndexIsASyntaxError(t *testing.T) {
	const near = `Syntax error; token: ":i", near: "[:i]"`

	values := map[string]*types.Item{
		":i":   {N: new("1")},
		":v":   {S: new("x")},
		":one": {N: new("1")},
		":ss":  {SS: []*string{new("a")}},
		":z":   {S: new("unused")},
	}

	update := func(expression string, values map[string]*types.Item) func(*Table, map[string]*types.Item) error {
		return func(table *Table, key map[string]*types.Item) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: expression, ExpressionAttributeValues: values,
			})

			return err
		}
	}

	tests := map[string]struct {
		run   func(*Table, map[string]*types.Item) error
		label string
	}{
		"SET":               {update("SET l[:i] = :v", values), "UpdateExpression"},
		"SET right side":    {update("SET x = l[:i]", values), "UpdateExpression"},
		"SET nested after":  {update("SET l[:i].m = :v", values), "UpdateExpression"},
		"SET nested before": {update("SET m.l[:i] = :v", values), "UpdateExpression"},
		"SET mixed":         {update("SET l[0][:i] = :v", values), "UpdateExpression"},
		"REMOVE":            {update("REMOVE l[:i]", values), "UpdateExpression"},
		"ADD":               {update("ADD l[:i] :one", values), "UpdateExpression"},
		"DELETE":            {update("DELETE l[:i] :ss", values), "UpdateExpression"},
		"second clause":     {update("SET n = :one, l[:i] = :v", values), "UpdateExpression"},
		"undefined value":   {update("SET l[:i] = :v", map[string]*types.Item{":v": values[":v"]}), "UpdateExpression"},
		"no values":         {update("REMOVE l[:i]", nil), "UpdateExpression"},
		"condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: "SET n = :one", ConditionExpression: new("l[:i] = :v"),
				ExpressionAttributeValues: values,
			})

			return err
		}, "ConditionExpression"},
		"update before condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: "SET l[:i] = :v", ConditionExpression: new("l[:i] = :v"),
				ExpressionAttributeValues: values,
			})

			return err
		}, "UpdateExpression"},
		"put condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Put(&types.PutItemInput{
				TableName: &table.Name, Item: key, ConditionExpression: new("l[:i] = :v"), ExpressionAttributeValues: values,
			})

			return err
		}, "ConditionExpression"},
		"delete condition": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.Delete(&types.DeleteItemInput{
				TableName: &table.Name, Key: key, ConditionExpression: new("l[:i] = :v"), ExpressionAttributeValues: values,
			})

			return err
		}, "ConditionExpression"},
		"scan filter": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{Scan: true, FilterExpression: "m.l[:i] = :v", ExpressionAttributeValues: values})

			return err
		}, "FilterExpression"},
		"query filter": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression: "id = :v", FilterExpression: "l[:i] = :v", ExpressionAttributeValues: values,
			})

			return err
		}, "FilterExpression"},
		"query key condition": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression: "id = :v AND l[:i] = :v", ExpressionAttributeValues: values,
			})

			return err
		}, "KeyConditionExpression"},
		"query projection": {func(table *Table, _ map[string]*types.Item) error {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression: "id = :v", ProjectionExpression: "l[:i]", ExpressionAttributeValues: values,
			})

			return err
		}, "ProjectionExpression"},
		"get projection": {func(table *Table, key map[string]*types.Item) error {
			_, err := table.GetItem(key, "m.l[:i]", nil)

			return err
		}, "ProjectionExpression"},
		"name as index": {update("SET l[#n] = :v", values), "UpdateExpression"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			table, key := listIndexTable(t)

			token := ":i"
			if name == "name as index" {
				token = "#n"
			}

			want := "Invalid " + tt.label + ": " + strings.ReplaceAll(near, ":i", token)
			requireValidationError(t, tt.run(table, key), want)

			got, err := table.GetItem(key, "l", nil)
			require.NoError(t, err)
			require.Len(t, got.Projected["l"].L, 3)
		})
	}
}

// A literal index is still valid, and one that does not fit an int64 is minidyn's own error
// (D1 level 3: DynamoDB Local has a limit of its own, tracked apart), which names the index and
// leaves the item as it was (#267).
func TestTableUpdateLiteralListIndex(t *testing.T) {
	const prefix = "The document path provided in the update expression is invalid for update | field id: l; path: "

	table, key := listIndexTable(t)
	values := map[string]*types.Item{":v": {S: new("x")}, ":one": {N: new("1")}}

	for _, expression := range []string{"SET l[1] = :v", "SET l[0] = :v, l[2] = :v", "REMOVE l[2]"} {
		_, err := table.Update(&types.UpdateItemInput{
			TableName: &table.Name, Key: key, UpdateExpression: expression, ExpressionAttributeValues: values,
		})
		require.NoError(t, err, expression)
	}

	tests := map[string]string{
		"SET l[9223372036854775808] = :v":           prefix + "9223372036854775808; reason: list index is out of range",
		"REMOVE l[99999999999999999999]":            prefix + "99999999999999999999; reason: list index is out of range",
		"ADD l[9223372036854775808] :one":           prefix + "9223372036854775808; reason: list index is out of range",
		"SET n = :one, l[9223372036854775808] = :v": prefix + "9223372036854775808; reason: list index is out of range",
	}

	for expression, want := range tests {
		table, key := listIndexTable(t)

		_, err := table.Update(&types.UpdateItemInput{
			TableName: &table.Name, Key: key, UpdateExpression: expression, ExpressionAttributeValues: values,
		})
		requireValidationError(t, err, want)

		got, err := table.GetItem(key, "l", nil)
		require.NoError(t, err)
		require.Len(t, got.Projected["l"].L, 3, expression)
	}
}

// A condition is not an update expression, so it must not borrow DynamoDB's update message for a
// literal index out of range (D4). Every error of a condition expression carries the syntax
// error prefix.
func TestTableConditionLiteralListIndexOutOfRange(t *testing.T) {
	table, key := listIndexTable(t)

	_, err := table.Update(&types.UpdateItemInput{
		TableName:           &table.Name,
		Key:                 key,
		UpdateExpression:    "SET n = :one",
		ConditionExpression: new("l[9223372036854775808] = :x"),
		ExpressionAttributeValues: map[string]*types.Item{
			":x":   {S: new("b")},
			":one": {N: new("1")},
		},
	})
	requireValidationError(t, err, "Syntax error: ERROR: invalid list index in a document path | path: 9223372036854775808; reason: list index is out of range")
}
