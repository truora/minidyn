package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

const undefinedValueMessage = "An expression attribute value used in expression is not defined; attribute value: "

func requireUndefinedValueError(t *testing.T, err error, label, value string) {
	t.Helper()

	var apiErr types.Error

	require.Error(t, err)
	require.True(t, errors.As(err, &apiErr), "got %T: %v", err, err)
	require.Equal(t, "ValidationException", apiErr.Code())
	require.Equal(t, "Invalid "+label+": "+undefinedValueMessage+value, apiErr.Message())
}

func TestSearchData_undefinedExpressionAttributeValue(t *testing.T) {
	declared := map[string]*types.Item{":v": {S: new("x")}}

	cases := []struct {
		name  string
		input QueryInput
		label string
		value string
	}{
		{"key", QueryInput{KeyConditionExpression: "id = :nope"}, "KeyConditionExpression", ":nope"},
		{"filter", QueryInput{FilterExpression: "a = :nope", ExpressionAttributeValues: declared}, "FilterExpression", ":nope"},
		{"filter before key", QueryInput{
			KeyConditionExpression: "id = :k", FilterExpression: "a = :f",
		}, "FilterExpression", ":f"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newTable, err := createPokemonTable()
			require.NoError(t, err)

			// Empty table: the check must not depend on an item being evaluated.
			_, err = newTable.SearchData(tc.input)
			requireUndefinedValueError(t, err, tc.label, tc.value)
		})
	}
}

func TestWrites_undefinedExpressionAttributeValue(t *testing.T) {
	newTable, err := createPokemonTable()
	require.NoError(t, err)

	item := createPokemon(pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	key := primaryKeyFromPokemonItem(item)

	_, err = newTable.Update(&types.UpdateItemInput{Key: key, UpdateExpression: "SET a = :nope"})
	requireUndefinedValueError(t, err, "UpdateExpression", ":nope")

	// The update expression is reported before the condition.
	_, err = newTable.Update(&types.UpdateItemInput{
		Key: key, UpdateExpression: "SET a = :u", ConditionExpression: new("b = :c"),
	})
	requireUndefinedValueError(t, err, "UpdateExpression", ":u")

	_, err = newTable.Update(&types.UpdateItemInput{
		Key: key, UpdateExpression: "REMOVE a", ConditionExpression: new("b = :nope"),
	})
	requireUndefinedValueError(t, err, "ConditionExpression", ":nope")

	_, err = newTable.Put(&types.PutItemInput{Item: item, ConditionExpression: new("a = :nope")})
	requireUndefinedValueError(t, err, "ConditionExpression", ":nope")

	_, err = newTable.Delete(&types.DeleteItemInput{Key: key, ConditionExpression: new("a = :nope")})
	requireUndefinedValueError(t, err, "ConditionExpression", ":nope")
}
