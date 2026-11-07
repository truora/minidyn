package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

const undeclaredNameMessage = "An expression attribute name used in the document path is not defined; attribute name: "

func TestDeleteAliases(t *testing.T) {
	require.Equal(t, map[string]string{"#a": "attr"}, deleteAliases(map[string]*string{"#a": new("attr"), "#nil": nil}))
	require.Empty(t, deleteAliases(nil))
}

// DynamoDB reports an undeclared #name before an undefined :value in writes (#190).
func TestWrites_undeclaredNameBeforeUndefinedValue(t *testing.T) {
	newTable, err := createPokemonTable()
	require.NoError(t, err)

	item := createPokemon(pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	key := primaryKeyFromPokemonItem(item)
	declared := map[string]*types.Item{":v": {S: new("x")}}

	requireNameError := func(err error, label string) {
		t.Helper()

		var apiErr types.Error

		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "Invalid "+label+": "+undeclaredNameMessage+"#n", apiErr.Message())
	}

	_, err = newTable.Update(&types.UpdateItemInput{
		Key: key, UpdateExpression: "SET #n = :v", ConditionExpression: new("b = :nope"),
		ExpressionAttributeValues: declared,
	})
	requireNameError(err, "UpdateExpression")

	_, err = newTable.Update(&types.UpdateItemInput{Key: key, UpdateExpression: "SET #n = :nope"})
	requireNameError(err, "UpdateExpression")

	_, err = newTable.Put(&types.PutItemInput{Item: item, ConditionExpression: new("#n = :nope")})
	requireNameError(err, "ConditionExpression")

	_, err = newTable.Delete(&types.DeleteItemInput{Key: key, ConditionExpression: new("#n = :nope")})
	requireNameError(err, "ConditionExpression")
}
