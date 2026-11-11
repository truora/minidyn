package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

func TestTableGetItem(t *testing.T) {
	table, err := createPokemonTable()
	require.NoError(t, err)

	item := createPokemon(pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	_, err = table.Put(&types.PutItemInput{Item: item, TableName: &table.Name})
	require.NoError(t, err)

	key := primaryKeyFromPokemonItem(item)

	t.Run("returns the stored item and the projection", func(t *testing.T) {
		got, err := table.GetItem(key, "", nil)
		require.NoError(t, err)
		require.Equal(t, item, got.Stored)
		require.Equal(t, item, got.Projected)

		got, err = table.GetItem(key, "#n, id", map[string]string{"#n": "name"})
		require.NoError(t, err)
		require.Equal(t, item, got.Stored)
		require.Equal(t, map[string]*types.Item{"id": item["id"], "name": item["name"]}, got.Projected)
	})

	t.Run("a missing item is empty", func(t *testing.T) {
		missing := map[string]*types.Item{"id": {S: new("404")}, "name": {S: new("x")}}

		got, err := table.GetItem(missing, "id", nil)
		require.NoError(t, err)
		require.Empty(t, got.Stored)
		require.Empty(t, got.Projected)
	})

	const (
		syntaxMsg     = "Invalid ProjectionExpression: "
		undeclaredMsg = "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"
	)

	// D3: DynamoDB reports the projection before the key, whatever is wrong with the key.
	keys := map[string]map[string]*types.Item{
		"empty key value":   {"id": {S: new("a")}, "name": emptyS()},
		"missing range key": {"id": {S: new("a")}},
		"key type mismatch": {"id": {S: new("a")}, "name": nItem("1")},
		"extra attribute":   {"id": {S: new("a")}, "name": {S: new("b")}, "x": {S: new("y")}},
	}

	for name, badKey := range keys {
		t.Run("projection before the key: "+name, func(t *testing.T) {
			_, err := table.GetItem(badKey, "#x", nil)
			requireValidationError(t, err, undeclaredMsg)

			_, err = table.GetItem(badKey, "id,,", nil)
			require.ErrorContains(t, err, "ValidationException: "+syntaxMsg)

			_, err = table.GetItem(badKey, "id", nil)
			require.Error(t, err)
			require.NotContains(t, err.Error(), syntaxMsg)
		})
	}

	t.Run("key errors", func(t *testing.T) {
		_, err := table.GetItem(keys["empty key value"], "", nil)
		requireValidationError(t, err, emptyStringKeyMsg+" Key: name")

		_, err = table.GetItem(keys["missing range key"], "", nil)
		require.ErrorContains(t, err, "ValidationException: "+wrongKeyCountMsg)
	})
}

func requireValidationError(t *testing.T, err error, msg string) {
	t.Helper()

	var tErr types.Error

	require.ErrorAs(t, err, &tErr)
	require.Equal(t, "ValidationException", tErr.Code())
	require.Equal(t, msg, tErr.Message())
}

// DynamoDB validates every key of a table in a BatchGetItem before the ProjectionExpression, the
// opposite of GetItem, and checks the projection once per table.
func TestTableBatchGetItem(t *testing.T) {
	table, err := createPokemonTable()
	require.NoError(t, err)

	item := createPokemon(pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	_, err = table.Put(&types.PutItemInput{Item: item, TableName: &table.Name})
	require.NoError(t, err)

	okKey := primaryKeyFromPokemonItem(item)
	missing := map[string]*types.Item{"id": {S: new("404")}, "name": {S: new("x")}}

	const (
		syntaxMsg     = "Invalid ProjectionExpression: "
		undeclaredMsg = "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"
	)

	t.Run("one entry per key, in order", func(t *testing.T) {
		got, err := table.BatchGetItem([]map[string]*types.Item{missing, okKey}, "#n", map[string]string{"#n": "name"})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Empty(t, got[0].Stored)
		require.Empty(t, got[0].Projected)
		require.Equal(t, item, got[1].Stored)
		require.Equal(t, map[string]*types.Item{"name": item["name"]}, got[1].Projected)
	})

	t.Run("no projection", func(t *testing.T) {
		got, err := table.BatchGetItem([]map[string]*types.Item{okKey}, "", nil)
		require.NoError(t, err)
		require.Equal(t, item, got[0].Projected)
	})

	badKeys := map[string]struct {
		key map[string]*types.Item
		msg string
	}{
		"empty key value":   {map[string]*types.Item{"id": {S: new("a")}, "name": emptyS()}, emptyStringKeyMsg + " Key: name"},
		"missing range key": {map[string]*types.Item{"id": {S: new("a")}}, wrongKeyCountMsg},
	}

	for name, tt := range badKeys {
		t.Run("key before projection: "+name, func(t *testing.T) {
			for _, expr := range []string{"#x", "id,,"} {
				_, err := table.BatchGetItem([]map[string]*types.Item{tt.key}, expr, nil)
				require.ErrorContains(t, err, "ValidationException: "+tt.msg)
			}
		})

		t.Run("second key bad: "+name, func(t *testing.T) {
			for _, expr := range []string{"#x", "id,,"} {
				_, err := table.BatchGetItem([]map[string]*types.Item{okKey, tt.key}, expr, nil)
				require.ErrorContains(t, err, "ValidationException: "+tt.msg)
			}
		})
	}

	t.Run("valid keys still check the projection", func(t *testing.T) {
		keys := []map[string]*types.Item{okKey, missing}

		_, err := table.BatchGetItem(keys, "#x", nil)
		requireValidationError(t, err, undeclaredMsg)

		_, err = table.BatchGetItem(keys, "id,,", nil)
		require.ErrorContains(t, err, "ValidationException: "+syntaxMsg)
	})
}
