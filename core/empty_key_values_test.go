package core

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

const (
	emptyStringKeyMsg = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value."
	emptyBinaryKeyMsg = "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value."
	secondaryKeyMsg   = "One or more parameter values are not valid. A value specified for a secondary index key is not supported. "
	updateIndexKeyMsg = "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty string value."
	betweenOrderMsg   = "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: {S:0}, upper bound operand: AttributeValue: {S:} | attribute: name"
)

func emptyS() *types.Item { return &types.Item{S: new("")} }
func emptyB() *types.Item { return &types.Item{B: []byte{}} }

func TestSearchData_emptyKeyConditionValues(t *testing.T) {
	table := createKeyConditionTable(t)
	a, z, zero := &types.Item{S: new("a")}, &types.Item{S: new("z")}, &types.Item{S: new("0")}

	cases := []struct {
		name   string
		index  string
		expr   string
		values map[string]*types.Item
		want   string
	}{
		{"hash", "", "id = :p", map[string]*types.Item{":p": emptyS()}, emptyStringKeyMsg + " Key: id"},
		{"range equals", "", "id = :p AND name = :s", map[string]*types.Item{":p": a, ":s": emptyS()}, emptyStringKeyMsg + " Key: name"},
		{"range less", "", "id = :p AND name < :s", map[string]*types.Item{":p": a, ":s": emptyS()}, emptyStringKeyMsg + " Key: name"},
		{"reversed operands", "", ":p = id AND :s < name", map[string]*types.Item{":p": a, ":s": emptyS()}, emptyStringKeyMsg + " Key: name"},
		{"between low", "", "id = :p AND name BETWEEN :a AND :b", map[string]*types.Item{":p": a, ":a": emptyS(), ":b": z}, emptyStringKeyMsg + " Key: name"},
		{"between both", "", "id = :p AND name BETWEEN :a AND :b", map[string]*types.Item{":p": a, ":a": emptyS(), ":b": emptyS()}, emptyStringKeyMsg + " Key: name"},
		{"between high", "", "id = :p AND name BETWEEN :a AND :b", map[string]*types.Item{":p": a, ":a": zero, ":b": emptyS()}, betweenOrderMsg},
		{
			"between reversed", "", "id = :p AND name BETWEEN :a AND :b",
			map[string]*types.Item{":p": a, ":a": z, ":b": a},
			"Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a} | attribute: name",
		},
		{
			"gsi between reversed", "by-type", "type = :p AND rank BETWEEN :a AND :b",
			map[string]*types.Item{":p": a, ":a": z, ":b": a},
			"Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a} | attribute: rank; index: by-type",
		},
		{"begins_with", "", "id = :p AND begins_with(name, :s)", map[string]*types.Item{":p": a, ":s": emptyS()}, emptyStringKeyMsg + " Key: name"},
		{"both empty reports hash", "", "id = :p AND name = :s", map[string]*types.Item{":p": emptyS(), ":s": emptyS()}, emptyStringKeyMsg + " Key: id"},
		{"gsi hash", "by-type", "type = :p", map[string]*types.Item{":p": emptyS()}, secondaryKeyMsg + "The AttributeValue for a key attribute cannot contain an empty string value. IndexName: by-type, IndexKey: type"},
		{"gsi range", "by-type", "type = :p AND begins_with(rank, :s)", map[string]*types.Item{":p": a, ":s": emptyS()}, secondaryKeyMsg + "The AttributeValue for a key attribute cannot contain an empty string value. IndexName: by-type, IndexKey: rank"},
		{"non-empty", "", "id = :p AND name BETWEEN :a AND :b", map[string]*types.Item{":p": a, ":a": a, ":b": z}, ""},
		{"equal bounds", "", "id = :p AND name BETWEEN :a AND :b", map[string]*types.Item{":p": a, ":a": a, ":b": a}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := table.SearchData(QueryInput{
				Index:                     tc.index,
				KeyConditionExpression:    tc.expr,
				ExpressionAttributeValues: tc.values,
				ScanIndexForward:          true,
			})

			if tc.want == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSearchData_emptyBinaryKeyConditionValues(t *testing.T) {
	table := NewTable("bin")
	table.AttributesDef = map[string]string{"id": "B", "name": "B"}
	table.KeySchema = keySchema{"id", "name", false}

	one := &types.Item{B: []byte{1}}

	_, err := table.SearchData(QueryInput{
		KeyConditionExpression:    "id = :p AND begins_with(name, :s)",
		ExpressionAttributeValues: map[string]*types.Item{":p": one, ":s": emptyB()},
		ScanIndexForward:          true,
	})
	require.ErrorContains(t, err, emptyBinaryKeyMsg+" Key: name")

	_, err = table.SearchData(QueryInput{
		KeyConditionExpression:    "id = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": emptyB()},
		ScanIndexForward:          true,
	})
	require.ErrorContains(t, err, emptyBinaryKeyMsg+" Key: id")
}

func TestTable_emptyKeyValuesInItems(t *testing.T) {
	table := createKeyConditionTable(t)
	good := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}, "type": {S: new("t")}, "rank": {S: new("r")}}

	with := func(k string, v *types.Item) map[string]*types.Item {
		item := map[string]*types.Item{}
		maps.Copy(item, good)

		item[k] = v

		return item
	}

	_, err := table.Put(&types.PutItemInput{Item: good})
	require.NoError(t, err)

	t.Run("put empty table key", func(t *testing.T) {
		_, err := table.Put(&types.PutItemInput{Item: with("name", emptyS())})
		require.ErrorContains(t, err, emptyStringKeyMsg+" Key: name")

		_, err = table.Put(&types.PutItemInput{Item: with("id", emptyS())})
		require.ErrorContains(t, err, emptyStringKeyMsg+" Key: id")
	})

	t.Run("put empty index key", func(t *testing.T) {
		_, err := table.Put(&types.PutItemInput{Item: with("rank", emptyS())})
		require.ErrorContains(t, err, secondaryKeyMsg+"The AttributeValue for a key attribute cannot contain an empty string value. IndexName: by-type, IndexKey: rank")
		require.Len(t, table.Data, 1, "the rejected item is not stored")
	})

	t.Run("put empty non-key attribute", func(t *testing.T) {
		_, err := table.Put(&types.PutItemInput{Item: with("other", emptyS())})
		require.NoError(t, err)
	})

	t.Run("key maps", func(t *testing.T) {
		key := map[string]*types.Item{"id": {S: new("a")}, "name": emptyS()}
		require.ErrorContains(t, table.ValidatePrimaryKeyMap(key), emptyStringKeyMsg+" Key: name")

		_, err := table.Delete(&types.DeleteItemInput{Key: key})
		require.ErrorContains(t, err, emptyStringKeyMsg+" Key: name")

		_, err = table.Update(&types.UpdateItemInput{Key: key, UpdateExpression: "SET x = :v", ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("v")}}})
		require.ErrorContains(t, err, emptyStringKeyMsg+" Key: name")
	})

	t.Run("update sets empty index key", func(t *testing.T) {
		key := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}}
		_, err := table.Update(&types.UpdateItemInput{Key: key, UpdateExpression: "SET #r = :v", ExpressionAttributeNames: map[string]string{"#r": "rank"}, ExpressionAttributeValues: map[string]*types.Item{":v": emptyS()}})
		require.ErrorContains(t, err, updateIndexKeyMsg+" | index: by-type; attribute: rank")

		stored := table.StoredItem(key)
		require.Equal(t, "r", *stored["rank"].S, "the rejected update leaves the item as it was")
		requireIndexUnchanged(t, table)
	})

	t.Run("update fails in the interpreter after changing the item", func(t *testing.T) {
		key := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}}
		_, err := table.Update(&types.UpdateItemInput{
			Key:                       key,
			UpdateExpression:          "SET x = :v, y = #t + :v",
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("changed")}},
		})
		require.Error(t, err)

		stored := table.StoredItem(key)
		require.Equal(t, "t", *stored["type"].S)
		require.Equal(t, "r", *stored["rank"].S)
		require.NotContains(t, stored, "x", "the partial change is rolled back")
		requireIndexUnchanged(t, table)
	})

	t.Run("update sets empty non-key attribute", func(t *testing.T) {
		key := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}}
		_, err := table.Update(&types.UpdateItemInput{Key: key, UpdateExpression: "SET x = :v", ExpressionAttributeValues: map[string]*types.Item{":v": emptyS()}})
		require.NoError(t, err)
	})
}

// requireIndexUnchanged checks the by-type index still holds the one entry of the item put
// by the test, with its original values.
func requireIndexUnchanged(t *testing.T, table *Table) {
	t.Helper()

	idx := table.Indexes["by-type"]
	require.Len(t, idx.sortedKeys, 1)
	require.Len(t, idx.refs, 1)

	res, err := table.SearchData(QueryInput{
		Index:                     "by-type",
		KeyConditionExpression:    "#t = :t",
		Aliases:                   map[string]string{"#t": "type"},
		ExpressionAttributeValues: map[string]*types.Item{":t": {S: new("t")}},
		ScanIndexForward:          true,
	})
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	require.Equal(t, "r", *res.Items[0]["rank"].S)
	require.Equal(t, "n", *res.Items[0]["name"].S)
}
