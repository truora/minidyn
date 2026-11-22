package core

import (
	"strconv"
	"strings"
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

func listIndexTable(t *testing.T) (*Table, map[string]*types.Item) {
	t.Helper()

	table, err := createPokemonTable()
	require.NoError(t, err)

	item := createPokemon(pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	item["l"] = &types.Item{L: []*types.Item{{S: new("a")}, {S: new("b")}, {S: new("c")}}}

	_, err = table.Put(&types.PutItemInput{Item: item, TableName: &table.Name})
	require.NoError(t, err)

	return table, primaryKeyFromPokemonItem(item)
}

// A projection of a list element returns a list with only the selected elements; before #262 it
// held a nil element for the ones skipped, which the front ends could not encode.
func TestTableGetItemListElementProjection(t *testing.T) {
	table, key := listIndexTable(t)

	str := func(v string) *types.Item { return &types.Item{S: &v} }

	tests := map[string]struct {
		projection string
		want       map[string]*types.Item
	}{
		"element":       {"l[1]", map[string]*types.Item{"l": {L: []*types.Item{str("b")}}}},
		"in list order": {"l[2], l[0]", map[string]*types.Item{"l": {L: []*types.Item{str("a"), str("c")}}}},
		"past the end":  {"l[5]", map[string]*types.Item{}},
		"missing list":  {"nope[1]", map[string]*types.Item{}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := table.GetItem(key, tt.projection, nil)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Projected)

			batch, err := table.BatchGetItem([]map[string]*types.Item{key}, tt.projection, nil)
			require.NoError(t, err)
			require.Len(t, batch, 1)
			require.Equal(t, tt.want, batch[0].Projected)
		})
	}
}

// DynamoDB checks the keys of a BatchGetItem before it looks at any key or projection, and
// compares them as the request spells them (#246): a duplicate is reported even when the key is
// invalid, equal numbers spelled differently are not duplicates, and neither are the same keys of
// two tables.
func TestValidateBatchGetKeys(t *testing.T) {
	s := func(v string) *types.Item { return &types.Item{S: &v} }
	n := func(v string) *types.Item { return &types.Item{N: &v} }
	key := func(id, name string, extra ...string) map[string]*types.Item {
		m := map[string]*types.Item{"id": s(id), "name": s(name)}
		for i := 0; i+1 < len(extra); i += 2 {
			m[extra[i]] = s(extra[i+1])
		}

		return m
	}
	keys := func(k ...map[string]*types.Item) []map[string]*types.Item { return k }
	many := func(count int) []map[string]*types.Item {
		out := make([]map[string]*types.Item, count)
		for i := range out {
			out[i] = key("a", strconv.Itoa(i))
		}

		return out
	}

	const (
		dupMsg   = "Provided list of item keys contains duplicates"
		limitMsg = "Too many items requested for the BatchGetItem call"
	)

	type reqs = map[string][]map[string]*types.Item

	tests := []struct {
		name    string
		reqs    reqs
		message string
	}{
		{"valid", reqs{"t": keys(key("1", "a"), key("1", "b")), "u": keys(key("1", "a"))}, ""},
		{"duplicate", reqs{"t": keys(key("1", "a"), key("1", "a"))}, dupMsg},
		{"not adjacent", reqs{"t": keys(key("1", "a"), key("2", "a"), key("1", "a"))}, dupMsg},
		{"duplicate with extra attributes", reqs{"t": keys(key("1", "a", "x", "1"), key("1", "a", "x", "1"))}, dupMsg},
		{"different extra attributes are not duplicates", reqs{"t": keys(key("1", "a", "x", "1"), key("1", "a", "x", "2"))}, ""},
		{"duplicate of an empty key", reqs{"t": keys(key("", "a"), key("", "a"))}, dupMsg},
		{"duplicate of a key with a missing attribute", reqs{
			"t": keys(map[string]*types.Item{"id": s("1")}, map[string]*types.Item{"id": s("1")}),
		}, dupMsg},
		{"duplicate of a key of the wrong type", reqs{
			"t": keys(map[string]*types.Item{"id": n("1")}, map[string]*types.Item{"id": n("1")}),
		}, dupMsg},
		// the request is compared as it is spelled
		{"equal numbers spelled differently", reqs{
			"t": keys(map[string]*types.Item{"id": n("1")}, map[string]*types.Item{"id": n("1.0")}),
		}, ""},
		{"duplicate in the second table", reqs{"t": keys(key("1", "a")), "u": keys(key("2", "a"), key("2", "a"))}, dupMsg},
		{"duplicate in an unknown table", reqs{"zzz": keys(key("1", "a"), key("1", "a"))}, dupMsg},
		{"no tables", reqs{}, "BatchGetItem must have some requests set"},
		{"no keys", reqs{"t": nil}, "The list of keys in RequestItems for BatchGetItem is required: t has empty list"},
		{
			"no keys in the second table",
			reqs{"t": keys(key("1", "a")), "u": {}},
			"The list of keys in RequestItems for BatchGetItem is required: u has empty list",
		},
		{"100 keys", reqs{"t": many(100)}, ""},
		{"101 keys", reqs{"t": many(101)}, limitMsg},
		{"101 keys over two tables", reqs{"t": many(60), "u": many(41)}, limitMsg},
		// within a table each key is compared before it is counted
		{"duplicate as the 101st key", reqs{"t": append(many(100), key("a", "1"))}, dupMsg},
		// tables go by name: t has 100 keys, so the first key of u is the 101st
		{"duplicate in the table after the limit", reqs{"t": many(100), "u": keys(key("1", "a"), key("1", "a"))}, limitMsg},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBatchGetKeys(tt.reqs)
			if tt.message == "" {
				require.NoError(t, err)

				return
			}

			var typed types.Error

			require.ErrorAs(t, err, &typed)
			require.Equal(t, "ValidationException", typed.Code())
			require.Contains(t, typed.Message(), tt.message)
		})
	}
}

func benchBatchGetKeys(count int) map[string][]map[string]*types.Item {
	keys := make([]map[string]*types.Item, count)

	for i := range keys {
		a, b := "a", strconv.Itoa(i)
		keys[i] = map[string]*types.Item{"id": {S: &a}, "name": {S: &b}}
	}

	return map[string][]map[string]*types.Item{"t": keys}
}

// BenchmarkValidateBatchGetKeys100 pins the cost of the duplicate scan of a full BatchGetItem:
// it has to be linear in the keys, not a comparison of each key with all the keys before it.
func BenchmarkValidateBatchGetKeys100(b *testing.B) {
	requests := benchBatchGetKeys(100)

	b.ReportAllocs()

	for b.Loop() {
		if err := ValidateBatchGetKeys(requests); err != nil {
			b.Fatal(err)
		}
	}
}

// A full BatchGetItem is back near the cost it had before the duplicate scan: about 45 us on
// main, 1.9 ms with the quadratic scan. The bound is 20x the first and under the second.
func TestValidateBatchGetKeysCostOf100Keys(t *testing.T) {
	requests := benchBatchGetKeys(100)

	// Allocations, not time, so the guard holds on any machine and under -race: the
	// pairwise reflect.DeepEqual scan this replaced made about 9,300 per call, the
	// linear set about 900. BenchmarkValidateBatchGetKeys100 measures the time.
	allocs := testing.AllocsPerRun(20, func() {
		_ = ValidateBatchGetKeys(requests)
	})

	require.Less(t, allocs, 2000.0, "allocs per call: %v", allocs)
}

// The position of a duplicate is in the detail, never the key.
func TestValidateBatchGetKeysDuplicateDetail(t *testing.T) {
	s := func(v string) *types.Item { return &types.Item{S: &v} }
	k := func(v string) map[string]*types.Item { return map[string]*types.Item{"id": s(v)} }

	err := ValidateBatchGetKeys(map[string][]map[string]*types.Item{"t": {k("secret"), k("b"), k("secret")}})

	var typed types.Error

	require.ErrorAs(t, err, &typed)
	require.True(t, strings.HasSuffix(typed.Message(), " | table: t; key: 2"), typed.Message())
	require.NotContains(t, typed.Message(), "secret")
}
