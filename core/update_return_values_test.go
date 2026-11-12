package core

import (
	"errors"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

func updatedValuesSS(vals ...string) *types.Item {
	out := &types.Item{}
	for _, v := range vals {
		out.SS = append(out.SS, aws.String(v))
	}

	return out
}

func updatedValuesItem() map[string]*types.Item {
	return map[string]*types.Item{
		"id":   {S: aws.String("a")},
		"name": {S: aws.String("b")},
		"n":    {N: aws.String("1")},
		"s":    {S: aws.String("foo")},
		"set":  updatedValuesSS("x", "y", "w"),
		"m": {M: map[string]*types.Item{
			"n":  {N: aws.String("1")},
			"ss": updatedValuesSS("x", "y", "w"),
		}},
		"l": {L: []*types.Item{{N: aws.String("1")}, {S: aws.String("a")}, updatedValuesSS("x", "y", "w")}},
	}
}

// updatedValuesRun puts base (or nothing when nil) and runs expr with the return values mode.
func updatedValuesRun(t *testing.T, base map[string]*types.Item, expr string, names map[string]string,
	values map[string]*types.Item, rv string,
) map[string]*types.Item {
	t.Helper()

	table, err := createPokemonTable()
	require.NoError(t, err)

	if base != nil {
		_, err = table.Put(&types.PutItemInput{Item: base, TableName: &table.Name})
		require.NoError(t, err)
	}

	out, err := table.Update(&types.UpdateItemInput{
		TableName:                 &table.Name,
		Key:                       map[string]*types.Item{"id": {S: aws.String("a")}, "name": {S: aws.String("b")}},
		ReturnValues:              aws.String(rv),
		UpdateExpression:          expr,
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	require.NoError(t, err)

	return out
}

// UpdateItem rejects an invalid ReturnValues with the text of PutItem and DeleteItem, and
// before it looks at the key.
func TestUpdateReturnValuesMessageAndOrder(t *testing.T) {
	for _, rv := range []*string{nil, new(""), new("NONE"), new("ALL_OLD"), new("UPDATED_OLD"), new("UPDATED_NEW"), new("ALL_NEW")} {
		require.NoError(t, ValidateUpdateItemReturnValues(rv))
	}

	for _, bad := range []string{"BOGUS", "all_new", "ALL_NEWS"} {
		err := ValidateUpdateItemReturnValues(&bad)

		var tErr types.Error

		require.True(t, errors.As(err, &tErr), bad)
		require.Equal(t, "ValidationException", tErr.Code())
		require.Equal(t, "Return values set to invalid value"+types.Detail("value", bad, "field", "ReturnValues"), tErr.Message())
	}

	table, err := createPokemonTable()
	require.NoError(t, err)

	_, err = table.Update(&types.UpdateItemInput{
		TableName:        &table.Name,
		Key:              map[string]*types.Item{"id": {S: aws.String("")}, "name": {S: aws.String("b")}},
		ReturnValues:     aws.String("BOGUS"),
		UpdateExpression: "SET x = :v",
	})
	require.ErrorContains(t, err, "Return values set to invalid value")
	require.NotContains(t, err.Error(), "validation error detected")

	// It comes before the expressions too: an undeclared #name, a syntax error.
	for name, expr := range map[string]string{"undeclared name": "SET #n = :v", "syntax error": "SET"} {
		_, err = table.Update(&types.UpdateItemInput{
			TableName:                 &table.Name,
			Key:                       map[string]*types.Item{"id": {S: aws.String("a")}, "name": {S: aws.String("b")}},
			ReturnValues:              aws.String("BOGUS"),
			UpdateExpression:          expr,
			ExpressionAttributeValues: map[string]*types.Item{":v": {S: aws.String("x")}},
		})
		require.ErrorContains(t, err, "Return values set to invalid value", name)
	}
}

func sortedKeys(m map[string]*types.Item) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func TestUpdateUpdatedValuesIncludeUnchangedWrites(t *testing.T) {
	c := require.New(t)
	values := map[string]*types.Item{":v": {N: aws.String("2")}, ":s": {S: aws.String("foo")}}
	expr := "SET n = if_not_exists(n, :v), s = :s"

	oldOut := updatedValuesRun(t, updatedValuesItem(), expr, nil, values, "UPDATED_OLD")
	c.Equal([]string{"n", "s"}, sortedKeys(oldOut))
	c.Equal("1", *oldOut["n"].N)
	c.Equal("foo", *oldOut["s"].S)

	newOut := updatedValuesRun(t, updatedValuesItem(), expr, nil, values, "UPDATED_NEW")
	c.Equal([]string{"n", "s"}, sortedKeys(newOut))
	c.Equal("1", *newOut["n"].N)
	c.Equal("foo", *newOut["s"].S)
}

func TestUpdateUpdatedValuesSkipUntouchedSets(t *testing.T) {
	c := require.New(t)
	values := map[string]*types.Item{":v": {S: aws.String("nick")}}

	// Set element order is random on every write: untouched sets must never be reported.
	for range 100 {
		newOut := updatedValuesRun(t, updatedValuesItem(), "SET nickname = :v", nil, values, "UPDATED_NEW")
		c.Equal([]string{"nickname"}, sortedKeys(newOut))

		oldOut := updatedValuesRun(t, updatedValuesItem(), "SET nickname = :v", nil, values, "UPDATED_OLD")
		c.Empty(oldOut)
	}
}

func TestUpdateUpdatedValuesPaths(t *testing.T) {
	one := &types.Item{N: aws.String("1")}
	values := map[string]*types.Item{
		":one": one,
		":i":   one,
		":str": {S: aws.String("x")},
		":sx":  updatedValuesSS("x"),
		":sxy": updatedValuesSS("x", "y", "w"),
		":sz":  updatedValuesSS("z"),
	}

	tests := []struct {
		name     string
		upsert   bool
		expr     string
		names    map[string]string
		wantOld  []string
		wantNew  []string
		checkNew func(*require.Assertions, map[string]*types.Item)
	}{
		{name: "set same value", expr: "SET n = :one", wantOld: []string{"n"}, wantNew: []string{"n"}},
		{name: "set alias", expr: "SET #a = :one", names: map[string]string{"#a": "n"}, wantOld: []string{"n"}, wantNew: []string{"n"}},
		{name: "remove missing attribute", expr: "REMOVE nope", wantOld: nil, wantNew: nil},
		{name: "remove present attribute", expr: "REMOVE s", wantOld: []string{"s"}, wantNew: nil},
		{name: "add creates attribute", expr: "ADD created :one", wantOld: nil, wantNew: []string{"created"}},
		{name: "add existing element", expr: "ADD #st :sx", names: map[string]string{"#st": "set"}, wantOld: []string{"set"}, wantNew: []string{"set"}},
		{name: "delete missing element", expr: "DELETE #st :sz", names: map[string]string{"#st": "set"}, wantOld: []string{"set"}, wantNew: []string{"set"}},
		{name: "delete empties set", expr: "DELETE #st :sxy", names: map[string]string{"#st": "set"}, wantOld: []string{"set"}, wantNew: nil},
		{
			name: "all clauses", expr: "SET n = :one REMOVE s ADD created :one DELETE #st :sx", names: map[string]string{"#st": "set"},
			wantOld: []string{"n", "s", "set"}, wantNew: []string{"created", "n", "set"},
		},
		{
			name: "nested set returns top level attribute", expr: "SET m.n = :one", wantOld: []string{"m"}, wantNew: []string{"m"},
			checkNew: func(c *require.Assertions, out map[string]*types.Item) {
				c.Len(out["m"].M, 2)
			},
		},
		{name: "nested set creating key", expr: "SET m.fresh = :str", wantOld: nil, wantNew: []string{"m"}},
		{name: "nested remove missing key", expr: "REMOVE m.nope", wantOld: nil, wantNew: nil},
		{name: "nested remove present key", expr: "REMOVE m.n", wantOld: []string{"m"}, wantNew: nil},
		{name: "nested delete from set", expr: "DELETE m.ss :sx", wantOld: []string{"m"}, wantNew: []string{"m"}},
		{name: "nested delete empties set", expr: "DELETE m.ss :sxy", wantOld: []string{"m"}, wantNew: nil},
		{name: "list index set", expr: "SET l[1] = :str", wantOld: []string{"l"}, wantNew: []string{"l"}},
		{name: "list index append", expr: "SET l[3] = :str", wantOld: nil, wantNew: []string{"l"}},
		{name: "list index append beyond end", expr: "SET l[9] = :str", wantOld: nil, wantNew: nil},
		{name: "list index remove", expr: "REMOVE l[0]", wantOld: []string{"l"}, wantNew: []string{"l"}},
		{name: "list index remove last", expr: "REMOVE l[2]", wantOld: []string{"l"}, wantNew: nil},
		{name: "list index remove out of range", expr: "REMOVE l[9]", wantOld: nil, wantNew: nil},
		{name: "value index then plain", expr: "SET l[:i] = :str, n = :one", wantOld: []string{"l", "n"}, wantNew: []string{"l", "n"}},
		{name: "plain then value index", expr: "SET n = :one, l[:i] = :str", wantOld: []string{"l", "n"}, wantNew: []string{"l", "n"}},
		{name: "upsert", upsert: true, expr: "SET n = :one, s = :str", wantOld: nil, wantNew: []string{"n", "s"}},
		{name: "upsert remove", upsert: true, expr: "REMOVE n", wantOld: nil, wantNew: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)

			var base map[string]*types.Item
			if !tt.upsert {
				base = updatedValuesItem()
			}

			oldOut := updatedValuesRun(t, base, tt.expr, tt.names, values, "UPDATED_OLD")
			c.ElementsMatch(tt.wantOld, sortedKeys(oldOut))

			newOut := updatedValuesRun(t, base, tt.expr, tt.names, values, "UPDATED_NEW")
			c.ElementsMatch(tt.wantNew, sortedKeys(newOut))

			if tt.checkNew != nil {
				tt.checkNew(c, newOut)
			}
		})
	}
}
