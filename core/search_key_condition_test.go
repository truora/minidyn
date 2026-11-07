package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// createKeyConditionTable returns an empty table keyed by id (hash) and name (range) with a
// GSI keyed by type (hash) and rank (range), so the validation can only come from the up
// front schema check, never from an item.
func createKeyConditionTable(t *testing.T) *Table {
	t.Helper()

	table, err := createPokemonTable()
	require.NoError(t, err)

	table.AttributesDef["type"] = "S"
	table.AttributesDef["rank"] = "S"

	by, all := "by-type", "ALL"
	err = table.AddGlobalIndexes([]*types.GlobalSecondaryIndex{{
		IndexName:             &by,
		ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "type", KeyType: "HASH"},
			{AttributeName: "rank", KeyType: "RANGE"},
		},
		Projection: &types.Projection{ProjectionType: &all},
	}})
	require.NoError(t, err)

	return table
}

func TestSearchData_keyConditionSchema(t *testing.T) {
	aliases := map[string]string{"#s": "status", "#n": "name", "#i": "id"}
	values := map[string]*types.Item{
		":a": {S: new("a")},
		":b": {S: new("b")},
		":c": {S: new("c")},
	}

	cases := []struct {
		name    string
		index   string
		expr    string
		wantErr string // DynamoDB Local's message; empty means the query is valid
	}{
		{name: "hash only", expr: "id = :a"},
		{name: "hash and range", expr: "id = :a AND name = :b"},
		{name: "range first", expr: "name > :b AND id = :a"},
		{name: "aliases", expr: "#i = :a AND #n BETWEEN :b AND :c"},
		{name: "begins_with", expr: "id = :a AND begins_with(name, :b)"},
		{name: "parenthesized", expr: "(id = :a) AND (name <= :b)"},
		{name: "swapped operands", expr: ":a = id AND :b < name"},
		{name: "gsi", index: "by-type", expr: "type = :a AND rank >= :b"},

		{name: "range only", expr: "name = :b", wantErr: "Query condition missed key schema element"},
		{name: "non key attribute", expr: "id = :a AND #s = :b", wantErr: "Query condition missed key schema element"},
		{name: "non key attribute only", expr: "#s = :b", wantErr: "Query condition missed key schema element"},
		{name: "non key function", expr: "id = :a AND begins_with(#s, :b)", wantErr: "Query condition missed key schema element"},
		{name: "table key on gsi", index: "by-type", expr: "id = :a", wantErr: "Query condition missed key schema element"},
		{name: "table key as gsi range", index: "by-type", expr: "type = :a AND name = :b", wantErr: "Query condition missed key schema element"},
		{name: "gsi key on table", expr: "type = :a", wantErr: "Query condition missed key schema element"},
		{name: "gsi range only", index: "by-type", expr: "rank = :a", wantErr: "Query condition missed key schema element"},

		{name: "hash greater than", expr: "id > :a", wantErr: "Query key condition not supported"},
		{name: "hash between", expr: "id BETWEEN :a AND :b", wantErr: "Query key condition not supported"},
		{name: "hash begins_with", expr: "begins_with(id, :a)", wantErr: "Query key condition not supported"},
		{name: "hash not equal with range", expr: "id < :a AND name = :b", wantErr: "Query key condition not supported"},

		{name: "range twice", expr: "id = :a AND name > :b AND name < :c", wantErr: "KeyConditionExpressions must only contain one condition per key"},
		{name: "hash twice", expr: "id = :a AND id = :b", wantErr: "KeyConditionExpressions must only contain one condition per key"},
		{name: "non key twice", expr: "#s = :a AND #s = :b", wantErr: "KeyConditionExpressions must only contain one condition per key"},
		{name: "gsi range twice", index: "by-type", expr: "type = :a AND rank = :b AND rank = :c", wantErr: "KeyConditionExpressions must only contain one condition per key"},

		{name: "three keys", expr: "id = :a AND name = :b AND #s = :c", wantErr: "Conditions can be of length 1 or 2 only"},

		{name: "or", expr: "id = :a OR name = :b", wantErr: "Invalid operator used in KeyConditionExpression: OR"},
		{name: "not", expr: "id = :a AND NOT #s = :b", wantErr: "Invalid operator used in KeyConditionExpression: NOT"},
		{name: "not equal", expr: "id = :a AND name <> :b", wantErr: "Invalid operator used in KeyConditionExpression: <>"},
		{name: "contains", expr: "id = :a AND contains(name, :b)", wantErr: "Invalid operator used in KeyConditionExpression: contains"},
		{name: "in", expr: "id = :a AND name IN (:b, :c)", wantErr: "Invalid operator used in KeyConditionExpression: IN"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := require.New(t)

			table := createKeyConditionTable(t)

			_, err := table.SearchData(QueryInput{
				Index:                     tc.index,
				KeyConditionExpression:    tc.expr,
				Aliases:                   aliases,
				ExpressionAttributeValues: values,
				ScanIndexForward:          true,
			})

			if tc.wantErr == "" {
				c.NoError(err)

				return
			}

			c.Error(err)

			var apiErr types.Error
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.Code())
			c.Contains(apiErr.Message(), tc.wantErr)
		})
	}
}

func TestSearchData_keyConditionSchemaTableWithoutRangeKey(t *testing.T) {
	c := require.New(t)

	table := NewTable("hash-only")
	table.AttributesDef = map[string]string{"id": "S"}
	table.KeySchema = keySchema{HashKey: "id"}

	values := map[string]*types.Item{":a": {S: new("a")}, ":b": {S: new("b")}}

	_, err := table.SearchData(QueryInput{KeyConditionExpression: "id = :a", ExpressionAttributeValues: values})
	c.NoError(err)

	_, err = table.SearchData(QueryInput{KeyConditionExpression: "id = :a AND sk = :b", ExpressionAttributeValues: values})
	c.ErrorContains(err, "Query condition missed key schema element")
}

func TestSearchData_scanSkipsKeyConditionSchema(t *testing.T) {
	c := require.New(t)

	table := createKeyConditionTable(t)

	_, err := table.SearchData(QueryInput{Scan: true, FilterExpression: "#s = :a", Aliases: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]*types.Item{":a": {S: new("a")}}})
	c.NoError(err)
}
