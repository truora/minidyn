package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestSearchData_keyConditionValueTypes(t *testing.T) {
	table := createKeyConditionTable(t)
	aliases := map[string]string{"#n": "name"}

	cases := []struct {
		name   string
		expr   string
		values map[string]*types.Item
		bad    bool
	}{
		{"matching", "id = :a AND name = :b", map[string]*types.Item{":a": {S: new("a")}, ":b": {S: new("b")}}, false},
		{"missing placeholder", "id = :a", map[string]*types.Item{}, false},
		{"hash number", "id = :a", map[string]*types.Item{":a": {N: new("1")}}, true},
		{"range number", "id = :a AND #n > :b", map[string]*types.Item{":a": {S: new("a")}, ":b": {N: new("1")}}, true},
		{"between number", "id = :a AND #n BETWEEN :b AND :c", map[string]*types.Item{":a": {S: new("a")}, ":b": {N: new("1")}, ":c": {N: new("2")}}, true},
		{"begins_with binary", "id = :a AND begins_with(#n, :b)", map[string]*types.Item{":a": {S: new("a")}, ":b": {B: []byte("b")}}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := table.SearchData(QueryInput{
				KeyConditionExpression:    tc.expr,
				Aliases:                   aliases,
				ExpressionAttributeValues: tc.values,
				ScanIndexForward:          true,
			})

			if tc.bad {
				require.ErrorContains(t, err, "One or more parameter values were invalid: Condition parameter type does not match schema type")

				return
			}

			// A missing placeholder is reported by the evaluation, not by this check.
			if tc.name == "matching" {
				require.NoError(t, err)
			}
		})
	}
}
