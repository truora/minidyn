package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// A TransactWriteItems ConditionCheck takes its values from the request like any other
// condition, so a nil value must be rejected before the interpreter sees it (#268).
func TestMatchConditionCheck_rejectsNilValues(t *testing.T) {
	table, err := createPokemonTable()
	require.NoError(t, err)

	stored := map[string]*types.Item{"id": {S: new("a")}}

	cases := []struct {
		name   string
		values map[string]*types.Item
		want   string
	}{
		{"top-level nil", map[string]*types.Item{":v": nil}, "The supplied Item contains a null AttributeValue: :v is null"},
		{"nested nil", map[string]*types.Item{":v": {L: []*types.Item{nil}}}, "The supplied Item contains a null AttributeValue"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, mErr := table.MatchConditionCheck("x = :v", nil, tc.values, stored)
			require.ErrorContains(t, mErr, tc.want)
		})
	}

	matched, err := table.MatchConditionCheck("id = :v", nil, map[string]*types.Item{":v": {S: new("a")}}, stored)
	require.NoError(t, err)
	require.True(t, matched)
}
