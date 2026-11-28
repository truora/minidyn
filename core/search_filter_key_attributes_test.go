package core

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// A Query's FilterExpression cannot name a key attribute of the table or index it reads (#255):
// the first one in key schema order is reported. On a GSI only the index's own key counts, so
// the table's keys are plain attributes there. A Scan has no such rule. The test table
// (createProjectionTable) has the key id/name, the LSIs on id/l and the GSIs on g.
func TestSearchDataQueryFilterRejectsKeyAttributes(t *testing.T) {
	const keyErr = "Filter Expression can only contain non-primary key attributes: Primary key attribute: "

	const notProjected = "One or more parameter values were invalid: Secondary index "

	tests := []struct {
		name    string
		index   string
		filter  string
		aliases map[string]string
		wantErr string
	}{
		{name: "TableHash", filter: "id = :v", wantErr: keyErr + "id"},
		{name: "TableRange", filter: "#n = :v", wantErr: keyErr + "name"},
		{name: "TableHashFirst", filter: "#n = :v AND id = :v", wantErr: keyErr + "id"},
		{name: "TableRangeAfterPlain", filter: "a = :v AND #n = :v", wantErr: keyErr + "name"},
		{name: "TableOnce", filter: "id = :v OR id <> :v", wantErr: keyErr + "id"},
		{name: "TableFunction", filter: "attribute_exists(id)", wantErr: keyErr + "id"},
		{name: "TableSizeFunction", filter: "size(#n) > :v", wantErr: keyErr + "name"},
		{name: "TableNot", filter: "NOT (#n = :v)", wantErr: keyErr + "name"},
		{name: "TableAlias", filter: "#k = :v", aliases: map[string]string{"#k": "id"}, wantErr: keyErr + "id"},
		{name: "TableIndexKeysAreAttributes", filter: "g = :v AND l = :v"},
		{name: "TableAliasOfPlainName", filter: "#k = :v", aliases: map[string]string{"#k": "a"}},
		{name: "TableLiteralHashSign", filter: "#id = :v", aliases: map[string]string{"#id": "a"}},
		{name: "LSIHash", index: "lko", filter: "id = :v", wantErr: keyErr + "id"},
		{name: "LSIRange", index: "linc", filter: "l = :v", wantErr: keyErr + "l"},
		{name: "LSIHashFirst", index: "lko", filter: "l = :v AND id = :v", wantErr: keyErr + "id"},
		{name: "LSITableRangeIsAttribute", index: "lko", filter: "#n = :v"},
		{name: "LSIKeyBeforeNotProjected", index: "lko", filter: "g = :v AND l = :v", wantErr: keyErr + "l"},
		{name: "LSIAlias", index: "lko", filter: "#k = :v", aliases: map[string]string{"#k": "l"}, wantErr: keyErr + "l"},
		{name: "GSIHash", index: "gko", filter: "g = :v", wantErr: keyErr + "g"},
		{name: "GSIIncludeHash", index: "ginc", filter: "attribute_not_exists(g)", wantErr: keyErr + "g"},
		{name: "GSIKeyBeforeNotProjected", index: "gko", filter: "l = :v AND g = :v", wantErr: keyErr + "g"},
		{name: "GSITableHashIsAttribute", index: "gko", filter: "id = :v"},
		{name: "GSITableRangeIsAttribute", index: "ginc", filter: "#n = :v"},
		{name: "GSINotProjectedStillReported", index: "gko", filter: "l = :v", wantErr: notProjected + "gko does not project one or more filter attributes: [l]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, withItem := range []bool{true, false} {
				table := createProjectionTable(t, withItem)

				input := projectionQuery(tt.index)
				input.FilterExpression = tt.filter
				// name is a reserved word, so the filters spell it #n.
				input.Aliases = map[string]string{"#n": "name"}
				maps.Copy(input.Aliases, tt.aliases)
				input.ExpressionAttributeValues[":v"] = &types.Item{S: new("x")}

				_, err := table.SearchData(input)

				if tt.wantErr == "" {
					require.NoError(t, err)

					continue
				}

				require.Error(t, err)
				require.Contains(t, err.Error(), "ValidationException")
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}

	t.Run("ErrorCarriesIndexDetail", func(t *testing.T) {
		table := createProjectionTable(t, false)
		input := projectionQuery("lko")
		input.FilterExpression = "l = :v"
		input.ExpressionAttributeValues[":v"] = &types.Item{S: new("x")}

		_, err := table.SearchData(input)
		require.Error(t, err)
		require.Contains(t, err.Error(), keyErr+"l | index: lko; attribute: l")
	})

	t.Run("ScanHasNoSuchRule", func(t *testing.T) {
		table := createProjectionTable(t, true)

		for _, index := range []string{"", "lko", "linc", "gko", "ginc"} {
			input := projectionScan(index)
			input.FilterExpression = "id = :v AND #n = :v AND g = :v AND l = :v"
			input.Aliases = map[string]string{"#n": "name"}
			input.ExpressionAttributeValues = map[string]*types.Item{":v": {S: new("x")}}

			_, err := table.SearchData(input)
			require.NoError(t, err, index)
		}
	})
}

// The key check comes before Select, the "does not project" check and the projection of a GSI,
// and after the key condition and the start key.
func TestSearchDataQueryFilterKeyAttributesOrder(t *testing.T) {
	table := createProjectionTable(t, true)

	query := func(index, filter string) QueryInput {
		input := projectionQuery(index)
		input.FilterExpression = filter
		input.ExpressionAttributeValues[":v"] = &types.Item{S: new("x")}

		return input
	}

	const keyErr = "Filter Expression can only contain non-primary key attributes: Primary key attribute: "

	t.Run("BeforeSelect", func(t *testing.T) {
		input := query("lko", "l = :v")
		input.Select = "BOGUS"

		_, err := table.SearchData(input)
		require.ErrorContains(t, err, keyErr+"l")
	})

	t.Run("BeforeGSIProjection", func(t *testing.T) {
		input := query("gko", "g = :v")
		input.ProjectionExpression = "b"

		_, err := table.SearchData(input)
		require.ErrorContains(t, err, keyErr+"g")
	})

	t.Run("AfterKeyCondition", func(t *testing.T) {
		input := query("lko", "l = :v")
		input.KeyConditionExpression = "zz = :k"

		_, err := table.SearchData(input)
		require.ErrorContains(t, err, "Query condition missed key schema element")
	})

	t.Run("AfterStartKey", func(t *testing.T) {
		input := query("", "#n = :v")
		input.Aliases = map[string]string{"#n": "name"}
		input.ExclusiveStartKey = map[string]*types.Item{"id": {S: new("p")}}

		_, err := table.SearchData(input)
		require.Error(t, err)
		require.NotContains(t, err.Error(), keyErr)
	})
}
