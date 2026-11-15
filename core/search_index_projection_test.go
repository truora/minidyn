package core

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// createProjectionTable has the key id/name, the LSIs lko (KEYS_ONLY) and linc (INCLUDE a) on
// id/l, and the GSIs gko (KEYS_ONLY) and ginc (INCLUDE a) on g. Its only item has the attributes
// a and b too, and b is never projected.
func createProjectionTable(t *testing.T, withItem bool) *Table {
	t.Helper()

	table := NewTable("proj")
	table.AttributesDef = map[string]string{"id": "S", "name": "S", "g": "S", "l": "S"}
	table.KeySchema = keySchema{"id", "name", false}

	ko := &types.Projection{ProjectionType: new("KEYS_ONLY")}
	inc := &types.Projection{ProjectionType: new("INCLUDE"), NonKeyAttributes: []*string{new("a")}}
	lsiKeys := []*types.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}, {AttributeName: "l", KeyType: "RANGE"}}
	gsiKeys := []*types.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}}
	throughput := &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1}

	require.NoError(t, table.AddIndexes(
		[]*types.GlobalSecondaryIndex{
			{IndexName: new("gko"), KeySchema: gsiKeys, Projection: ko, ProvisionedThroughput: throughput},
			{IndexName: new("ginc"), KeySchema: gsiKeys, Projection: inc, ProvisionedThroughput: throughput},
		},
		[]*types.LocalSecondaryIndex{
			{IndexName: new("lko"), KeySchema: lsiKeys, Projection: ko},
			{IndexName: new("linc"), KeySchema: lsiKeys, Projection: inc},
		},
	))

	if withItem {
		s := func(v string) *types.Item { return &types.Item{S: &v} }

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: map[string]*types.Item{
			"id": s("p"), "name": s("n"), "g": s("g"), "l": s("l"), "a": s("a"), "b": s("b"),
		}})
		require.NoError(t, err)
	}

	return table
}

func projectionNames(items []map[string]*types.Item) []string {
	names := []string{}

	for _, item := range items {
		for k := range item {
			names = append(names, k)
		}
	}

	sort.Strings(names)

	return names
}

func projectionQuery(index string) QueryInput {
	key := "id"
	val := "p"

	if index == "gko" || index == "ginc" {
		key, val = "g", "g"
	}

	return QueryInput{
		Index:                     index,
		KeyConditionExpression:    key + " = :k",
		ExpressionAttributeValues: map[string]*types.Item{":k": {S: &val}},
		ScanIndexForward:          true,
	}
}

func projectionScan(index string) QueryInput {
	return QueryInput{Index: index, Scan: true, ScanIndexForward: true}
}

// An LSI reads the attributes it does not project from the table (#202).
func TestSearchDataLSIFetchesFromTable(t *testing.T) {
	table := createProjectionTable(t, true)

	all := []string{"a", "b", "g", "id", "l", "name"}

	tests := []struct {
		name       string
		index      string
		sel        string
		projection string
		want       []string
	}{
		{name: "KeysOnlyAllAttributes", index: "lko", sel: "ALL_ATTRIBUTES", want: all},
		{name: "IncludeAllAttributes", index: "linc", sel: "ALL_ATTRIBUTES", want: all},
		{name: "KeysOnlySpecificNonProjected", index: "lko", sel: "SPECIFIC_ATTRIBUTES", projection: "b", want: []string{"b"}},
		{name: "KeysOnlyProjectionNonProjected", index: "lko", projection: "a, b", want: []string{"a", "b"}},
		{name: "IncludeProjectionNonProjected", index: "linc", projection: "b", want: []string{"b"}},
		{name: "IncludeProjectionProjected", index: "linc", projection: "a", want: []string{"a"}},
		{name: "KeysOnlyProjectionTableKey", index: "lko", projection: "g, l", want: []string{"g", "l"}},
		{name: "KeysOnlyDefault", index: "lko", want: []string{"id", "l", "name"}},
		{name: "IncludeDefault", index: "linc", want: []string{"a", "id", "l", "name"}},
		{name: "KeysOnlyAllProjected", index: "lko", sel: "ALL_PROJECTED_ATTRIBUTES", want: []string{"id", "l", "name"}},
		{name: "IncludeAllProjected", index: "linc", sel: "ALL_PROJECTED_ATTRIBUTES", want: []string{"a", "id", "l", "name"}},
	}

	for _, tt := range tests {
		for op, build := range map[string]func(string) QueryInput{"Query": projectionQuery, "Scan": projectionScan} {
			t.Run(op+tt.name, func(t *testing.T) {
				input := build(tt.index)
				input.Select = tt.sel
				input.ProjectionExpression = tt.projection

				res, err := table.SearchData(input)
				require.NoError(t, err)
				require.Equal(t, tt.want, projectionNames(res.Items))
			})
		}
	}
}

// Fetching from the table must not change the stored item.
func TestSearchDataLSIFetchReturnsCopy(t *testing.T) {
	table := createProjectionTable(t, true)

	input := projectionQuery("lko")
	input.Select = "ALL_ATTRIBUTES"

	res, err := table.SearchData(input)
	require.NoError(t, err)
	require.Len(t, res.Items, 1)

	delete(res.Items[0], "b")

	res, err = table.SearchData(input)
	require.NoError(t, err)
	require.Contains(t, res.Items[0], "b")

	// A projected result is built from the stored item and does not alias it either.
	input.Select = ""
	input.ProjectionExpression = "a, b"

	res, err = table.SearchData(input)
	require.NoError(t, err)
	require.Len(t, res.Items, 1)

	delete(res.Items[0], "b")

	res, err = table.SearchData(input)
	require.NoError(t, err)
	require.Contains(t, res.Items[0], "b")
}

// A Query's FilterExpression can only name attributes the index projects, on an LSI and a GSI. A
// Scan's filter reads the stored item, so it can name any attribute.
func TestSearchDataQueryFilterMustBeProjected(t *testing.T) {
	table := createProjectionTable(t, true)

	b := "b"

	const prefix = "One or more parameter values were invalid: Secondary index "

	tests := []struct {
		name    string
		index   string
		filter  string
		wantErr string
	}{
		{name: "LSIKeysOnly", index: "lko", filter: "b = :b", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
		{name: "LSIIncludeNonProjected", index: "linc", filter: "b = :b", wantErr: prefix + "linc does not project one or more filter attributes: [b]"},
		{name: "LSIIncludeProjected", index: "linc", filter: "a = :b"},
		{name: "GSIKeysOnly", index: "gko", filter: "b = :b", wantErr: prefix + "gko does not project one or more filter attributes: [b]"},
		{name: "GSIIncludeNonProjected", index: "ginc", filter: "b = :b", wantErr: prefix + "ginc does not project one or more filter attributes: [b]"},
		{name: "GSIIncludeProjected", index: "ginc", filter: "a = :b"},
		{name: "Sorted", index: "lko", filter: "b = :b AND a = :b", wantErr: prefix + "lko does not project one or more filter attributes: [a, b]"},
		{name: "SortedReverse", index: "lko", filter: "a = :b AND b = :b", wantErr: prefix + "lko does not project one or more filter attributes: [a, b]"},
		{name: "Once", index: "lko", filter: "b = :b OR b <> :b", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
		{name: "NestedPath", index: "lko", filter: "b.x = :b", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
		{name: "ListPath", index: "lko", filter: "b[0] = :b", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
		{name: "Function", index: "lko", filter: "attribute_exists(b)", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
		{name: "SizeFunction", index: "lko", filter: "size(b) > :b", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
		{name: "Alias", index: "lko", filter: "#x = :b", wantErr: prefix + "lko does not project one or more filter attributes: [b]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := projectionQuery(tt.index)
			input.FilterExpression = tt.filter

			// Unused placeholders are rejected by the front ends, so pass only what is used.
			if strings.Contains(tt.filter, ":b") {
				input.ExpressionAttributeValues[":b"] = &types.Item{S: &b}
			}

			if strings.Contains(tt.filter, "#x") {
				input.Aliases = map[string]string{"#x": "b"}
			}

			_, err := table.SearchData(input)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), "ValidationException")
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}

	t.Run("ScanReadsStoredItem", func(t *testing.T) {
		for _, index := range []string{"lko", "linc", "gko", "ginc"} {
			input := projectionScan(index)
			input.FilterExpression = "b = :b"
			input.ExpressionAttributeValues = map[string]*types.Item{":b": {S: &b}}

			res, err := table.SearchData(input)
			require.NoError(t, err, index)
			require.Len(t, res.Items, 1, index)
			require.NotContains(t, res.Items[0], "b", index)
		}
	})

	t.Run("TableQueryFilterUnrestricted", func(t *testing.T) {
		input := QueryInput{
			KeyConditionExpression:    "id = :k",
			FilterExpression:          "b = :b",
			ExpressionAttributeValues: map[string]*types.Item{":k": {S: new("p")}, ":b": {S: &b}},
			ScanIndexForward:          true,
		}

		res, err := table.SearchData(input)
		require.NoError(t, err)
		require.Len(t, res.Items, 1)
	})
}

// The filter check is a request check: it fails on an empty table, after the placeholders and the
// key condition, and before Select.
func TestSearchDataQueryFilterProjectionOrder(t *testing.T) {
	const notProjected = "does not project one or more filter attributes: [b]"

	b := "b"
	k := "p"
	vals := map[string]*types.Item{":k": {S: &k}, ":b": {S: &b}}

	tests := []struct {
		name    string
		mutate  func(*QueryInput)
		wantErr string
	}{
		{name: "Plain", mutate: func(*QueryInput) {}, wantErr: notProjected},
		{name: "BeforeInvalidSelect", mutate: func(in *QueryInput) { in.Select = "BOGUS" }, wantErr: notProjected},
		{name: "BeforeSpecificWithoutProjection", mutate: func(in *QueryInput) { in.Select = "SPECIFIC_ATTRIBUTES" }, wantErr: notProjected},
		{name: "BeforeCountWithProjection", mutate: func(in *QueryInput) { in.Select = "COUNT"; in.ProjectionExpression = "a" }, wantErr: notProjected},
		{
			name: "AfterKeyCondition", mutate: func(in *QueryInput) { in.KeyConditionExpression = "zz = :k" },
			wantErr: "Query condition missed key schema element",
		},
		{
			name: "AfterUndeclaredProjectionName", mutate: func(in *QueryInput) { in.ProjectionExpression = "#u" },
			wantErr: "An expression attribute name used in the document path is not defined; attribute name: #u",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, withItem := range []bool{true, false} {
				table := createProjectionTable(t, withItem)

				input := projectionQuery("lko")
				input.FilterExpression = "b = :b"
				input.ExpressionAttributeValues = vals
				tt.mutate(&input)

				_, err := table.SearchData(input)
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
