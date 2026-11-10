package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func createSelectTable(t *testing.T) *Table {
	t.Helper()

	table, err := createPokemonTable()
	require.NoError(t, err)

	keysOnly := "KEYS_ONLY"
	require.NoError(t, table.AddGlobalIndexes([]*types.GlobalSecondaryIndex{{
		IndexName: new("keys"),
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
			{AttributeName: "name", KeyType: "RANGE"},
		},
		Projection:            &types.Projection{ProjectionType: &keysOnly},
		ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
	}}))

	_, err = table.Put(&types.PutItemInput{Item: createPokemon(pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}), TableName: &table.Name})
	require.NoError(t, err)

	return table
}

func TestSearchDataSelectValidation(t *testing.T) {
	table := createSelectTable(t)

	const (
		invalidSelect = "Select must be ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT, or SPECIFIC_ATTRIBUTES"
		specific      = "Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"
		onlyIndex     = "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
		cannotProject = "Cannot specify the ProjectionExpression when choosing to get "
		gsiNotAll     = "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index keys because its projection type is not ALL"
	)

	tests := []struct {
		name       string
		index      string
		sel        string
		projection string
		wantErr    string
	}{
		{name: "EmptyAccepted", sel: ""},
		{name: "AllAttributesAccepted", sel: "ALL_ATTRIBUTES"},
		{name: "CountAccepted", sel: "COUNT"},
		{name: "SpecificWithProjection", sel: "SPECIFIC_ATTRIBUTES", projection: "id"},
		{name: "EmptyWithProjection", sel: "", projection: "id"},
		{name: "AllProjectedOnIndex", index: "keys", sel: "ALL_PROJECTED_ATTRIBUTES"},
		{name: "AllAttributesOnAllIndex", index: "invert", sel: "ALL_ATTRIBUTES"},
		{name: "Unknown", sel: "BOGUS", wantErr: invalidSelect},
		{name: "UnknownWithProjection", sel: "BOGUS", projection: "id", wantErr: invalidSelect},
		{name: "SpecificWithoutProjection", sel: "SPECIFIC_ATTRIBUTES", wantErr: specific},
		{name: "AllProjectedOnTable", sel: "ALL_PROJECTED_ATTRIBUTES", wantErr: onlyIndex},
		{name: "CountWithProjection", sel: "COUNT", projection: "id", wantErr: cannotProject + "only the Count"},
		{name: "AllAttributesWithProjection", sel: "ALL_ATTRIBUTES", projection: "id", wantErr: cannotProject + "ALL_ATTRIBUTES"},
		{name: "AllProjectedWithProjection", sel: "ALL_PROJECTED_ATTRIBUTES", projection: "id", wantErr: cannotProject + "ALL_PROJECTED_ATTRIBUTES"},
		{
			name: "AllProjectedWithProjectionOnIndex", index: "keys", sel: "ALL_PROJECTED_ATTRIBUTES", projection: "id",
			wantErr: cannotProject + "ALL_PROJECTED_ATTRIBUTES",
		},
		{name: "AllAttributesOnKeysOnlyGSI", index: "keys", sel: "ALL_ATTRIBUTES", wantErr: gsiNotAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := table.SearchData(QueryInput{
				Index:                tt.index,
				Select:               tt.sel,
				ProjectionExpression: tt.projection,
				Scan:                 true,
				ScanIndexForward:     true,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Len(t, res.Items, 1)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), "ValidationException")
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestSearchDataSelectAllProjectedReturnsProjection(t *testing.T) {
	table := createSelectTable(t)

	res, err := table.SearchData(QueryInput{Index: "keys", Select: "ALL_PROJECTED_ATTRIBUTES", Scan: true, ScanIndexForward: true})
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	require.Len(t, res.Items[0], 2)
}

// DynamoDB reports a key condition error before an invalid Select.
func TestSearchDataKeyConditionErrorsBeforeInvalidSelect(t *testing.T) {
	table := createSelectTable(t)

	const onlyIndex = "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"

	str := func(s string) *types.Item { return &types.Item{S: &s} }
	num := func(s string) *types.Item { return &types.Item{N: &s} }

	tests := []struct {
		name      string
		condition string
		values    map[string]*types.Item
		wantErr   string
	}{
		{name: "EmptyHash", condition: "id = :id", values: map[string]*types.Item{":id": str("")}, wantErr: "empty string value"},
		{name: "TypeMismatch", condition: "id = :id", values: map[string]*types.Item{":id": num("1")}, wantErr: errKeyConditionType},
		{
			name: "BetweenReversed", condition: "id = :id AND #n BETWEEN :a AND :b",
			values:  map[string]*types.Item{":id": str("001"), ":a": str("z"), ":b": str("a")},
			wantErr: "The BETWEEN operator requires upper bound to be greater than or equal to lower bound",
		},
		{name: "ValidKeyReportsSelect", condition: "id = :id", values: map[string]*types.Item{":id": str("001")}, wantErr: onlyIndex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := table.SearchData(QueryInput{
				Select:                    "ALL_PROJECTED_ATTRIBUTES",
				KeyConditionExpression:    tt.condition,
				Aliases:                   map[string]string{"#n": "name"},
				ExpressionAttributeValues: tt.values,
				ScanIndexForward:          true,
			})

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)

			if tt.wantErr != onlyIndex {
				require.NotContains(t, err.Error(), onlyIndex)
			}
		})
	}
}
