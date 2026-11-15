package core

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestSetAttributeDefinitionRejectsMalformedEntries(t *testing.T) {
	valid := &types.AttributeDefinition{AttributeName: new("ok"), AttributeType: new("S")}

	tests := []struct {
		name    string
		attrs   []*types.AttributeDefinition
		message string
	}{
		{"NilEntry", []*types.AttributeDefinition{nil}, types.WithDetail(msgNoKeyAttribute, "table", "described")},
		{"Empty", []*types.AttributeDefinition{{}}, types.WithDetail(msgNoKeyAttribute, "table", "described")},
		{"EmptyName", []*types.AttributeDefinition{{AttributeName: new(""), AttributeType: new("S")}}, types.WithDetail(msgEmptyAttributeName, "table", "described")},
		{"NoName", []*types.AttributeDefinition{{AttributeType: new("S")}}, types.WithDetail(msgNoKeyAttribute, "table", "described")},
		{"NoType", []*types.AttributeDefinition{{AttributeName: new("id")}}, types.WithDetail(msgNoAttributeType, "attribute", "id", "table", "described")},
		{"EmptyType", []*types.AttributeDefinition{{AttributeName: new("id"), AttributeType: new("")}}, types.WithDetail(msgNoAttributeType, "attribute", "id", "table", "described")},
		{"NameBeforeType", []*types.AttributeDefinition{{}, {AttributeName: new("id")}}, types.WithDetail(msgNoKeyAttribute, "table", "described")},
		{"LaterEntry", []*types.AttributeDefinition{valid, {}}, types.WithDetail(msgNoKeyAttribute, "table", "described")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := NewTable("described")

			requireValidation(t, table.SetAttributeDefinition(tt.attrs), "ValidationException", tt.message)
			// none of the entries is kept, not even the valid one before the bad one
			require.Empty(t, table.AttributesDef)
		})
	}
}

func TestUpdateTableKeepsAttributeDefinitionsWhenRejected(t *testing.T) {
	table := newTableWithExistingIndex(t)
	before := len(table.AttributesDef)

	desc, err := table.UpdateTable("described",
		[]*types.AttributeDefinition{{AttributeName: new("extra"), AttributeType: new("S")}},
		[]*types.GlobalSecondaryIndexUpdate{gsiDelete(nil)})
	requireValidation(t, err, "ValidationException", msgInvalidGSIName)
	require.NotNil(t, desc)
	require.Len(t, table.AttributesDef, before)
	require.NotContains(t, table.AttributesDef, "extra")

	// a bad attribute definition stops the request before any index change
	_, err = table.UpdateTable("described",
		[]*types.AttributeDefinition{{}},
		[]*types.GlobalSecondaryIndexUpdate{gsiDelete(new("existing"))})
	requireValidation(t, err, "ValidationException", types.WithDetail(msgNoKeyAttribute, "table", "described"))
	require.Contains(t, table.Indexes, "existing")
}

func TestUpdateTableAppliesAttributeDefinitionsAndChanges(t *testing.T) {
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

	_, err := table.UpdateTable("described",
		[]*types.AttributeDefinition{{AttributeName: new("other"), AttributeType: new("S")}},
		[]*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("by-other"), []*types.KeySchemaElement{{AttributeName: "other", KeyType: "HASH"}}, gsiAllProjection()),
		})
	require.NoError(t, err)
	require.Contains(t, table.Indexes, "by-other")
	require.Equal(t, "S", table.AttributesDef["other"])
}

func TestUpdateTableChecksIndexChangesBeforeAttributeDefinitions(t *testing.T) {
	badAttrs := []*types.AttributeDefinition{{}}
	missingAttr := []*types.KeySchemaElement{{AttributeName: "undefined", KeyType: "HASH"}}

	tests := []struct {
		name    string
		changes []*types.GlobalSecondaryIndexUpdate
		code    string
		message string
	}{
		{"MalformedDelete", []*types.GlobalSecondaryIndexUpdate{gsiDelete(nil)}, "ValidationException", msgInvalidGSIName},
		{"TwoCreates", []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("a"), gsiHashKey(), gsiAllProjection()), gsiCreate(new("b"), gsiHashKey(), gsiAllProjection()),
		}, "LimitExceededException", limitDetail("2")},
		{"NoAction", []*types.GlobalSecondaryIndexUpdate{{}}, "ValidationException", msgNullGSIAction},
		{
			"DeleteMissing",
			[]*types.GlobalSecondaryIndexUpdate{gsiDelete(new("missing"))},
			"ResourceNotFoundException",
			types.WithDetail("Requested resource not found", "index", "missing", "table", "described"),
		},
		// what depends on the attribute definitions is checked after the structure of every entry
		{"UndefinedAttributeBeforeNoAction", []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("a"), missingAttr, gsiAllProjection()), {},
		}, "ValidationException", msgNullGSIAction},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newDescribedTable(t, "PAY_PER_REQUEST", nil)
			before := maps.Clone(table.AttributesDef)

			_, err := table.UpdateTable("described", badAttrs, tt.changes)
			requireValidation(t, err, tt.code, tt.message)
			require.Equal(t, before, table.AttributesDef)
		})
	}
}

func TestUpdateTableChecksIndexAttributesAfterAttributeDefinitions(t *testing.T) {
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)
	before := maps.Clone(table.AttributesDef)

	_, err := table.UpdateTable("described",
		[]*types.AttributeDefinition{{AttributeName: new("extra"), AttributeType: new("S")}},
		[]*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("a"), []*types.KeySchemaElement{{AttributeName: "undefined", KeyType: "HASH"}}, gsiAllProjection()),
		})
	require.Error(t, err)
	require.Equal(t, before, table.AttributesDef)
	require.Empty(t, table.Indexes)
}
