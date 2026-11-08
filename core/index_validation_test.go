package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func gsiCreate(name *string, ks []*types.KeySchemaElement, proj *types.Projection) *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{Create: &types.CreateGlobalSecondaryIndexAction{
		IndexName: name, KeySchema: ks, Projection: proj,
	}}
}

func gsiDelete(name *string) *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: name}}
}

func gsiUpdate(name *string) *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{Update: &types.UpdateGlobalSecondaryIndexAction{IndexName: name}}
}

func gsiHashKey() []*types.KeySchemaElement {
	return []*types.KeySchemaElement{{AttributeName: "gk", KeyType: "HASH"}}
}

func gsiAllProjection() *types.Projection {
	return &types.Projection{ProjectionType: new("ALL")}
}

func requireValidation(t *testing.T, err error, code, message string) {
	t.Helper()

	require.Error(t, err)

	typed, ok := errors.AsType[types.Error](err)
	require.True(t, ok, "error %v is not a types.Error", err)
	require.Equal(t, code, typed.Code())
	require.Equal(t, message, typed.Message())
}

func limitDetail(changes string) string {
	return types.WithDetail(msgIndexLimit, "table", "described", "changes", changes)
}

func newTableWithExistingIndex(t *testing.T) *Table {
	t.Helper()

	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)
	_, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{
		gsiCreate(new("existing"), gsiHashKey(), gsiAllProjection()),
	})
	require.NoError(t, err)

	return table
}

func TestApplyIndexChangesRejectsMalformedEntries(t *testing.T) {
	multi := &types.GlobalSecondaryIndexUpdate{
		Update: &types.UpdateGlobalSecondaryIndexAction{IndexName: new("existing")},
		Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: new("existing")},
	}
	createAndUpdate := gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection())
	createAndUpdate.Update = &types.UpdateGlobalSecondaryIndexAction{IndexName: new("existing")}

	tests := []struct {
		name    string
		changes []*types.GlobalSecondaryIndexUpdate
		code    string
		message string
	}{
		{"DeleteWithoutIndexName", []*types.GlobalSecondaryIndexUpdate{gsiDelete(nil)}, "ValidationException", msgInvalidGSIName},
		{"DeleteEmptyIndexName", []*types.GlobalSecondaryIndexUpdate{gsiDelete(new(""))}, "ValidationException", msgInvalidGSIName},
		{"DeleteMissingIndex", []*types.GlobalSecondaryIndexUpdate{gsiDelete(new("nope"))}, "ResourceNotFoundException", types.WithDetail("Requested resource not found", "index", "nope", "table", "described")},
		{"CreateWithoutIndexName", []*types.GlobalSecondaryIndexUpdate{gsiCreate(nil, gsiHashKey(), gsiAllProjection())}, "ValidationException", msgInvalidGSIName},
		{"CreateEmptyIndexName", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new(""), gsiHashKey(), gsiAllProjection())}, "ValidationException", msgInvalidGSIName},
		{"CreateEmptyAction", []*types.GlobalSecondaryIndexUpdate{gsiCreate(nil, nil, nil)}, "ValidationException", msgInvalidGSIName},
		{"CreateWithoutProjection", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("fresh"), gsiHashKey(), nil)}, "ValidationException", types.WithDetail(msgNoProjection, "index", "fresh")},
		{"CreateEmptyProjection", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("fresh"), gsiHashKey(), &types.Projection{})}, "ValidationException", types.WithDetail(msgNoProjection, "index", "fresh")},
		{"CreateNameOnlyReportsProjectionFirst", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("fresh"), nil, nil)}, "ValidationException", types.WithDetail(msgNoProjection, "index", "fresh")},
		{"CreateWithoutKeySchema", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("fresh"), nil, gsiAllProjection())}, "ValidationException", types.WithDetail(msgNoKeySchema, "index", "fresh")},
		{"CreateEmptyKeySchema", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("fresh"), []*types.KeySchemaElement{}, gsiAllProjection())}, "ValidationException", types.WithDetail(msgNoKeySchema, "index", "fresh")},
		{"CreateKeyWithoutAttributeName", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("fresh"), []*types.KeySchemaElement{{KeyType: "HASH"}}, gsiAllProjection())}, "ValidationException", types.WithDetail(msgNoKeyAttribute, "index", "fresh")},
		{"CreateExistingName", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("existing"), gsiHashKey(), gsiAllProjection())}, "ValidationException", types.WithDetail(msgIndexExists, "index", "existing", "table", "described")},
		{"UpdateWithoutIndexName", []*types.GlobalSecondaryIndexUpdate{gsiUpdate(nil)}, "ValidationException", msgInvalidGSIName},
		{"UpdateEmptyIndexName", []*types.GlobalSecondaryIndexUpdate{gsiUpdate(new(""))}, "ValidationException", msgInvalidGSIName},
		{"UpdateMissingIndex", []*types.GlobalSecondaryIndexUpdate{gsiUpdate(new("nope"))}, "ValidationException", types.WithDetail(msgInvalidGSIName, "index", "nope", "table", "described")},
		{"NoAction", []*types.GlobalSecondaryIndexUpdate{{}}, "ValidationException", msgNullGSIAction},
		{"NilEntry", []*types.GlobalSecondaryIndexUpdate{nil}, "ValidationException", msgNullGSIAction},
		{"TwoNoActionEntries", []*types.GlobalSecondaryIndexUpdate{{}, {}}, "ValidationException", msgNullGSIAction},
		{"UpdateAndDeleteInOneEntry", []*types.GlobalSecondaryIndexUpdate{multi}, "ValidationException", types.WithDetail(msgOneUpdatePerName, "reason", "create or delete combined with update in one entry", "index", "existing")},
		{"CreateAndUpdateInOneEntry", []*types.GlobalSecondaryIndexUpdate{createAndUpdate}, "ValidationException", types.WithDetail(msgOneUpdatePerName, "reason", "create or delete combined with update in one entry", "index", "fresh")},
		{"CreateAndDeleteInOneEntry", []*types.GlobalSecondaryIndexUpdate{{
			Create: gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection()).Create,
			Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: new("existing")},
		}}, "LimitExceededException", limitDetail("2")},
		{"TwoCreates", []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection()),
			gsiCreate(new("other"), gsiHashKey(), gsiAllProjection()),
		}, "LimitExceededException", limitDetail("2")},
		{"LimitBeatsMalformedEntry", []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection()), gsiDelete(nil),
		}, "LimitExceededException", limitDetail("2")},
		{"LimitBeatsNoAction", []*types.GlobalSecondaryIndexUpdate{
			{}, gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection()), gsiCreate(new("other"), gsiHashKey(), gsiAllProjection()),
		}, "LimitExceededException", limitDetail("2")},
		{"EntriesAreCheckedInOrder", []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(nil, gsiHashKey(), gsiAllProjection()), {},
		}, "ValidationException", msgInvalidGSIName},
		{"TwoUpdatesSameIndex", []*types.GlobalSecondaryIndexUpdate{gsiUpdate(new("existing")), gsiUpdate(new("existing"))}, "ValidationException", types.WithDetail(msgOneUpdatePerName, "index", "existing")},
		{"UpdateAfterDeleteSameIndex", []*types.GlobalSecondaryIndexUpdate{gsiDelete(new("existing")), gsiUpdate(new("existing"))}, "ValidationException", types.WithDetail(msgOneUpdatePerName, "index", "existing")},
		{"UpdateAfterCreateSameIndex", []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection()), gsiUpdate(new("fresh")),
		}, "ValidationException", types.WithDetail(msgOneUpdatePerName, "index", "fresh")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newTableWithExistingIndex(t)

			desc, err := table.ApplyIndexChanges("described", tt.changes)
			requireValidation(t, err, tt.code, tt.message)
			require.NotNil(t, desc)

			// nothing was applied
			require.Len(t, table.Indexes, 1)
			require.Contains(t, table.Indexes, "existing")
			require.Len(t, desc.GlobalSecondaryIndexes, 1)
			require.Equal(t, "ACTIVE", *desc.GlobalSecondaryIndexes[0].IndexStatus)

			// the single-change entry point is as safe
			if len(tt.changes) == 1 {
				requireValidation(t, table.ApplyIndexChange(tt.changes[0]), tt.code, tt.message)
			}
		})
	}
}

func TestApplyIndexChangesIsAtomic(t *testing.T) {
	table := newTableWithExistingIndex(t)

	// the delete is valid, the update that follows it is not: the delete must not be applied
	_, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{
		gsiDelete(new("existing")), gsiUpdate(new("nope")),
	})
	requireValidation(t, err, "ValidationException", types.WithDetail(msgInvalidGSIName, "index", "nope", "table", "described"))
	require.Contains(t, table.Indexes, "existing")

	_, err = table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{
		gsiUpdate(new("nope")), gsiDelete(new("existing")),
	})
	requireValidation(t, err, "ValidationException", types.WithDetail(msgInvalidGSIName, "index", "nope", "table", "described"))
	require.Contains(t, table.Indexes, "existing")
}

func TestApplyIndexChangesUpdateExistingIndex(t *testing.T) {
	table := newTableWithExistingIndex(t)

	_, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiUpdate(new("existing"))})
	require.NoError(t, err)
	require.NoError(t, table.ApplyIndexChange(gsiUpdate(new("existing"))))
}

func TestApplyIndexChangeSingle(t *testing.T) {
	table := newTableWithExistingIndex(t)

	require.NoError(t, table.ApplyIndexChange(gsiCreate(new("fresh"), gsiHashKey(), gsiAllProjection())))
	require.Contains(t, table.Indexes, "fresh")
	require.NoError(t, table.ApplyIndexChange(gsiDelete(new("fresh"))))
	require.NotContains(t, table.Indexes, "fresh")
}

func TestAddGlobalIndexesRejectsMalformedIndexes(t *testing.T) {
	tests := []struct {
		name    string
		gsi     *types.GlobalSecondaryIndex
		message string
	}{
		{"NoName", &types.GlobalSecondaryIndex{KeySchema: gsiHashKey(), Projection: gsiAllProjection()}, msgInvalidIndexName},
		{"EmptyName", &types.GlobalSecondaryIndex{IndexName: new(""), KeySchema: gsiHashKey(), Projection: gsiAllProjection()}, msgInvalidIndexName},
		{"Empty", &types.GlobalSecondaryIndex{}, msgInvalidIndexName},
		{"NoProjection", &types.GlobalSecondaryIndex{IndexName: new("by-gk"), KeySchema: gsiHashKey()}, types.WithDetail(msgNoProjection, "index", "by-gk")},
		{"EmptyProjection", &types.GlobalSecondaryIndex{IndexName: new("by-gk"), KeySchema: gsiHashKey(), Projection: &types.Projection{}}, types.WithDetail(msgNoProjection, "index", "by-gk")},
		{"NameOnly", &types.GlobalSecondaryIndex{IndexName: new("by-gk")}, types.WithDetail(msgNoProjection, "index", "by-gk")},
		{"NoKeySchema", &types.GlobalSecondaryIndex{IndexName: new("by-gk"), Projection: gsiAllProjection()}, types.WithDetail(msgNoKeySchema, "index", "by-gk")},
		{"KeyWithoutAttributeName", &types.GlobalSecondaryIndex{
			IndexName: new("by-gk"), Projection: gsiAllProjection(), KeySchema: []*types.KeySchemaElement{{KeyType: "HASH"}},
		}, types.WithDetail(msgNoKeyAttribute, "index", "by-gk")},
		{"NilEntry", nil, msgInvalidIndexName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

			err := table.AddGlobalIndexes([]*types.GlobalSecondaryIndex{tt.gsi})
			requireValidation(t, err, "ValidationException", tt.message)
			require.Empty(t, table.Indexes)
		})
	}
}

func TestAddLocalIndexesRejectsMalformedIndexes(t *testing.T) {
	lsiKeys := []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "gk", KeyType: "RANGE"}}

	tests := []struct {
		name    string
		lsi     *types.LocalSecondaryIndex
		message string
	}{
		{"NoName", &types.LocalSecondaryIndex{KeySchema: lsiKeys, Projection: gsiAllProjection()}, msgInvalidIndexName},
		{"EmptyName", &types.LocalSecondaryIndex{IndexName: new(""), KeySchema: lsiKeys, Projection: gsiAllProjection()}, msgInvalidIndexName},
		{"Empty", &types.LocalSecondaryIndex{}, msgInvalidIndexName},
		{"NoProjection", &types.LocalSecondaryIndex{IndexName: new("by-gk"), KeySchema: lsiKeys}, types.WithDetail(msgNoProjection, "index", "by-gk")},
		{"NameOnly", &types.LocalSecondaryIndex{IndexName: new("by-gk")}, types.WithDetail(msgNoProjection, "index", "by-gk")},
		{"NoKeySchema", &types.LocalSecondaryIndex{IndexName: new("by-gk"), Projection: gsiAllProjection()}, types.WithDetail(msgNoKeySchema, "index", "by-gk")},
		{"KeyWithoutAttributeName", &types.LocalSecondaryIndex{
			IndexName: new("by-gk"), Projection: gsiAllProjection(), KeySchema: []*types.KeySchemaElement{{KeyType: "HASH"}},
		}, types.WithDetail(msgNoKeyAttribute, "index", "by-gk")},
		{"NilEntry", nil, msgInvalidIndexName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

			err := table.AddLocalIndexes([]*types.LocalSecondaryIndex{tt.lsi})
			requireValidation(t, err, "ValidationException", tt.message)
			require.Empty(t, table.Indexes)
		})
	}
}

func TestAddIndexesChecksLocalIndexesBeforeGlobalOnes(t *testing.T) {
	lsiKeys := []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "gk", KeyType: "RANGE"}}
	shapeless := func(name string) *types.GlobalSecondaryIndex {
		return &types.GlobalSecondaryIndex{IndexName: new(name)}
	}
	nameless := &types.GlobalSecondaryIndex{KeySchema: gsiHashKey(), Projection: gsiAllProjection()}
	namelessLSI := &types.LocalSecondaryIndex{KeySchema: lsiKeys, Projection: gsiAllProjection()}

	tests := []struct {
		name    string
		gsis    []*types.GlobalSecondaryIndex
		lsis    []*types.LocalSecondaryIndex
		message string
	}{
		{"LSINameBeforeGSIShape", []*types.GlobalSecondaryIndex{shapeless("by-gk")}, []*types.LocalSecondaryIndex{namelessLSI}, msgInvalidIndexName},
		{"LSIShapeBeforeGSIName", []*types.GlobalSecondaryIndex{nameless}, []*types.LocalSecondaryIndex{{IndexName: new("by-gk")}}, types.WithDetail(msgNoProjection, "index", "by-gk")},
		{
			"GSIShapeAfterLSIShape",
			[]*types.GlobalSecondaryIndex{shapeless("by-gsi")},
			[]*types.LocalSecondaryIndex{{IndexName: new("by-lsi"), KeySchema: lsiKeys}},
			types.WithDetail(msgNoProjection, "index", "by-lsi"),
		},
		{
			"LSIUndefinedAttributeBeforeGSIShape",
			[]*types.GlobalSecondaryIndex{shapeless("by-gsi")},
			[]*types.LocalSecondaryIndex{{IndexName: new("by-lsi"), Projection: gsiAllProjection(), KeySchema: []*types.KeySchemaElement{
				{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "undefined", KeyType: "RANGE"},
			}}},
			"Local Secondary Index Range Key not specified in Attribute Definitions.  Type unknown.",
		},
		{"SecondGSIName", []*types.GlobalSecondaryIndex{{IndexName: new("ok"), KeySchema: gsiHashKey(), Projection: gsiAllProjection()}, nameless}, nil, msgInvalidIndexName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

			requireValidation(t, table.AddIndexes(tt.gsis, tt.lsis), "ValidationException", tt.message)
			require.Empty(t, table.Indexes)
		})
	}
}
