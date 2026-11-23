package core

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// An index created by UpdateTable holds the items the table already has (#271). The ones whose
// index key violates the schema are left out, as the developer guide describes ("Phases of
// index creation").

const backfillIndex = "by-gk"

func backfillItem(pk string, gk *types.Item) map[string]*types.Item {
	item := map[string]*types.Item{"pk": {S: new(pk)}, "extra": {S: new("x-" + pk)}}
	if gk != nil {
		item["gk"] = gk
	}

	return item
}

// newBackfillTable returns a table keyed by pk (S) holding one item per entry of gks, a nil
// entry being an item without gk, and no index. Items are named i0, i1, ...
func newBackfillTable(t *testing.T, gks ...*types.Item) *Table {
	t.Helper()

	table := NewTable("described")
	table.BillingMode = new("PAY_PER_REQUEST")
	require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{
		{AttributeName: new("pk"), AttributeType: new("S")},
	}))
	require.NoError(t, table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema: []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}))

	for n, gk := range gks {
		putBackfill(t, table, backfillItem("i"+string(rune('0'+n)), gk))
	}

	return table
}

func putBackfill(t *testing.T, table *Table, item map[string]*types.Item) {
	t.Helper()

	_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: item})
	require.NoError(t, err)
}

func createBackfillIndex(table *Table, projection *types.Projection) error {
	_, err := table.UpdateTable(table.Name,
		[]*types.AttributeDefinition{{AttributeName: new("gk"), AttributeType: new("S")}},
		[]*types.GlobalSecondaryIndexUpdate{gsiCreate(new(backfillIndex), gsiHashKey(), projection)})

	return err
}

// scanBackfillIndex returns the pk of every item a Scan of the index returns, sorted.
func scanBackfillIndex(t *testing.T, table *Table) []string {
	t.Helper()

	res, err := table.SearchData(QueryInput{Index: backfillIndex, Scan: true, ScanIndexForward: true})
	require.NoError(t, err)

	return backfillPKs(res.Items)
}

func backfillPKs(items []map[string]*types.Item) []string {
	pks := make([]string, 0, len(items))
	for _, item := range items {
		pks = append(pks, *item["pk"].S)
	}

	slices.Sort(pks)

	return pks
}

func queryBackfillIndex(t *testing.T, table *Table, gk string) []string {
	t.Helper()

	res, err := table.SearchData(QueryInput{
		Index:                     backfillIndex,
		KeyConditionExpression:    "gk = :gk",
		ExpressionAttributeValues: map[string]*types.Item{":gk": {S: new(gk)}},
		ScanIndexForward:          true,
	})
	require.NoError(t, err)

	return backfillPKs(res.Items)
}

func TestUpdateTableCreateIndexIndexesExistingItems(t *testing.T) {
	table := newBackfillTable(t, &types.Item{S: new("a")}, &types.Item{S: new("b")}, &types.Item{S: new("a")})

	require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

	require.Equal(t, []string{"i0", "i1", "i2"}, scanBackfillIndex(t, table))
	require.Equal(t, []string{"i0", "i2"}, queryBackfillIndex(t, table, "a"))
	require.Equal(t, []string{"i1"}, queryBackfillIndex(t, table, "b"))

	gsi, _ := table.IndexesDescription()
	require.Len(t, gsi, 1)
	require.Equal(t, int64(3), gsi[0].ItemCount)
}

func TestUpdateTableCreateIndexLeavesOutItemsWithoutKey(t *testing.T) {
	table := newBackfillTable(t, &types.Item{S: new("a")}, nil)

	require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

	require.Equal(t, []string{"i0"}, scanBackfillIndex(t, table))
	require.Equal(t, int64(1), table.Indexes[backfillIndex].count())
}

func TestUpdateTableCreateIndexSkipsKeyViolations(t *testing.T) {
	table := newBackfillTable(t,
		&types.Item{S: new("a")},
		&types.Item{S: new("")},
		&types.Item{N: new("5")},
		&types.Item{B: []byte{}},
		&types.Item{BOOL: new(true)},
	)

	require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

	require.Equal(t, []string{"i0"}, scanBackfillIndex(t, table))
	require.Equal(t, int64(1), table.Indexes[backfillIndex].count())
	// the violating items stay in the table
	require.Len(t, table.Data, 5)
}

func TestUpdateTableCreateIndexProjectsExistingItems(t *testing.T) {
	tests := []struct {
		name       string
		projection *types.Projection
		want       []string
	}{
		{"KeysOnly", &types.Projection{ProjectionType: new("KEYS_ONLY")}, []string{"gk", "pk"}},
		{"Include", &types.Projection{ProjectionType: new("INCLUDE"), NonKeyAttributes: []*string{new("extra")}}, []string{"extra", "gk", "pk"}},
		{"All", gsiAllProjection(), []string{"extra", "gk", "pk"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := newBackfillTable(t, &types.Item{S: new("a")})

			require.NoError(t, createBackfillIndex(table, tt.projection))

			res, err := table.SearchData(QueryInput{Index: backfillIndex, Scan: true, ScanIndexForward: true})
			require.NoError(t, err)
			require.Len(t, res.Items, 1)

			var attrs []string
			for name := range res.Items[0] {
				attrs = append(attrs, name)
			}

			slices.Sort(attrs)
			require.Equal(t, tt.want, attrs)
		})
	}
}

func TestUpdateTableCreateIndexBackfillsNothingWhenRejected(t *testing.T) {
	table := newBackfillTable(t, &types.Item{S: new("a")})
	previous := maps.Clone(table.AttributesDef)
	data := maps.Clone(table.Data)

	// the definitions are set, then the index is rejected: gk, its key attribute, is not defined
	_, err := table.UpdateTable(table.Name,
		[]*types.AttributeDefinition{{AttributeName: new("other"), AttributeType: new("S")}},
		[]*types.GlobalSecondaryIndexUpdate{gsiCreate(new(backfillIndex), gsiHashKey(), gsiAllProjection())})
	requireValidation(t, err, "ValidationException",
		"Global Secondary Index Hash Key not specified in Attribute Definitions.  Type unknown.")
	require.Empty(t, table.Indexes)
	require.Equal(t, previous, table.AttributesDef)
	require.Equal(t, data, table.Data)
}

func TestIndexBackfillReportsUnexpectedKeyErrors(t *testing.T) {
	table := newBackfillTable(t, &types.Item{S: new("a")}, nil)
	table.AttributesDef["gk"] = "S"

	i, err := buildGSI(table, &types.GlobalSecondaryIndex{
		IndexName: new(backfillIndex), KeySchema: gsiHashKey(), Projection: gsiAllProjection(),
	})
	require.NoError(t, err)

	// a primary schema is not sparse: the missing gk of i1 is an error GetKey never gives an
	// index, the case backfill reports instead of skipping
	i.keySchema.Secondary = false

	require.ErrorIs(t, i.backfill(), errMissingField)
}

func TestKeyViolatingItemsCanBeWrittenAfterBackfill(t *testing.T) {
	violations := map[string]*types.Item{"empty": {S: new("")}, "wrong type": {N: new("5")}}

	for name, gk := range violations {
		t.Run("Delete "+name, func(t *testing.T) {
			table := newBackfillTable(t, &types.Item{S: new("a")}, gk)
			require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

			_, err := table.Delete(&types.DeleteItemInput{
				TableName: &table.Name, Key: map[string]*types.Item{"pk": {S: new("i1")}},
			})
			require.NoError(t, err)
			require.NotContains(t, table.Data, "i1")
			require.Equal(t, []string{"i0"}, scanBackfillIndex(t, table))
			require.Equal(t, int64(1), table.Indexes[backfillIndex].count())
		})

		t.Run("Overwrite "+name, func(t *testing.T) {
			table := newBackfillTable(t, &types.Item{S: new("a")}, gk)
			require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

			putBackfill(t, table, backfillItem("i1", &types.Item{S: new("a")}))

			require.Equal(t, []string{"i0", "i1"}, queryBackfillIndex(t, table, "a"))
			require.Equal(t, int64(2), table.Indexes[backfillIndex].count())
		})

		t.Run("Update "+name, func(t *testing.T) {
			table := newBackfillTable(t, &types.Item{S: new("a")}, gk)
			require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

			_, err := table.Update(&types.UpdateItemInput{
				TableName:                 &table.Name,
				Key:                       map[string]*types.Item{"pk": {S: new("i1")}},
				UpdateExpression:          "SET gk = :gk",
				ExpressionAttributeValues: map[string]*types.Item{":gk": {S: new("b")}},
			})
			require.NoError(t, err)
			require.Equal(t, []string{"i1"}, queryBackfillIndex(t, table, "b"))
			require.Equal(t, int64(2), table.Indexes[backfillIndex].count())
		})

		t.Run("Update other attribute "+name, func(t *testing.T) {
			table := newBackfillTable(t, &types.Item{S: new("a")}, gk)
			require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

			before := maps.Clone(table.Data["i1"])

			_, err := table.Update(&types.UpdateItemInput{
				TableName:                 &table.Name,
				Key:                       map[string]*types.Item{"pk": {S: new("i1")}},
				UpdateExpression:          "SET extra = :x",
				ExpressionAttributeValues: map[string]*types.Item{":x": {S: new("changed")}},
			})
			require.Error(t, err)
			require.Equal(t, before, table.Data["i1"])
			require.Equal(t, int64(1), table.Indexes[backfillIndex].count())
		})

		t.Run("Remove attribute "+name, func(t *testing.T) {
			table := newBackfillTable(t, &types.Item{S: new("a")}, gk)
			require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

			_, err := table.Update(&types.UpdateItemInput{
				TableName:        &table.Name,
				Key:              map[string]*types.Item{"pk": {S: new("i1")}},
				UpdateExpression: "REMOVE gk",
			})
			require.NoError(t, err)
			require.Equal(t, []string{"i0"}, scanBackfillIndex(t, table))
			require.Equal(t, int64(1), table.Indexes[backfillIndex].count())
		})
	}
}

func TestIndexedItemsAreDeletedAfterBackfill(t *testing.T) {
	table := newBackfillTable(t, &types.Item{S: new("a")}, &types.Item{S: new("a")})
	require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

	_, err := table.Delete(&types.DeleteItemInput{
		TableName: &table.Name, Key: map[string]*types.Item{"pk": {S: new("i0")}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"i1"}, queryBackfillIndex(t, table, "a"))
	require.Equal(t, int64(1), table.Indexes[backfillIndex].count())
}

func TestWritesViolatingTheIndexKeyAreRejectedAfterBackfill(t *testing.T) {
	table := newBackfillTable(t, &types.Item{S: new("a")})
	require.NoError(t, createBackfillIndex(table, gsiAllProjection()))

	for name, gk := range map[string]*types.Item{"empty": {S: new("")}, "wrong type": {N: new("5")}} {
		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: backfillItem("new", gk)})
		require.Error(t, err, name)
		require.NotContains(t, table.Data, "new")
	}

	require.Equal(t, []string{"i0"}, scanBackfillIndex(t, table))
}
