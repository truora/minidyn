package core

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

const (
	betweenSameTypeMsg = "Invalid KeyConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; "
	betweenUpperMsg    = "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; "
	wrongKeyCountMsg   = "The number of conditions on the keys is invalid"
)

func nItem(v string) *types.Item { return &types.Item{N: &v} }

// D3: DynamoDB reports the Key problems before the placeholders and the syntax of the
// expressions, so these tests pin the order of the checks.
func TestUpdateDelete_keyBeforeExpressions(t *testing.T) {
	table := createKeyConditionTable(t)
	emptyKey := map[string]*types.Item{"id": {S: new("a")}, "name": emptyS()}
	shortKey := map[string]*types.Item{"id": {S: new("a")}}

	cases := []struct {
		name string
		key  map[string]*types.Item
		want string
	}{
		{"empty key", emptyKey, emptyStringKeyMsg + " Key: name"},
		{"wrong key count", shortKey, wrongKeyCountMsg},
	}

	for _, tc := range cases {
		t.Run("update undefined value "+tc.name, func(t *testing.T) {
			_, err := table.Update(&types.UpdateItemInput{Key: tc.key, UpdateExpression: "SET n = :v"})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("update undeclared name "+tc.name, func(t *testing.T) {
			_, err := table.Update(&types.UpdateItemInput{
				Key: tc.key, UpdateExpression: "SET #n = :v",
				ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("x")}},
			})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("update syntax error "+tc.name, func(t *testing.T) {
			_, err := table.Update(&types.UpdateItemInput{Key: tc.key, UpdateExpression: "SET"})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("update condition "+tc.name, func(t *testing.T) {
			_, err := table.Update(&types.UpdateItemInput{
				Key: tc.key, UpdateExpression: "SET n = :v", ConditionExpression: new("m = :x"),
				ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("x")}},
			})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("update invalid RVOCCF "+tc.name, func(t *testing.T) {
			_, err := table.Update(&types.UpdateItemInput{
				Key: tc.key, UpdateExpression: "SET n = :v", ReturnValuesOnConditionCheckFailure: new("BOGUS"),
				ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("x")}},
			})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("delete undefined value "+tc.name, func(t *testing.T) {
			_, err := table.Delete(&types.DeleteItemInput{Key: tc.key, ConditionExpression: new("n = :x")})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("delete undeclared name "+tc.name, func(t *testing.T) {
			_, err := table.Delete(&types.DeleteItemInput{Key: tc.key, ConditionExpression: new("attribute_exists(#n)")})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("delete syntax error "+tc.name, func(t *testing.T) {
			_, err := table.Delete(&types.DeleteItemInput{Key: tc.key, ConditionExpression: new("attribute_exists(")})
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("update invalid ReturnValues comes before the key", func(t *testing.T) {
		_, err := table.Update(&types.UpdateItemInput{
			Key: emptyKey, UpdateExpression: "SET n = :v", ReturnValues: new("BOGUS"),
			ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("x")}},
		})
		require.ErrorContains(t, err, "Return values set to invalid value")
	})

	t.Run("put reports the key values before the placeholders", func(t *testing.T) {
		_, err := table.Put(&types.PutItemInput{
			Item:                map[string]*types.Item{"id": {S: new("a")}, "name": emptyS()},
			ConditionExpression: new("n = :x"),
		})
		require.ErrorContains(t, err, emptyStringKeyMsg+" Key: name")

		_, err = table.Put(&types.PutItemInput{
			Item:                map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}, "type": emptyS()},
			ConditionExpression: new("n = :x"),
		})
		require.ErrorContains(t, err, "IndexName: by-type, IndexKey: type")
	})
}

func betweenTable(attrType string) *Table {
	table := NewTable("between")
	table.AttributesDef = map[string]string{"id": "S", "r": attrType}
	table.KeySchema = keySchema{"id", "r", false}

	return table
}

func queryBetween(table *Table, vals map[string]*types.Item) error {
	vals[":a"], vals[":b"] = vals["lo"], vals["hi"]
	delete(vals, "lo")
	delete(vals, "hi")

	_, err := table.SearchData(QueryInput{
		KeyConditionExpression:    "id = :p AND r BETWEEN :a AND :b",
		ExpressionAttributeValues: vals,
		ScanIndexForward:          true,
	})

	return err
}

func TestSearchData_betweenBoundsByType(t *testing.T) {
	p := &types.Item{S: new("p")}

	cases := []struct {
		name  string
		attr  string
		lo    *types.Item
		hi    *types.Item
		extra map[string]*types.Item
		want  string
	}{
		{
			"N reversed", "N", nItem("10"), nItem("9"), nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {N:10}, upper bound operand: AttributeValue: {N:9} | attribute: r",
		},
		{
			"N reversed decimals", "N", nItem("1.5"), nItem("1.25"), nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {N:1.5}, upper bound operand: AttributeValue: {N:1.25} | attribute: r",
		},
		{
			"N trailing zero is printed as given", "N", nItem("10.0"), nItem("9"), nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {N:10.0}, upper bound operand: AttributeValue: {N:9} | attribute: r",
		},
		{
			"N exponent is printed as given", "N", nItem("1e1"), nItem("9"), nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {N:1e1}, upper bound operand: AttributeValue: {N:9} | attribute: r",
		},
		{
			"N negative", "N", nItem("-1"), nItem("-2"), nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {N:-1}, upper bound operand: AttributeValue: {N:-2} | attribute: r",
		},
		{"N in order numerically", "N", nItem("9"), nItem("10"), nil, ""},
		{"N equal spellings", "N", nItem("10"), nItem("10.0"), nil, ""},
		{
			"B reversed", "B", &types.Item{B: []byte{2}}, &types.Item{B: []byte{1}}, nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {B:java.nio.HeapByteBuffer[pos=0 lim=1 cap=1]}, upper bound operand: AttributeValue: {B:java.nio.HeapByteBuffer[pos=0 lim=1 cap=1]} | attribute: r",
		},
		{"B in order", "B", &types.Item{B: []byte{1}}, &types.Item{B: []byte{1, 0}}, nil, ""},
		{
			"B empty high", "B", &types.Item{B: []byte{1}}, &types.Item{B: []byte{}}, nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {B:java.nio.HeapByteBuffer[pos=0 lim=1 cap=1]}, upper bound operand: AttributeValue: {B:java.nio.HeapByteBuffer[pos=0 lim=0 cap=0]} | attribute: r",
		},
		{
			"mixed S and N", "S", &types.Item{S: new("z")}, nItem("1"), nil,
			betweenSameTypeMsg + "lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {N:1} | attribute: r",
		},
		{
			"mixed N and S", "S", nItem("1"), &types.Item{S: new("a")}, nil,
			betweenSameTypeMsg + "lower bound operand: AttributeValue: {N:1}, upper bound operand: AttributeValue: {S:a} | attribute: r",
		},
		// The bounds are checked before every other value of the key condition.
		{
			"reversed bounds before an empty hash", "S", &types.Item{S: new("z")}, &types.Item{S: new("a")},
			map[string]*types.Item{":p": emptyS()},
			betweenUpperMsg + "lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a} | attribute: r",
		},
		{
			"reversed bounds before a hash type mismatch", "S", &types.Item{S: new("z")}, &types.Item{S: new("a")},
			map[string]*types.Item{":p": nItem("1")},
			betweenUpperMsg + "lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a} | attribute: r",
		},
		{
			"reversed bounds before their own type mismatch", "N", &types.Item{S: new("z")}, &types.Item{S: new("a")}, nil,
			betweenUpperMsg + "lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {S:a} | attribute: r",
		},
		{
			"mixed bounds before an empty hash", "S", &types.Item{S: new("z")}, nItem("1"),
			map[string]*types.Item{":p": emptyS()},
			betweenSameTypeMsg + "lower bound operand: AttributeValue: {S:z}, upper bound operand: AttributeValue: {N:1} | attribute: r",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vals := map[string]*types.Item{":p": p, "lo": tc.lo, "hi": tc.hi}
			maps.Copy(vals, tc.extra)

			err := queryBetween(betweenTable(tc.attr), vals)
			if tc.want == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestSearchData_betweenBoundsOnIndex(t *testing.T) {
	table := createKeyConditionTable(t)

	_, err := table.SearchData(QueryInput{
		Index:                  "by-type",
		KeyConditionExpression: "type = :p AND rank BETWEEN :a AND :b",
		ExpressionAttributeValues: map[string]*types.Item{
			":p": emptyS(), ":a": {S: new("b")}, ":b": {S: new("a")},
		},
		ScanIndexForward: true,
	})
	require.ErrorContains(t, err, betweenUpperMsg+
		"lower bound operand: AttributeValue: {S:b}, upper bound operand: AttributeValue: {S:a} | attribute: rank; index: by-type")
}

// Local reports the local secondary indexes before the global ones, whatever their names.
func TestTable_emptyIndexKeyReportOrder(t *testing.T) {
	table := createKeyConditionTable(t)
	table.AttributesDef["l"] = "S"

	all := "ALL"
	require.NoError(t, table.AddLocalIndexes([]*types.LocalSecondaryIndex{{
		IndexName:  new("zz-local"),
		KeySchema:  []*types.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}, {AttributeName: "l", KeyType: "RANGE"}},
		Projection: &types.Projection{ProjectionType: &all},
	}}))

	_, err := table.Put(&types.PutItemInput{Item: map[string]*types.Item{
		"id": {S: new("a")}, "name": {S: new("n")}, "type": emptyS(), "l": emptyS(),
	}})
	require.ErrorContains(t, err, secondaryKeyMsg+"The AttributeValue for a key attribute cannot contain an empty string value. IndexName: zz-local, IndexKey: l")
}

// D8: an index that rejects the item leaves the table and every index as they were.
func TestTable_indexRejectionWritesNothing(t *testing.T) {
	newTable := func(t *testing.T) (*Table, map[string]*types.Item) {
		t.Helper()

		table := createKeyConditionTable(t)
		key := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}}
		_, err := table.Put(&types.PutItemInput{Item: map[string]*types.Item{
			"id": {S: new("a")}, "name": {S: new("n")}, "type": {S: new("t")}, "rank": {S: new("r")}, "x": {S: new("old")},
		}})
		require.NoError(t, err)

		return table, key
	}

	requireOld := func(t *testing.T, table *Table, key map[string]*types.Item) {
		t.Helper()

		stored := table.StoredItem(key)
		require.Equal(t, "old", *stored["x"].S)
		require.Equal(t, "t", *stored["type"].S)
		requireIndexUnchanged(t, table)
	}

	t.Run("update", func(t *testing.T) {
		table, key := newTable(t)

		_, err := table.Update(&types.UpdateItemInput{
			Key: key, UpdateExpression: "SET #t = :n, x = :w",
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]*types.Item{":n": nItem("5"), ":w": {S: new("changed")}},
		})
		require.Error(t, err)
		requireOld(t, table, key)
	})

	t.Run("put", func(t *testing.T) {
		table, key := newTable(t)

		_, err := table.Put(&types.PutItemInput{Item: map[string]*types.Item{
			"id": {S: new("a")}, "name": {S: new("n")}, "type": nItem("5"), "x": {S: new("changed")},
		}})
		require.Error(t, err)
		requireOld(t, table, key)
	})

	t.Run("put of a new item", func(t *testing.T) {
		table, _ := newTable(t)

		_, err := table.Put(&types.PutItemInput{Item: map[string]*types.Item{
			"id": {S: new("b")}, "name": {S: new("n")}, "type": nItem("5"),
		}})
		require.Error(t, err)
		require.Len(t, table.Data, 1)
		require.Len(t, table.SortedKeys, 1)
	})

	t.Run("update of a new item", func(t *testing.T) {
		table, key := newTable(t)

		_, err := table.Update(&types.UpdateItemInput{
			Key:                       map[string]*types.Item{"id": {S: new("b")}, "name": {S: new("n")}},
			UpdateExpression:          "SET #t = :n",
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]*types.Item{":n": nItem("5")},
		})
		require.Error(t, err)
		require.Len(t, table.Data, 1)
		require.Len(t, table.SortedKeys, 1)
		requireOld(t, table, key)
	})
}

// DynamoDB rejects an IndexName the table does not have before anything else of a Query or
// Scan, ahead of the placeholders, the key condition and Select, and before any item is read.
func TestSearchData_unknownIndexComesFirst(t *testing.T) {
	table := createSelectTable(t)

	const want = "ValidationException: The table does not have the specified index: nope | table: pokemons"

	cases := []struct {
		name  string
		input QueryInput
	}{
		{"query", QueryInput{
			KeyConditionExpression:    "id = :id",
			ExpressionAttributeValues: map[string]*types.Item{":id": {S: new("001")}},
		}},
		{"scan", QueryInput{Scan: true}},
		{"query empty hash value", QueryInput{
			KeyConditionExpression:    "id = :id",
			ExpressionAttributeValues: map[string]*types.Item{":id": emptyS()},
		}},
		{"query key condition type mismatch", QueryInput{
			KeyConditionExpression:    "id = :id",
			ExpressionAttributeValues: map[string]*types.Item{":id": nItem("1")},
		}},
		{"query undeclared name", QueryInput{
			KeyConditionExpression:    "#n = :id",
			ExpressionAttributeValues: map[string]*types.Item{":id": {S: new("001")}},
		}},
		{"query undefined value", QueryInput{KeyConditionExpression: "id = :id"}},
		{"query invalid select", QueryInput{
			Select:                    "BOGUS",
			KeyConditionExpression:    "id = :id",
			ExpressionAttributeValues: map[string]*types.Item{":id": {S: new("001")}},
		}},
		{"scan undeclared name", QueryInput{Scan: true, FilterExpression: "#n = :v", ExpressionAttributeValues: map[string]*types.Item{":v": {S: new("x")}}}},
		{"scan undefined value", QueryInput{Scan: true, FilterExpression: "name = :v"}},
		{"scan invalid select", QueryInput{Scan: true, Select: "BOGUS"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.input.Index = "nope"
			tc.input.ScanIndexForward = true

			_, err := table.SearchData(tc.input)
			require.EqualError(t, err, want)
		})
	}
}

func TestValidateSearchIndex(t *testing.T) {
	table := createSelectTable(t)

	require.NoError(t, table.ValidateSearchIndex(PrimaryIndexName))
	require.NoError(t, table.ValidateSearchIndex("keys"))

	err := table.ValidateSearchIndex("nope")
	require.EqualError(t, err, "ValidationException: The table does not have the specified index: nope | table: "+table.Name)
}

const gsiConsistentReadMsg = "ValidationException: Consistent reads are not supported on global secondary indexes | index: keys"

func createConsistentReadTable(t *testing.T) *Table {
	t.Helper()

	table := createSelectTable(t)
	lsis := createLocalSecondaryIndex()
	lsis[0].KeySchema = []*types.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}}
	require.NoError(t, table.AddLocalIndexes(lsis))

	return table
}

// DynamoDB serves a GSI from an eventually consistent copy, so ConsistentRead true is an
// error there, and only there: the table and an LSI take it.
func TestValidateConsistentRead(t *testing.T) {
	table := createConsistentReadTable(t)

	require.EqualError(t, table.ValidateConsistentRead("keys", true), gsiConsistentReadMsg)
	require.NoError(t, table.ValidateConsistentRead("keys", false))
	require.NoError(t, table.ValidateConsistentRead("indexname", true))
	require.NoError(t, table.ValidateConsistentRead(PrimaryIndexName, true))
	// An unknown index is ValidateSearchIndex's to report.
	require.NoError(t, table.ValidateConsistentRead("nope", true))
}

func TestSearchDataConsistentReadOnIndex(t *testing.T) {
	table := createConsistentReadTable(t)
	keyCond := "id = :id"
	vals := map[string]*types.Item{":id": {S: new("001")}}

	cases := []struct {
		name    string
		input   QueryInput
		wantErr string
	}{
		{"query GSI", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, ConsistentRead: true}, gsiConsistentReadMsg},
		{"scan GSI", QueryInput{Index: "keys", Scan: true, ConsistentRead: true}, gsiConsistentReadMsg},
		{"query GSI eventually consistent", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals}, ""},
		{"scan GSI eventually consistent", QueryInput{Index: "keys", Scan: true}, ""},
		{"query LSI", QueryInput{Index: "indexname", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, ConsistentRead: true}, ""},
		{"scan LSI", QueryInput{Index: "indexname", Scan: true, ConsistentRead: true}, ""},
		{"query table", QueryInput{KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, ConsistentRead: true}, ""},
		{"scan table", QueryInput{Scan: true, ConsistentRead: true}, ""},
		// The unknown index comes first.
		{
			"query unknown index",
			QueryInput{Index: "nope", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, ConsistentRead: true},
			"ValidationException: The table does not have the specified index: nope | table: " + table.Name,
		},
		// A Query reports undefined placeholders first, and everything else of the request after.
		{
			"query undefined value",
			QueryInput{Index: "keys", KeyConditionExpression: keyCond, ConsistentRead: true},
			"ValidationException: Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :id",
		},
		{
			"query undeclared name",
			QueryInput{Index: "keys", KeyConditionExpression: "#n = :id", ExpressionAttributeValues: vals, ConsistentRead: true},
			"ValidationException: Invalid KeyConditionExpression: An expression attribute name used in the document path is not defined; attribute name: #n",
		},
		{"query empty hash value", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: map[string]*types.Item{":id": emptyS()}, ConsistentRead: true}, gsiConsistentReadMsg},
		{"query key condition type mismatch", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: map[string]*types.Item{":id": nItem("1")}, ConsistentRead: true}, gsiConsistentReadMsg},
		{"query key condition on a non-key", QueryInput{Index: "keys", KeyConditionExpression: "type = :id", ExpressionAttributeValues: vals, ConsistentRead: true}, gsiConsistentReadMsg},
		{"query projection the index lacks", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, ProjectionExpression: "type", ConsistentRead: true}, gsiConsistentReadMsg},
		{"scan projection the index lacks", QueryInput{Index: "keys", Scan: true, ProjectionExpression: "type", ConsistentRead: true}, gsiConsistentReadMsg},
		{"query invalid select", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, Select: "BOGUS", ConsistentRead: true}, gsiConsistentReadMsg},
		{"scan invalid select", QueryInput{Index: "keys", Scan: true, Select: "BOGUS", ConsistentRead: true}, gsiConsistentReadMsg},
		{"query ALL_ATTRIBUTES on a keys-only GSI", QueryInput{Index: "keys", KeyConditionExpression: keyCond, ExpressionAttributeValues: vals, Select: "ALL_ATTRIBUTES", ConsistentRead: true}, gsiConsistentReadMsg},
		// A Scan reports it ahead of its placeholders (DynamoDB Local does).
		{"scan undeclared name", QueryInput{Index: "keys", Scan: true, FilterExpression: "#n = :id", ExpressionAttributeValues: vals, ConsistentRead: true}, gsiConsistentReadMsg},
		{"scan undefined value", QueryInput{Index: "keys", Scan: true, FilterExpression: "id = :id", ConsistentRead: true}, gsiConsistentReadMsg},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.input.ScanIndexForward = true

			_, err := table.SearchData(tc.input)
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.EqualError(t, err, tc.wantErr)
		})
	}
}
