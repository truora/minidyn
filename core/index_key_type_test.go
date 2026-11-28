package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// An index key attribute that holds a value of another type than the index declares is
// rejected (#213). DynamoDB Local words a scalar N or B under an S key as "Type mismatch for
// Index Key", and any other type (NULL, BOOL, L, M and the sets) as "Invalid attribute value
// type".
const (
	indexKeyMismatchMsg = "One or more parameter values were invalid: Type mismatch for Index Key"
	indexKeyInvalidMsg  = "Invalid attribute value type"
)

func ikTable(t *testing.T) *Table {
	t.Helper()

	table := createKeyConditionTable(t)
	table.AttributesDef["l"] = "S"

	all := "ALL"
	require.NoError(t, table.AddLocalIndexes([]*types.LocalSecondaryIndex{{
		IndexName:  new("zz-local"),
		KeySchema:  []*types.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}, {AttributeName: "l", KeyType: "RANGE"}},
		Projection: &types.Projection{ProjectionType: &all},
	}}))

	return table
}

func ikItem(extra map[string]*types.Item) map[string]*types.Item {
	item := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}}
	for k, v := range extra {
		item[k] = v
	}

	return item
}

func ikWrongValues() map[string]*types.Item {
	return map[string]*types.Item{
		"NULL": {NULL: new(true)},
		"N":    {N: new("1")},
		"B":    {B: []byte{1}},
		"BOOL": {BOOL: new(true)},
		"L":    {L: []*types.Item{{S: new("x")}}},
		"M":    {M: map[string]*types.Item{"a": {S: new("x")}}},
		"SS":   {SS: []*string{new("x")}},
		"NS":   {NS: []*string{new("1")}},
		"BS":   {BS: [][]byte{{1}}},
	}
}

func ikWantMsg(kind string) string {
	if kind == "N" || kind == "B" {
		return indexKeyMismatchMsg
	}

	return indexKeyInvalidMsg
}

func TestTable_PutRejectsWrongTypeIndexKeys(t *testing.T) {
	for kind, wrong := range ikWrongValues() {
		for _, attr := range []string{"type", "rank", "l"} {
			t.Run(attr+"_"+kind, func(t *testing.T) {
				table := ikTable(t)
				cond := "attribute_exists(zzz)"

				// The validation wins over a condition that would fail.
				_, err := table.Put(&types.PutItemInput{Item: ikItem(map[string]*types.Item{attr: wrong}), ConditionExpression: &cond})
				require.Error(t, err)
				require.Contains(t, err.Error(), ikWantMsg(kind))
				require.NotContains(t, err.Error(), "ConditionalCheckFailed")

				require.Empty(t, table.Data, "a rejected item is not stored")
			})
		}
	}
}

func TestTable_PutIndexKeyOrder(t *testing.T) {
	cases := []struct {
		name string
		item map[string]*types.Item
		want string
	}{
		{"primary type before an empty index key", map[string]*types.Item{"name": {N: new("1")}, "type": emptyS()}, "Type mismatch for key"},
		{"primary type before a NULL index key", map[string]*types.Item{"name": {N: new("1")}, "type": {NULL: new(true)}}, "Type mismatch for key"},
		{"local before global", map[string]*types.Item{"type": {N: new("1")}, "l": {N: new("1")}}, "Type mismatch for Index Key | index: zz-local; attribute: l"},
		{"wrong global before empty local", map[string]*types.Item{"type": {N: new("1")}, "l": emptyS()}, "IndexName: zz-local, IndexKey: l"},
		{"empty global before wrong local", map[string]*types.Item{"type": emptyS(), "l": {N: new("1")}}, "Type mismatch for Index Key | index: zz-local; attribute: l"},
		{"hash before range, empty first", map[string]*types.Item{"type": emptyS(), "rank": {N: new("1")}}, "IndexName: by-type, IndexKey: type"},
		{"hash before range, wrong first", map[string]*types.Item{"type": {N: new("1")}, "rank": emptyS()}, "Type mismatch for Index Key | index: by-type; attribute: type"},
		{"hash before range, NULL range", map[string]*types.Item{"type": {N: new("1")}, "rank": {NULL: new(true)}}, "Type mismatch for Index Key | index: by-type; attribute: type"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ikTable(t).Put(&types.PutItemInput{Item: ikItem(tt.item)})
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestTable_PutAcceptsCorrectIndexKeys(t *testing.T) {
	table := ikTable(t)
	_, err := table.Put(&types.PutItemInput{Item: ikItem(map[string]*types.Item{
		"type": {S: new("t")}, "rank": {S: new("r")}, "l": {S: new("l")}, "other": {NULL: new(true)},
	})})
	require.NoError(t, err)
}

func TestTable_UpdateRejectsWrongTypeIndexKeys(t *testing.T) {
	for kind, wrong := range ikWrongValues() {
		for _, attr := range []string{"type", "rank", "l"} {
			t.Run(attr+"_"+kind, func(t *testing.T) {
				table := ikTable(t)
				key := map[string]*types.Item{"id": {S: new("a")}, "name": {S: new("n")}}

				_, err := table.Put(&types.PutItemInput{Item: ikItem(nil)})
				require.NoError(t, err)

				// The resulting item is checked, after the condition.
				failing := "attribute_exists(zzz)"
				_, err = table.Update(&types.UpdateItemInput{
					Key: key, UpdateExpression: "SET #a = :v", ConditionExpression: &failing,
					ExpressionAttributeNames:  map[string]string{"#a": attr},
					ExpressionAttributeValues: map[string]*types.Item{":v": wrong},
				})
				require.ErrorContains(t, err, "The conditional request failed")

				_, err = table.Update(&types.UpdateItemInput{
					Key: key, UpdateExpression: "SET #a = :v",
					ExpressionAttributeNames:  map[string]string{"#a": attr},
					ExpressionAttributeValues: map[string]*types.Item{":v": wrong},
				})
				require.Error(t, err)
				require.Contains(t, err.Error(), ikWantMsg(kind))
				require.NotContains(t, table.StoredItem(key), attr, "a rejected update changes nothing")
			})
		}
	}
}

// A scalar N or B is a ValidationError of the item in a transaction; the front ends read the
// code from the error.
func TestTable_indexKeyMismatchCancellationCode(t *testing.T) {
	table := ikTable(t)

	_, err := table.Put(&types.PutItemInput{Item: ikItem(map[string]*types.Item{"type": {N: new("1")}})})
	require.Equal(t, "ValidationError", CancellationCodeOf(err))

	_, err = table.Put(&types.PutItemInput{Item: ikItem(map[string]*types.Item{"type": {NULL: new(true)}})})
	require.Empty(t, CancellationCodeOf(err), "any other type is a ValidationException of the request")

	_, err = table.Put(&types.PutItemInput{Item: ikItem(map[string]*types.Item{"type": emptyS()})})
	require.Empty(t, CancellationCodeOf(err))
}
