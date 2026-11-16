package core

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// An ExclusiveStartKey does not have to be an existing item (#258): the search resumes at
// the first key after it in the order the search walks.

func startKeyValue(typ, v string) *types.Item {
	switch typ {
	case "N":
		return &types.Item{N: aws.String(v)}
	case "B":
		return &types.Item{B: []byte(v)}
	default:
		return &types.Item{S: aws.String(v)}
	}
}

func startKeyLabel(it *types.Item) string {
	switch {
	case it == nil:
		return "<nil>"
	case it.N != nil:
		return *it.N
	case it.B != nil:
		return string(it.B)
	case it.S != nil:
		return *it.S
	}

	return "?"
}

// startKeyTable returns table pk (S) / sk (skType), LSI "by-l" on pk / l and GSI "by-g" on
// g / gr, holding one item per sk, whose l and gr are the matching entry of idxKeys.
func startKeyTestTable(t *testing.T, skType string, sks, idxKeys []string) *Table {
	t.Helper()

	table := NewTable("startkey")
	table.AttributesDef = map[string]string{"pk": "S", "sk": skType, "l": "S", "g": "S", "gr": "S"}
	table.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	all := "ALL"
	proj := &types.Projection{ProjectionType: &all}
	name := func(s string) *string { return &s }

	require.NoError(t, table.AddIndexes(
		[]*types.GlobalSecondaryIndex{{
			IndexName: name("by-g"), Projection: proj,
			ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			KeySchema:             []*types.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}, {AttributeName: "gr", KeyType: "RANGE"}},
		}},
		[]*types.LocalSecondaryIndex{{
			IndexName: name("by-l"), Projection: proj,
			KeySchema: []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "l", KeyType: "RANGE"}},
		}},
	))

	for i, sk := range sks {
		_, err := table.Put(&types.PutItemInput{TableName: aws.String("startkey"), Item: map[string]*types.Item{
			"pk": {S: aws.String("p")}, "sk": startKeyValue(skType, sk),
			"l": {S: aws.String(idxKeys[i])}, "g": {S: aws.String("g")}, "gr": {S: aws.String(idxKeys[i])},
		}})
		require.NoError(t, err)
	}

	return table
}

func startKeySearch(t *testing.T, table *Table, in QueryInput, start map[string]*types.Item, forward bool) []string {
	t.Helper()

	in.ExclusiveStartKey = start
	in.ScanIndexForward = forward

	res, err := table.SearchData(in)
	require.NoError(t, err)

	keys := []string{}
	for _, it := range res.Items {
		keys = append(keys, startKeyLabel(it["sk"]))
	}

	return keys
}

func TestSearchData_exclusiveStartKeyOfMissingItemOnTable(t *testing.T) {
	tests := []struct {
		typ      string
		sks      []string
		start    string
		forward  []string
		backward []string
	}{
		// Between two keys, before the first, after the last, on an existing key.
		{"S", []string{"b", "d", "f"}, "c", []string{"d", "f"}, []string{"b"}},
		{"S", []string{"b", "d", "f"}, "a", []string{"b", "d", "f"}, []string{}},
		{"S", []string{"b", "d", "f"}, "g", []string{}, []string{"f", "d", "b"}},
		{"S", []string{"b", "d", "f"}, "d", []string{"f"}, []string{"b"}},
		{"S", []string{"b", "d", "f"}, "bb", []string{"d", "f"}, []string{"b"}},
		// Numbers order by value, whatever the spelling.
		{"N", []string{"-5", "2", "10", "100"}, "2.5", []string{"10", "100"}, []string{"2", "-5"}},
		{"N", []string{"-5", "2", "10", "100"}, "9.99", []string{"10", "100"}, []string{"2", "-5"}},
		{"N", []string{"-5", "2", "10", "100"}, "1e3", []string{}, []string{"100", "10", "2", "-5"}},
		{"N", []string{"-5", "2", "10", "100"}, "10.0", []string{"100"}, []string{"2", "-5"}},
		// Binary keys order by their bytes.
		{"B", []string{"\x01", "\x01\x00", "\x02"}, "\x01\x05", []string{"\x02"}, []string{"\x01\x00", "\x01"}},
		{"B", []string{"\x01", "\x01\x00", "\x02"}, "\x00", []string{"\x01", "\x01\x00", "\x02"}, []string{}},
		{"B", []string{"\x01", "\x01\x00", "\x02"}, "\x01\x00\x00", []string{"\x02"}, []string{"\x01\x00", "\x01"}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%q", tt.typ, tt.start), func(t *testing.T) {
			table := startKeyTestTable(t, tt.typ, tt.sks, tt.sks)
			q := QueryInput{
				KeyConditionExpression:    "pk = :p",
				ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}},
			}
			start := map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": startKeyValue(tt.typ, tt.start)}

			require.Equal(t, tt.forward, startKeySearch(t, table, q, start, true))
			require.Equal(t, tt.backward, startKeySearch(t, table, q, start, false))
		})
	}
}

func TestSearchData_exclusiveStartKeyOfDeletedItem(t *testing.T) {
	table := startKeyTestTable(t, "S", []string{"a", "b", "c"}, []string{"1", "2", "3"})

	_, err := table.Delete(&types.DeleteItemInput{
		TableName: aws.String("startkey"),
		Key:       map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String("b")}},
	})
	require.NoError(t, err)

	q := QueryInput{
		KeyConditionExpression:    "pk = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}},
	}
	start := map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String("b")}}

	require.Equal(t, []string{"c"}, startKeySearch(t, table, q, start, true))
	require.Equal(t, []string{"a"}, startKeySearch(t, table, q, start, false))
}

func TestSearchData_exclusiveStartKeyOfMissingItemKeepsLimitAndFilter(t *testing.T) {
	table := startKeyTestTable(t, "S", []string{"b", "d", "f", "h"}, []string{"1", "2", "3", "4"})

	res, err := table.SearchData(QueryInput{
		KeyConditionExpression:    "pk = :p",
		FilterExpression:          "l <> :x",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}, ":x": {S: aws.String("3")}},
		ExclusiveStartKey:         map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String("c")}},
		Limit:                     2,
		ScanIndexForward:          true,
	})
	require.NoError(t, err)
	require.Len(t, res.Items, 1)
	require.Equal(t, "d", startKeyLabel(res.Items[0]["sk"]))
	require.Equal(t, "f", startKeyLabel(res.LastEvaluatedKey["sk"]))
}

func TestSearchData_scanExclusiveStartKeyOfMissingItem(t *testing.T) {
	table := startKeyTestTable(t, "S", []string{"b", "d", "f"}, []string{"1", "2", "3"})

	for start, want := range map[string][]string{"a": {"b", "d", "f"}, "c": {"d", "f"}, "e": {"f"}, "g": {}} {
		got := startKeySearch(t, table, QueryInput{Scan: true},
			map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String(start)}}, true)
		require.Equal(t, want, got, start)
	}
}

// The index key decides where the walk resumes: the start item need not exist, nor have
// that index key.
func TestSearchData_exclusiveStartKeyOfMissingItemOnIndexes(t *testing.T) {
	// l and gr: b=1, a=3, c=5 (index order b, a, c; table order a, b, c).
	table := startKeyTestTable(t, "S", []string{"b", "a", "c"}, []string{"1", "3", "5"})

	lsiKey := func(sk, l string) map[string]*types.Item {
		return map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String(sk)}, "l": {S: aws.String(l)}}
	}
	gsiKey := func(sk, gr string) map[string]*types.Item {
		return map[string]*types.Item{
			"pk": {S: aws.String("p")}, "sk": {S: aws.String(sk)}, "g": {S: aws.String("g")}, "gr": {S: aws.String(gr)},
		}
	}

	lsi := QueryInput{
		Index:                     "by-l",
		KeyConditionExpression:    "pk = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}},
	}
	gsi := QueryInput{
		Index:                     "by-g",
		KeyConditionExpression:    "g = :g",
		ExpressionAttributeValues: map[string]*types.Item{":g": {S: aws.String("g")}},
	}

	tests := []struct {
		name     string
		in       QueryInput
		start    func(sk, v string) map[string]*types.Item
		sk, v    string
		forward  []string
		backward []string
	}{
		{"existing", lsi, lsiKey, "a", "3", []string{"c"}, []string{"b"}},
		{"missing item between", lsi, lsiKey, "zz", "2", []string{"a", "c"}, []string{"b"}},
		// Entries sharing an index key are ordered by primary key, minidyn's own tie order (D1
		// level 3: Local's order within such a run is internal and can't be matched).
		{"missing item on an index key, sk after", lsi, lsiKey, "zz", "3", []string{"c"}, []string{"a", "b"}},
		{"missing item on an index key, sk before", lsi, lsiKey, "0", "3", []string{"a", "c"}, []string{"b"}},
		{"missing item before first", lsi, lsiKey, "zz", "0", []string{"b", "a", "c"}, []string{}},
		{"missing item after last", lsi, lsiKey, "zz", "9", []string{}, []string{"c", "a", "b"}},
		{"existing item other index key", lsi, lsiKey, "a", "2", []string{"a", "c"}, []string{"b"}},
		{"existing item other index key on a key", lsi, lsiKey, "b", "5", []string{"c"}, []string{"a", "b"}},
		{"gsi existing", gsi, gsiKey, "a", "3", []string{"c"}, []string{"b"}},
		{"gsi missing item between", gsi, gsiKey, "zz", "4", []string{"c"}, []string{"a", "b"}},
		{"gsi missing item on an index key, sk after", gsi, gsiKey, "zz", "3", []string{"c"}, []string{"a", "b"}},
		{"gsi missing item on an index key, sk before", gsi, gsiKey, "0", "3", []string{"a", "c"}, []string{"b"}},
		{"gsi missing item before first", gsi, gsiKey, "zz", "0", []string{"b", "a", "c"}, []string{}},
		{"gsi missing item after last", gsi, gsiKey, "zz", "9", []string{}, []string{"c", "a", "b"}},
		{"gsi existing item other index key", gsi, gsiKey, "a", "2", []string{"a", "c"}, []string{"b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := tt.start(tt.sk, tt.v)

			require.Equal(t, tt.forward, startKeySearch(t, table, tt.in, start, true))
			require.Equal(t, tt.backward, startKeySearch(t, table, tt.in, start, false))
		})
	}
}

// A start key outside the queried partition, or lacking the index key, stays as lenient as
// before (#171): it is not rejected and the page is what it was.
func TestSearchData_exclusiveStartKeyStaysLenient(t *testing.T) {
	table := startKeyTestTable(t, "S", []string{"b", "a", "c"}, []string{"1", "3", "5"})

	lsi := QueryInput{
		Index:                     "by-l",
		KeyConditionExpression:    "pk = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}},
	}
	// The index key is missing: the table key finds the entry, as it always did.
	start := map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String("a")}}
	require.Equal(t, []string{"c"}, startKeySearch(t, table, lsi, start, true))

	// An item that does not exist and has no index key: nothing to resume from.
	start = map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": {S: aws.String("zz")}}
	require.Empty(t, startKeySearch(t, table, lsi, start, true))

	// Another partition.
	q := QueryInput{
		KeyConditionExpression:    "pk = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}},
	}
	start = map[string]*types.Item{"pk": {S: aws.String("a")}, "sk": {S: aws.String("1")}}
	require.Empty(t, startKeySearch(t, table, q, start, true))
	require.Empty(t, startKeySearch(t, table, q, start, false))
}

// Entries sharing an index key are ordered by primary key: minidyn's own tie order (D1
// level 3). The LastEvaluatedKey it hands out then resumes the same walk even when its item
// is deleted before the next page, with no entry repeated or lost.
func TestSearchData_resumeAfterDeletingLastEvaluatedItemInARun(t *testing.T) {
	for _, index := range []string{"by-g", "by-l"} {
		for _, forward := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/forward=%v", index, forward), func(t *testing.T) {
				table := startKeyTestTable(t, "S", []string{"a", "b", "c", "d"}, []string{"1", "1", "1", "1"})

				in := QueryInput{
					Index:                     index,
					KeyConditionExpression:    "pk = :p",
					ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("p")}},
					Limit:                     2,
					ScanIndexForward:          forward,
				}
				if index == "by-g" {
					in.KeyConditionExpression = "g = :g"
					in.ExpressionAttributeValues = map[string]*types.Item{":g": {S: aws.String("g")}}
				}

				first, err := table.SearchData(in)
				require.NoError(t, err)
				require.Len(t, first.Items, 2)
				require.NotEmpty(t, first.LastEvaluatedKey)

				seen := []string{}
				for _, it := range first.Items {
					seen = append(seen, startKeyLabel(it["sk"]))
				}

				_, err = table.Delete(&types.DeleteItemInput{
					TableName: aws.String("startkey"),
					Key:       map[string]*types.Item{"pk": {S: aws.String("p")}, "sk": first.LastEvaluatedKey["sk"]},
				})
				require.NoError(t, err)

				in.ExclusiveStartKey = first.LastEvaluatedKey
				in.Limit = 0

				second, err := table.SearchData(in)
				require.NoError(t, err)

				for _, it := range second.Items {
					seen = append(seen, startKeyLabel(it["sk"]))
				}

				want := []string{"a", "b", "c", "d"}
				if !forward {
					want = []string{"d", "c", "b", "a"}
				}

				// The deleted item is the last of the first page; nothing else repeats or is lost.
				require.Equal(t, want, seen)
			})
		}
	}
}
