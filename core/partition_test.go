package core

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestPartitionPrefix(t *testing.T) {
	table := NewTable("partition")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "S", "n": "N"}

	composite := keySchema{HashKey: "pk", RangeKey: "sk"}
	hashOnly := keySchema{HashKey: "pk"}
	numeric := keySchema{HashKey: "n"}

	values := map[string]*types.Item{
		":p": {S: aws.String("alice")},
		":n": {N: aws.String("7")},
	}

	tests := []struct {
		name   string
		input  QueryInput
		schema keySchema
		want   string
		ok     bool
	}{
		{"composite key", QueryInput{KeyConditionExpression: "pk = :p AND sk > :p", ExpressionAttributeValues: values}, composite, "alice.", true},
		{"hash-only key", QueryInput{KeyConditionExpression: "pk = :p", ExpressionAttributeValues: values}, hashOnly, "alice", true},
		// Stored numbers keep the text they were written with ("7" vs "7.0"), so a prefix
		// can't find every spelling of a number: numeric partition keys keep the full walk.
		{"numeric hash key", QueryInput{KeyConditionExpression: "n = :n", ExpressionAttributeValues: values}, numeric, "", false},
		{"scan", QueryInput{Scan: true, KeyConditionExpression: "pk = :p", ExpressionAttributeValues: values}, composite, "", false},
		{"no key condition", QueryInput{ExpressionAttributeValues: values}, composite, "", false},
		{"no hash equality", QueryInput{KeyConditionExpression: "sk = :p", ExpressionAttributeValues: values}, composite, "", false},
		{"missing value", QueryInput{KeyConditionExpression: "pk = :missing", ExpressionAttributeValues: values}, composite, "", false},
		{"nil value", QueryInput{KeyConditionExpression: "pk = :p", ExpressionAttributeValues: map[string]*types.Item{":p": nil}}, composite, "", false},
		{"type mismatch", QueryInput{KeyConditionExpression: "pk = :n", ExpressionAttributeValues: values}, composite, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := table.partitionPrefix(tt.input, tt.schema)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestKeysWithPrefix(t *testing.T) {
	keys := []string{"a", "a.1", "a.2", "a.b.1", "ab.1", "b.1"}

	require.Equal(t, []string{"a.1", "a.2", "a.b.1"}, keysWithPrefix(keys, "a."))
	require.Equal(t, []string{"b.1"}, keysWithPrefix(keys, "b."))
	require.Empty(t, keysWithPrefix(keys, "c."))
	require.Empty(t, keysWithPrefix(nil, "a."))
}

// seekTestTable returns a table with keys (pk, sk) or just pk, holding the given items.
func seekTestTable(t *testing.T, withRange bool, items ...map[string]*types.Item) *Table {
	t.Helper()

	table := NewTable("seek")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "S"}
	table.KeySchema = keySchema{HashKey: "pk"}

	if withRange {
		table.KeySchema.RangeKey = "sk"
	}

	for _, it := range items {
		_, err := table.Put(&types.PutItemInput{TableName: aws.String("seek"), Item: it})
		require.NoError(t, err)
	}

	return table
}

func seekItem(pk, sk string) map[string]*types.Item {
	it := map[string]*types.Item{"pk": {S: aws.String(pk)}, "f": {S: aws.String("yes")}}
	if sk != "" {
		it["sk"] = &types.Item{S: aws.String(sk)}
	}

	return it
}

func TestSearchData_otherPartitionsSharingThePrefix(t *testing.T) {
	// "alicex" and "alicez" share the "alice" prefix the seek narrows to; they must neither
	// count toward Limit nor become the LastEvaluatedKey.
	table := seekTestTable(t, false, seekItem("alice", ""), seekItem("alicex", ""), seekItem("alicez", ""), seekItem("bob", ""))

	items, last, err := searchData(table, QueryInput{
		KeyConditionExpression:    "pk = :p",
		FilterExpression:          "f = :f",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("alice")}, ":f": {S: aws.String("yes")}},
		Limit:                     2,
		ScanIndexForward:          true,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Empty(t, last)

	composite := seekTestTable(t, true, seekItem("a", "1"), seekItem("a", "2"), seekItem("a.b", "1"), seekItem("a.b", "2"))

	items, last, err = searchData(composite, QueryInput{
		KeyConditionExpression:    "pk = :p",
		FilterExpression:          "f = :f",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("a")}, ":f": {S: aws.String("yes")}},
		Limit:                     3,
		ScanIndexForward:          true,
	})
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.Empty(t, last)
}

func TestSearchData_invalidKeyConditionOnEmptyPartition(t *testing.T) {
	// Before the seek, the first item of the table surfaced evaluation errors; an empty
	// partition must not turn an invalid key condition into an empty success.
	table := seekTestTable(t, true, seekItem("a", "1"))

	_, _, err := searchData(table, QueryInput{
		KeyConditionExpression:    "pk = :p AND sk > :n",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("zzz")}, ":n": {N: aws.String("1")}},
		ScanIndexForward:          true,
	})
	require.Error(t, err)
}

func TestSearchData_exclusiveStartKeyFromAnotherPartition(t *testing.T) {
	// DynamoDB rejects this start key; minidyn stays lenient (consumers' tests rely on it)
	// and returns an empty page, since the start key is never reached.
	table := seekTestTable(t, true, seekItem("a", "1"), seekItem("b", "1"))

	items, last, err := searchData(table, QueryInput{
		KeyConditionExpression:    "pk = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": {S: aws.String("b")}},
		ExclusiveStartKey:         map[string]*types.Item{"pk": {S: aws.String("a")}, "sk": {S: aws.String("1")}},
		ScanIndexForward:          true,
	})
	require.NoError(t, err)
	require.Empty(t, items)
	require.Empty(t, last)
}
