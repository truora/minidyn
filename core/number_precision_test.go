package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

const digits38 = "12345678901234567890123456789012345678"

func numberPrecisionTable() *Table {
	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"pk": "S", "sk": "N"}
	table.KeySchema = keySchema{"pk", "sk", false}

	return table
}

func numberPrecisionKey() map[string]*types.Item {
	return map[string]*types.Item{"pk": {S: new("a")}, "sk": {N: new(digits38)}}
}

func numberPrecisionItem() map[string]*types.Item {
	item := numberPrecisionKey()
	item["n"] = &types.Item{N: new(digits38)}
	item["l"] = &types.Item{L: []*types.Item{{N: new(digits38)}}}
	item["m"] = &types.Item{M: map[string]*types.Item{"x": {N: new(digits38)}}}
	item["ns"] = &types.Item{NS: []*string{new(digits38)}}

	return item
}

func requireNumberPrecisionItem(t *testing.T, item map[string]*types.Item) {
	t.Helper()

	require.Equal(t, digits38, *item["sk"].N)
	require.Equal(t, digits38, *item["n"].N)
	require.Equal(t, digits38, *item["l"].L[0].N)
	require.Equal(t, digits38, *item["m"].M["x"].N)
	require.Equal(t, []*string{new(digits38)}, item["ns"].NS)
}

// storedNumberPrecisionItem reads the item stored under the 38-digit key, without a projection.
func storedNumberPrecisionItem(t *testing.T, table *Table) map[string]*types.Item {
	t.Helper()

	got, err := table.GetItem(numberPrecisionKey(), "", nil)
	require.NoError(t, err)

	return got.Stored
}

// A ProjectionExpression keeps the 38 digits of every N it returns: GetItem, BatchGetItem, Scan.
func TestProjectionKeepsNumberPrecision(t *testing.T) {
	t.Parallel()

	table := numberPrecisionTable()
	_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numberPrecisionItem()})
	require.NoError(t, err)

	const projection = "sk, n, l, m, ns"

	got, err := table.GetItem(numberPrecisionKey(), projection, nil)
	require.NoError(t, err)
	requireNumberPrecisionItem(t, got.Projected)

	batch, err := table.BatchGetItem([]map[string]*types.Item{numberPrecisionKey()}, projection, nil)
	require.NoError(t, err)
	requireNumberPrecisionItem(t, batch[0].Projected)

	res, err := table.SearchData(QueryInput{
		Index:                     PrimaryIndexName,
		Scan:                      true,
		ProjectionExpression:      projection,
		ExpressionAttributeValues: map[string]*types.Item{},
	})
	require.NoError(t, err)
	requireNumberPrecisionItem(t, res.Items[0])
}

// UpdateItem keeps the 38 digits of the key it creates and of every other N it rewrites.
func TestUpdateKeepsNumberPrecision(t *testing.T) {
	t.Parallel()

	t.Run("upsert key item", func(t *testing.T) {
		t.Parallel()

		table := numberPrecisionTable()
		_, err := table.Update(&types.UpdateItemInput{
			TableName:                 &table.Name,
			Key:                       numberPrecisionKey(),
			UpdateExpression:          "SET s = :s",
			ExpressionAttributeValues: map[string]*types.Item{":s": {S: new("v")}},
		})
		require.NoError(t, err)

		require.Equal(t, digits38, *storedNumberPrecisionItem(t, table)["sk"].N)
	})

	t.Run("existing item and return values", func(t *testing.T) {
		t.Parallel()

		table := numberPrecisionTable()
		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numberPrecisionItem()})
		require.NoError(t, err)

		for _, rv := range []string{"ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"} {
			out, err := table.Update(&types.UpdateItemInput{
				TableName:                 &table.Name,
				Key:                       numberPrecisionKey(),
				ReturnValues:              &rv,
				UpdateExpression:          "SET s = :s, c = n, ln = list_append(l, l), ie = if_not_exists(nope, n)",
				ExpressionAttributeValues: map[string]*types.Item{":s": {S: new("v")}},
			})
			require.NoError(t, err, rv)

			if rv == "ALL_OLD" || rv == "ALL_NEW" {
				requireNumberPrecisionItem(t, out)
			}
		}

		stored := storedNumberPrecisionItem(t, table)
		requireNumberPrecisionItem(t, stored)
		require.Equal(t, digits38, *stored["c"].N)
		require.Equal(t, digits38, *stored["ie"].N)
		require.Equal(t, digits38, *stored["ln"].L[1].N)
	})
}

// A condition compares N values exactly, not as float64.
func TestConditionComparesNumbersExactly(t *testing.T) {
	t.Parallel()

	table := numberPrecisionTable()
	_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numberPrecisionItem()})
	require.NoError(t, err)

	other := "12345678901234567890123456789012345679"

	for cond, want := range map[string]bool{"n = :same": true, "n = :other": false, "n < :other": true, "n >= :other": false} {
		_, err := table.Update(&types.UpdateItemInput{
			TableName:                 &table.Name,
			Key:                       numberPrecisionKey(),
			ConditionExpression:       &cond,
			UpdateExpression:          "SET s = :s",
			ExpressionAttributeValues: map[string]*types.Item{":s": {S: new("v")}, ":same": {N: new(digits38)}, ":other": {N: new(other)}},
		})
		if want {
			require.NoError(t, err, cond)
		} else {
			require.Error(t, err, cond)
		}
	}
}

// Arithmetic on up to 38 significant digits is exact, and a result that needs more fails.
func TestUpdateArithmeticIsExact(t *testing.T) {
	t.Parallel()

	const precisionErr = "DynamoDB only supports precision up to 38 digits"

	tests := []struct{ stored, expr, value, want, wantErr string }{
		{stored: digits38, expr: "SET n = n + :v", value: "1", want: "12345678901234567890123456789012345679"},
		{stored: digits38, expr: "ADD n :v", value: "1", want: "12345678901234567890123456789012345679"},
		{stored: digits38, expr: "SET n = n - :v", value: "1", want: "12345678901234567890123456789012345677"},
		{stored: "99999999999999999999999999999999999999", expr: "SET n = n + :v", value: "1", want: "100000000000000000000000000000000000000"},
		{stored: "0.1", expr: "SET n = n + :v", value: "0.2", want: "0.3"},
		{stored: "1", expr: "SET n = n + :v", value: "1e-37", want: "1.0000000000000000000000000000000000001"},
		{stored: "1.1", expr: "SET n = n + :v", value: "1.20", want: "2.3"},
		{stored: "1", expr: "SET n = n - :v", value: "3", want: "-2"},
		{stored: "1.5", expr: "SET n = n + :v", value: "-1.5", want: "0"},
		{stored: digits38, expr: "SET n = n + :v", value: "0.5", wantErr: precisionErr},
		{stored: "1", expr: "SET n = n + :v", value: "1e-38", wantErr: precisionErr},
		{stored: "1", expr: "ADD n :v", value: "1e-38", wantErr: precisionErr},
	}

	for _, tt := range tests {
		t.Run(tt.expr+" "+tt.stored+" "+tt.value, func(t *testing.T) {
			t.Parallel()

			table := numberPrecisionTable()
			item := numberPrecisionKey()
			item["n"] = &types.Item{N: new(tt.stored)}
			_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: item})
			require.NoError(t, err)

			_, err = table.Update(&types.UpdateItemInput{
				TableName:                 &table.Name,
				Key:                       numberPrecisionKey(),
				UpdateExpression:          tt.expr,
				ExpressionAttributeValues: map[string]*types.Item{":v": {N: new(tt.value)}},
			})

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, tt.stored, *storedNumberPrecisionItem(t, table)["n"].N, "a failed update changes nothing")

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, *storedNumberPrecisionItem(t, table)["n"].N)
		})
	}
}
