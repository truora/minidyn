package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

// N values are validated like DynamoDB Local does (#226): text that is not a number, an
// exponent past 32 bits, a magnitude out of range and more than 38 significant digits are
// rejected on every write, wherever the number sits, and in the values of an expression.

const (
	msgInvalidNumber = "A value provided cannot be converted into a number"
	msgOverflow      = "Number overflow. Attempting to store a number with magnitude larger than supported range"
	msgUnderflow     = "Number underflow. Attempting to store a number with magnitude smaller than supported range"
	msgPrecision     = "DynamoDB only supports precision up to 38 digits"
	msgEAV           = "ExpressionAttributeValues contains invalid value: "

	digits39 = "123456789012345678901234567890123456789"
)

var invalidNumbers = []struct{ in, msg string }{
	{"abc", msgInvalidNumber},
	{"", msgInvalidNumber},
	{"-", msgInvalidNumber},
	{"1e", msgInvalidNumber},
	{"0x10", msgInvalidNumber},
	{" 5", msgInvalidNumber},
	{"NaN", msgInvalidNumber},
	{"Infinity", msgInvalidNumber},
	{"1e2147483648", msgInvalidNumber},
	{"1e126", msgOverflow},
	{"1e2147483647", msgOverflow},
	{"1e-131", msgUnderflow},
	{"1e-2147483647", msgUnderflow},
	{digits39, msgPrecision},
	{"-0." + digits39, msgPrecision},
	{digits39 + "e130", msgPrecision},
}

var validNumbers = []string{
	"0", "-0", "0.0", "5.", ".5", "+5", "007", "1.50", "1e2", "1e125", "1e-130", "-9.99e125",
	digits38, "1.00000000000000000000000000000000000000000", "123456789012345678901234567890123456780",
}

func numValidationTable() *Table {
	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"pk": "S", "sk": "N"}
	table.KeySchema = keySchema{"pk", "sk", false}

	return table
}

func numKey(sk string) map[string]*types.Item {
	return map[string]*types.Item{"pk": {S: new("a")}, "sk": nItem(sk)}
}

func numItem(attr *types.Item) map[string]*types.Item {
	item := numKey("1")
	item["x"] = attr

	return item
}

func ns(members ...string) *types.Item {
	item := &types.Item{}
	for _, m := range members {
		item.NS = append(item.NS, new(m))
	}

	return item
}

func list(items ...*types.Item) *types.Item { return &types.Item{L: items} }

func requireNumberError(t *testing.T, err error, msg string) {
	t.Helper()

	require.Error(t, err)

	var typed types.Error

	require.ErrorAs(t, err, &typed)
	require.Equal(t, "ValidationException", typed.Code())
	require.True(t, strings.HasPrefix(typed.Message(), msg), typed.Message())
}

func TestPut_validatesNumbers(t *testing.T) {
	t.Parallel()

	shapes := map[string]func(n string) *types.Item{
		"top level": nItem,
		"list":      func(n string) *types.Item { return list(nItem("1"), nItem(n)) },
		"map":       func(n string) *types.Item { return &types.Item{M: map[string]*types.Item{"a": nItem(n)}} },
		"deep":      func(n string) *types.Item { return list(&types.Item{M: map[string]*types.Item{"a": list(nItem(n))}}) },
		"set":       func(n string) *types.Item { return ns("1", n) },
	}

	for shape, build := range shapes {
		for _, tc := range invalidNumbers {
			t.Run(shape+"/"+tc.in, func(t *testing.T) {
				t.Parallel()

				table := numValidationTable()

				_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(build(tc.in))})
				requireNumberError(t, err, tc.msg)
				require.Empty(t, table.Data, "nothing is stored")
			})
		}

		for _, in := range validNumbers {
			t.Run(shape+"/valid/"+in, func(t *testing.T) {
				t.Parallel()

				table := numValidationTable()

				_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(build(in))})
				require.NoError(t, err)
			})
		}
	}
}

// The key check the front ends run ahead of the expressions reports a number before the shape.
func TestValidatePrimaryKeyMap_numbersBeforeShape(t *testing.T) {
	t.Parallel()

	table := numValidationTable()

	require.ErrorContains(t, table.ValidatePrimaryKeyMap(numKey("abc")), msgInvalidNumber)
	require.ErrorContains(t, table.ValidatePrimaryKeyMap(map[string]*types.Item{"pk": emptyS(), "sk": nItem("1e126")}), msgOverflow)
	require.ErrorContains(t, table.ValidatePrimaryKeyMap(map[string]*types.Item{"pk": emptyS(), "sk": nItem("1")}), emptyStringKeyMsg)
	require.NoError(t, table.ValidatePrimaryKeyMap(numKey("1")))
}

func TestPut_numberErrorNamesTheAttribute(t *testing.T) {
	t.Parallel()

	table := numValidationTable()
	item := numItem(&types.Item{M: map[string]*types.Item{"vals": list(nItem("1"), nItem("abc"))}})

	_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: item})
	require.EqualError(t, err, "ValidationException: "+msgInvalidNumber+" | path: x.vals[1]; number: abc")
}

func TestKeyOperations_validateNumbers(t *testing.T) {
	t.Parallel()

	for _, tc := range invalidNumbers {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			table := numValidationTable()
			key := numKey(tc.in)

			_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: key})
			requireNumberError(t, err, tc.msg)

			_, err = table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: key, UpdateExpression: "SET x = :v",
				ExpressionAttributeValues: map[string]*types.Item{":v": nItem("1")},
			})
			requireNumberError(t, err, tc.msg)

			_, err = table.Delete(&types.DeleteItemInput{TableName: &table.Name, Key: key})
			requireNumberError(t, err, tc.msg)

			_, err = table.GetItem(key, "", nil)
			requireNumberError(t, err, tc.msg)

			_, err = table.CheckedKeyOf(key)
			requireNumberError(t, err, tc.msg)
		})
	}
}

// The values of an expression are reported the way DynamoDB Local words them, with the
// placeholder, for every operation that takes ExpressionAttributeValues.
func TestExpressionAttributeValues_validateNumbers(t *testing.T) {
	t.Parallel()

	for _, tc := range invalidNumbers {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			table := numValidationTable()
			want := msgEAV + tc.msg
			values := map[string]*types.Item{":v": nItem(tc.in)}
			nested := map[string]*types.Item{":v": list(&types.Item{M: map[string]*types.Item{"a": nItem(tc.in)}})}

			_, err := table.Put(&types.PutItemInput{
				TableName: &table.Name, Item: numKey("1"),
				ConditionExpression: new("x = :v"), ExpressionAttributeValues: values,
			})
			requireNumberError(t, err, want)

			_, err = table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: numKey("1"),
				UpdateExpression: "SET x = :v", ExpressionAttributeValues: values,
			})
			requireNumberError(t, err, want)

			_, err = table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: numKey("1"),
				UpdateExpression: "SET x = :v", ExpressionAttributeValues: nested,
			})
			requireNumberError(t, err, want)

			_, err = table.Delete(&types.DeleteItemInput{
				TableName: &table.Name, Key: numKey("1"),
				ConditionExpression: new("x = :v"), ExpressionAttributeValues: values,
			})
			requireNumberError(t, err, want)

			_, err = table.SearchData(QueryInput{Scan: true, FilterExpression: "x = :v", ExpressionAttributeValues: values})
			requireNumberError(t, err, want)

			_, err = table.SearchData(QueryInput{
				KeyConditionExpression:    "pk = :p AND sk = :v",
				ExpressionAttributeValues: map[string]*types.Item{":p": {S: new("a")}, ":v": nItem(tc.in)},
			})
			requireNumberError(t, err, want)

			_, err = table.SearchData(QueryInput{
				KeyConditionExpression:    "pk = :p AND sk BETWEEN :a AND :v",
				ExpressionAttributeValues: map[string]*types.Item{":p": {S: new("a")}, ":a": nItem("0"), ":v": nItem(tc.in)},
			})
			requireNumberError(t, err, want)
		})
	}

	t.Run("valid values", func(t *testing.T) {
		t.Parallel()

		table := numValidationTable()

		for _, in := range validNumbers {
			_, err := table.SearchData(QueryInput{
				Scan: true, FilterExpression: "x = :v",
				ExpressionAttributeValues: map[string]*types.Item{":v": nItem(in)},
			})
			require.NoError(t, err, in)
		}
	})
}

func TestSearchData_validatesStartKeyNumbers(t *testing.T) {
	t.Parallel()

	table := numValidationTable()

	for _, tc := range invalidNumbers {
		_, err := table.SearchData(QueryInput{Scan: true, ExclusiveStartKey: numKey(tc.in)})
		requireNumberError(t, err, tc.msg)
	}
}

func TestValidateBatchWrite_validatesNumbers(t *testing.T) {
	t.Parallel()

	table := numValidationTable()
	lookup := func(string) (*Table, bool) { return table, true }

	for _, tc := range invalidNumbers {
		err := ValidateBatchWrite(lookup, map[string][]WriteRequest{tableName: {{Put: numItem(nItem(tc.in))}}})
		requireNumberError(t, err, tc.msg)

		err = ValidateBatchWrite(lookup, map[string][]WriteRequest{tableName: {{Delete: numKey(tc.in)}}})
		requireNumberError(t, err, tc.msg)
	}

	err := ValidateBatchWrite(lookup, map[string][]WriteRequest{tableName: {{Put: numItem(nItem(digits38))}}})
	require.NoError(t, err)
}

// DynamoDB Local reports an invalid number before the other problems of the request. Where a
// request has several, the order of its checks is pinned by e2e/parity_number_validation_test.go.
func TestNumbers_reportedBeforeOtherItemProblems(t *testing.T) {
	t.Parallel()

	table := numValidationTable()
	bad := nItem("abc")

	t.Run("put: empty key", func(t *testing.T) {
		t.Parallel()

		item := numItem(bad)
		item["pk"] = emptyS()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: item})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: missing key", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: map[string]*types.Item{"sk": nItem("1"), "x": bad}})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: key of the wrong type", func(t *testing.T) {
		t.Parallel()

		item := numItem(bad)
		item["pk"] = nItem("1")

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: item})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: undeclared name", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(bad), ConditionExpression: new("attribute_exists(#n)")})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: syntax error", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(bad), ConditionExpression: new("x = =")})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: ReturnValuesOnConditionCheckFailure", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(bad), ReturnValuesOnConditionCheckFailure: new("BOGUS")})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: ReturnValues comes first", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(bad), ReturnValues: new("UPDATED_NEW")})
		require.ErrorContains(t, err, "Return values set to invalid value")
	})

	t.Run("put: the item before the values of the condition", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{
			TableName: &table.Name, Item: numItem(nItem("1e126")), ConditionExpression: new("x = :v"),
			ExpressionAttributeValues: map[string]*types.Item{":v": bad},
		})
		requireNumberError(t, err, msgOverflow)
	})

	t.Run("put: ReturnValuesOnConditionCheckFailure before the values of the condition", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{
			TableName: &table.Name, Item: numKey("1"), ConditionExpression: new("x = :v"),
			ExpressionAttributeValues:           map[string]*types.Item{":v": bad},
			ReturnValuesOnConditionCheckFailure: new("BOGUS"),
		})
		require.ErrorContains(t, err, "returnValuesOnConditionCheckFailure")
	})

	t.Run("put: a duplicate after the number in a set is not reported", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(ns("abc", "1", "1"))})
		requireNumberError(t, err, msgInvalidNumber)
	})

	t.Run("put: a duplicate before the number in a set", func(t *testing.T) {
		t.Parallel()

		_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: numItem(ns("1", "1", "abc"))})
		require.ErrorContains(t, err, "contains duplicates")
	})
}

func TestUpdateDelete_numbersAfterTheKey(t *testing.T) {
	t.Parallel()

	table := numValidationTable()
	bad := map[string]*types.Item{":v": nItem("1e126")}
	wantEAV := msgEAV + msgOverflow

	cases := []struct {
		name string
		key  map[string]*types.Item
		want string
	}{
		{"empty key", map[string]*types.Item{"pk": emptyS(), "sk": nItem("1")}, emptyStringKeyMsg + " Key: pk"},
		{"missing key attribute", map[string]*types.Item{"pk": {S: new("a")}}, wrongKeyCountMsg},
		{"key of the wrong type", map[string]*types.Item{"pk": nItem("1"), "sk": nItem("1")}, "Type mismatch for key"},
		{"invalid key number before the shape", map[string]*types.Item{"pk": emptyS(), "sk": nItem("abc")}, msgInvalidNumber},
		{"invalid key number before the values", map[string]*types.Item{"pk": {S: new("a")}, "sk": nItem("abc")}, msgInvalidNumber},
	}

	for _, tc := range cases {
		t.Run("update/"+tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: tc.key, UpdateExpression: "SET x = :v", ExpressionAttributeValues: bad,
			})
			require.ErrorContains(t, err, tc.want)
		})

		t.Run("delete/"+tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := table.Delete(&types.DeleteItemInput{
				TableName: &table.Name, Key: tc.key, ConditionExpression: new("x = :v"), ExpressionAttributeValues: bad,
			})
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("update: values before undeclared names, syntax and undefined values", func(t *testing.T) {
		t.Parallel()

		for _, expr := range []string{"SET #n = :v", "SET x = = :v", "SET x = :v, y = :nope"} {
			_, err := table.Update(&types.UpdateItemInput{
				TableName: &table.Name, Key: numKey("1"), UpdateExpression: expr, ExpressionAttributeValues: bad,
			})
			requireNumberError(t, err, wantEAV)
		}
	})

	t.Run("update: values before ReturnValuesOnConditionCheckFailure", func(t *testing.T) {
		t.Parallel()

		_, err := table.Update(&types.UpdateItemInput{
			TableName: &table.Name, Key: numKey("1"), UpdateExpression: "SET x = :v", ExpressionAttributeValues: bad,
			ReturnValuesOnConditionCheckFailure: new("BOGUS"),
		})
		requireNumberError(t, err, wantEAV)
	})

	t.Run("update: ReturnValues comes first", func(t *testing.T) {
		t.Parallel()

		_, err := table.Update(&types.UpdateItemInput{
			TableName: &table.Name, Key: numKey("1"), UpdateExpression: "SET x = :v", ExpressionAttributeValues: bad,
			ReturnValues: new("BOGUS"),
		})
		require.ErrorContains(t, err, "Return values set to invalid value")
	})

	t.Run("delete: ReturnValuesOnConditionCheckFailure comes first", func(t *testing.T) {
		t.Parallel()

		_, err := table.Delete(&types.DeleteItemInput{
			TableName: &table.Name, Key: numKey("1"), ConditionExpression: new("x = :v"), ExpressionAttributeValues: bad,
			ReturnValuesOnConditionCheckFailure: new("BOGUS"),
		})
		require.ErrorContains(t, err, "returnValuesOnConditionCheckFailure")
	})
}

func TestSearchData_numbersBeforeExpressions(t *testing.T) {
	t.Parallel()

	table := numValidationTable()
	bad := map[string]*types.Item{":v": nItem("abc")}
	want := msgEAV + msgInvalidNumber

	for name, in := range map[string]QueryInput{
		"undeclared name": {Scan: true, FilterExpression: "#n = :v", ExpressionAttributeValues: bad},
		"select":          {Scan: true, Select: "BOGUS", FilterExpression: "x = :v", ExpressionAttributeValues: bad},
		"key condition": {
			KeyConditionExpression: "pk > :p", FilterExpression: "x = :v",
			ExpressionAttributeValues: map[string]*types.Item{":p": {S: new("a")}, ":v": nItem("abc")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := table.SearchData(in)
			requireNumberError(t, err, want)
		})
	}

	t.Run("the values before the start key", func(t *testing.T) {
		t.Parallel()

		_, err := table.SearchData(QueryInput{
			Scan: true, FilterExpression: "x = :v", ExpressionAttributeValues: map[string]*types.Item{":v": nItem("1e126")},
			ExclusiveStartKey: numKey("abc"),
		})
		requireNumberError(t, err, msgEAV+msgOverflow)
	})
}

// A batch write reports an invalid number anywhere in it before any other problem.
func TestValidateBatchWrite_numbersFirst(t *testing.T) {
	t.Parallel()

	table := numValidationTable()
	lookup := func(string) (*Table, bool) { return table, true }

	emptyKey := numKey("2")
	emptyKey["pk"] = emptyS()

	err := ValidateBatchWrite(lookup, map[string][]WriteRequest{tableName: {{Put: emptyKey}, {Put: numItem(nItem("abc"))}}})
	requireNumberError(t, err, msgInvalidNumber)

	err = ValidateBatchWrite(lookup, map[string][]WriteRequest{tableName: {{Put: emptyKey}}})
	require.ErrorContains(t, err, emptyStringKeyMsg)
}

// A transaction reports an invalid number in any of its operations before any other problem,
// the values of an operation's expression before its item or key.
func TestValidateWriteValues(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateWriteValues(nil))
	require.NoError(t, ValidateWriteValues([]WriteValues{{Item: numKey("1")}, {Expression: map[string]*types.Item{":v": nItem("1")}}}))

	err := ValidateWriteValues([]WriteValues{
		{Item: numKey("1")},
		{Expression: map[string]*types.Item{":v": nItem("abc")}, Item: numItem(nItem("1e126"))},
	})
	requireNumberError(t, err, msgEAV+msgInvalidNumber)

	err = ValidateWriteValues([]WriteValues{
		{Item: numItem(nItem("1e126")), Expression: map[string]*types.Item{":v": nItem("1")}},
		{Expression: map[string]*types.Item{":v": nItem("abc")}},
	})
	requireNumberError(t, err, msgOverflow)

	err = ValidateWriteValues([]WriteValues{{Item: numKey(digits39)}})
	requireNumberError(t, err, msgPrecision)
}
