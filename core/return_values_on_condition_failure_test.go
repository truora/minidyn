package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestPutDeleteReturnValuesOnConditionCheckFailure(t *testing.T) {
	existing := map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Pikachu")}}
	key := map[string]*types.Item{"id": {S: new("025")}}

	failedItem := func(t *testing.T, err error) (map[string]*types.Item, bool) {
		t.Helper()

		var ccf *types.ConditionalCheckFailedException

		require.True(t, errors.As(err, &ccf), "%v", err)
		require.Contains(t, ccf.Error(), "The conditional request failed")

		return ccf.Item, ccf.Item != nil
	}

	ops := map[string]func(table *Table, mode *string, condition string) error{
		"Put": func(table *Table, mode *string, condition string) error {
			_, err := table.Put(&types.PutItemInput{
				TableName:                           &table.Name,
				Item:                                map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Raichu")}},
				ConditionExpression:                 &condition,
				ReturnValuesOnConditionCheckFailure: mode,
			})

			return err
		},
		"Delete": func(table *Table, mode *string, condition string) error {
			_, err := table.Delete(&types.DeleteItemInput{
				TableName:                           &table.Name,
				Key:                                 key,
				ConditionExpression:                 &condition,
				ReturnValuesOnConditionCheckFailure: mode,
			})

			return err
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			newTable := func(seed bool) *Table {
				table := NewTable(tableName)
				table.AttributesDef = map[string]string{"id": "S"}
				table.KeySchema = keySchema{HashKey: "id"}

				if seed {
					_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: existing})
					require.NoError(t, err)
				}

				return table
			}

			item, ok := failedItem(t, op(newTable(true), new("ALL_OLD"), "attribute_not_exists(id)"))
			require.True(t, ok)
			require.Equal(t, existing, item)

			for _, mode := range []*string{nil, new(""), new("NONE")} {
				_, ok = failedItem(t, op(newTable(true), mode, "attribute_not_exists(id)"))
				require.False(t, ok)
			}

			_, ok = failedItem(t, op(newTable(false), new("ALL_OLD"), "attribute_exists(id)"))
			require.False(t, ok, "a missing item has nothing to return")
		})
	}
}

func TestValidateReturnValuesOnConditionCheckFailure(t *testing.T) {
	for _, rv := range []*string{nil, new(""), new("NONE"), new("ALL_OLD")} {
		require.NoError(t, ValidateReturnValuesOnConditionCheckFailure(rv))
	}

	err := ValidateReturnValuesOnConditionCheckFailure(new("BOGUS"))

	var tErr types.Error

	require.True(t, errors.As(err, &tErr))
	require.Equal(t, "ValidationException", tErr.Code())
	require.Equal(t,
		"1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
		tErr.Message())
}

func TestValidateNoneOrAllOldReturnValues(t *testing.T) {
	for _, rv := range []*string{nil, new(""), new("NONE"), new("ALL_OLD")} {
		require.NoError(t, ValidateNoneOrAllOldReturnValues(rv))
	}

	err := ValidateNoneOrAllOldReturnValues(new("BOGUS"))

	var tErr types.Error

	require.True(t, errors.As(err, &tErr))
	require.Equal(t, "ValidationException", tErr.Code())
	require.Equal(t,
		"Return values set to invalid value"+types.Detail("value", "BOGUS", "field", "ReturnValues"),
		tErr.Message())
}

func TestValidateTransactItemReturnValuesOnConditionCheckFailure(t *testing.T) {
	for _, rv := range []*string{nil, new(""), new("NONE"), new("ALL_OLD")} {
		require.NoError(t, ValidateTransactItemReturnValuesOnConditionCheckFailure(0, "put", rv))
	}

	err := ValidateTransactItemReturnValuesOnConditionCheckFailure(1, "conditionCheck", new("BOGUS"))

	var tErr types.Error

	require.True(t, errors.As(err, &tErr))
	require.Equal(t, "ValidationException", tErr.Code())
	require.Equal(t,
		"1 validation error detected: Value 'BOGUS' at 'transactItems.2.member.conditionCheck.returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
		tErr.Message())
}

// Put, Update and Delete validate the mode themselves, so core never runs with a bad one.
func TestWriteOperationsRejectInvalidReturnValuesOnConditionCheckFailure(t *testing.T) {
	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"id": "S"}
	table.KeySchema = keySchema{HashKey: "id"}
	key := map[string]*types.Item{"id": {S: new("025")}}
	bogus := new("BOGUS")

	_, err := table.Put(&types.PutItemInput{TableName: &table.Name, Item: key, ReturnValuesOnConditionCheckFailure: bogus})
	require.ErrorContains(t, err, "at 'returnValuesOnConditionCheckFailure'")

	_, err = table.Delete(&types.DeleteItemInput{TableName: &table.Name, Key: key, ReturnValuesOnConditionCheckFailure: bogus})
	require.ErrorContains(t, err, "at 'returnValuesOnConditionCheckFailure'")

	_, err = table.Update(&types.UpdateItemInput{
		TableName: &table.Name, Key: key, UpdateExpression: "REMOVE x", ReturnValuesOnConditionCheckFailure: bogus,
	})
	require.ErrorContains(t, err, "at 'returnValuesOnConditionCheckFailure'")
}

// The ReturnValues enums: absent or empty means the default; values are case sensitive.
func TestReturnValuesEnums(t *testing.T) {
	for _, v := range []string{"", "NONE", "ALL_OLD"} {
		require.True(t, isNoneOrAllOld(v), v)
		require.True(t, isUpdateReturnValue(v), v)
	}

	for _, v := range []string{"UPDATED_OLD", "UPDATED_NEW", "ALL_NEW"} {
		require.False(t, isNoneOrAllOld(v), v)
		require.True(t, isUpdateReturnValue(v), v)
	}

	for _, v := range []string{"all_old", "BOGUS", " NONE"} {
		require.False(t, isNoneOrAllOld(v), v)
		require.False(t, isUpdateReturnValue(v), v)
	}
}
