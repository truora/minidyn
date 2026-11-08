package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestConditionalCheckFailedItemIsACopy(t *testing.T) {
	key := map[string]*types.Item{"id": {S: new("025")}}

	ops := map[string]func(table *Table) error{
		"Put": func(table *Table) error {
			_, err := table.Put(&types.PutItemInput{
				TableName:                           &table.Name,
				Item:                                map[string]*types.Item{"id": {S: new("025")}},
				ConditionExpression:                 new("attribute_not_exists(id)"),
				ReturnValuesOnConditionCheckFailure: new("ALL_OLD"),
			})

			return err
		},
		"Delete": func(table *Table) error {
			_, err := table.Delete(&types.DeleteItemInput{
				TableName:                           &table.Name,
				Key:                                 key,
				ConditionExpression:                 new("attribute_not_exists(id)"),
				ReturnValuesOnConditionCheckFailure: new("ALL_OLD"),
			})

			return err
		},
		"Update": func(table *Table) error {
			_, err := table.Update(&types.UpdateItemInput{
				TableName:                           &table.Name,
				Key:                                 key,
				UpdateExpression:                    "SET #n = :n",
				ConditionExpression:                 new("attribute_not_exists(id)"),
				ExpressionAttributeNames:            map[string]string{"#n": "name"},
				ExpressionAttributeValues:           map[string]*types.Item{":n": {S: new("x")}},
				ReturnValuesOnConditionCheckFailure: new("ALL_OLD"),
			})

			return err
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			table := NewTable(tableName)
			table.AttributesDef = map[string]string{"id": "S"}
			table.KeySchema = keySchema{HashKey: "id"}

			_, err := table.Put(&types.PutItemInput{
				TableName: &table.Name,
				Item:      map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Pikachu")}},
			})
			require.NoError(t, err)

			var ccf *types.ConditionalCheckFailedException

			require.ErrorAs(t, op(table), &ccf)
			require.Len(t, ccf.Item, 2)

			ccf.Item["name"].S = new("Mutated")
			ccf.Item["extra"] = &types.Item{S: new("x")}

			// Stored keys are opaque: derive the key through the key schema.
			storedKey, err := table.KeySchema.GetKey(table.AttributesDef, map[string]*types.Item{"id": {S: new("025")}})
			require.NoError(t, err)

			stored := table.Data[storedKey]
			require.Len(t, stored, 2)
			require.Equal(t, "Pikachu", *stored["name"].S)
		})
	}
}

func TestConditionalCheckFailedRule(t *testing.T) {
	stored := map[string]*types.Item{"id": {S: new("025")}}

	got := ConditionalCheckFailed(new("ALL_OLD"), stored)
	require.Equal(t, ErrConditionalRequestFailed.Error(), got.MessageText)
	require.Equal(t, stored, got.Item)

	got.Item["id"].S = new("mutated")
	require.Equal(t, "025", *stored["id"].S, "the returned item must be a copy")

	require.Nil(t, ConditionalCheckFailed(new("ALL_OLD"), map[string]*types.Item{}).Item, "an empty item has nothing to return")
	require.Nil(t, ConditionalCheckFailed(new("ALL_OLD"), nil).Item)
	require.Nil(t, ConditionalCheckFailed(new("NONE"), stored).Item)
	require.Nil(t, ConditionalCheckFailed(nil, stored).Item)
}
