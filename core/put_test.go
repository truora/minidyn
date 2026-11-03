package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestPutReturnValues(t *testing.T) {
	c := require.New(t)

	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"id": "S"}
	table.KeySchema = keySchema{HashKey: "id"}

	put := func(name string, rv *string) map[string]*types.Item {
		old, err := table.Put(&types.PutItemInput{
			TableName:    &table.Name,
			Item:         map[string]*types.Item{"id": {S: new("025")}, "name": {S: new(name)}},
			ReturnValues: rv,
		})
		c.NoError(err)

		return old
	}

	c.Empty(put("Pichu", new("ALL_OLD")))
	c.Empty(put("Pikachu", nil))
	c.Empty(put("Pikachu", new("NONE")))
	c.Equal(map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Pikachu")}}, put("Raichu", new("ALL_OLD")))

	old, err := table.Put(&types.PutItemInput{
		TableName:    &table.Name,
		Item:         map[string]*types.Item{"name": {S: new("Missing")}},
		ReturnValues: new("ALL_OLD"),
	})
	c.Error(err)
	c.Nil(old)
}

func TestPutInvalidReturnValues(t *testing.T) {
	c := require.New(t)

	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"id": "S"}
	table.KeySchema = keySchema{HashKey: "id"}

	for _, rv := range []string{"ALL_NEW", "UPDATED_OLD", "UPDATED_NEW", "BOGUS"} {
		old, err := table.Put(&types.PutItemInput{
			TableName:    &table.Name,
			Item:         map[string]*types.Item{"id": {S: new("025")}},
			ReturnValues: new(rv),
		})
		c.Error(err, rv)
		c.Nil(old)
		c.Contains(err.Error(), "Return values set to invalid value")
		c.Empty(table.Data)
	}
}

func TestPutConditionFailedReturnsNoAttributes(t *testing.T) {
	c := require.New(t)

	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"id": "S"}
	table.KeySchema = keySchema{HashKey: "id"}

	_, err := table.Put(&types.PutItemInput{
		TableName: &table.Name,
		Item:      map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Pikachu")}},
	})
	c.NoError(err)

	old, err := table.Put(&types.PutItemInput{
		TableName:           &table.Name,
		Item:                map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Raichu")}},
		ConditionExpression: new("attribute_not_exists(id)"),
		ReturnValues:        new("ALL_OLD"),
	})
	c.Error(err)
	c.Nil(old)
}
