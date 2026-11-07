package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func newDeleteTestTable(t *testing.T) *Table {
	t.Helper()

	table := NewTable(tableName)
	table.AttributesDef = map[string]string{"id": "S"}
	table.KeySchema = keySchema{HashKey: "id"}

	_, err := table.Put(&types.PutItemInput{
		TableName: &table.Name,
		Item:      map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Pikachu")}},
	})
	require.NoError(t, err)

	return table
}

func TestDeleteReturnValues(t *testing.T) {
	c := require.New(t)

	key := map[string]*types.Item{"id": {S: new("025")}}
	item := map[string]*types.Item{"id": {S: new("025")}, "name": {S: new("Pikachu")}}

	for _, rv := range []*string{nil, new(""), new("NONE")} {
		table := newDeleteTestTable(t)

		old, err := table.Delete(&types.DeleteItemInput{TableName: &table.Name, Key: key, ReturnValues: rv})
		c.NoError(err)
		c.Empty(old)
		c.Empty(table.Data)
	}

	table := newDeleteTestTable(t)

	old, err := table.Delete(&types.DeleteItemInput{TableName: &table.Name, Key: key, ReturnValues: new("ALL_OLD")})
	c.NoError(err)
	c.Equal(item, old)
	c.Empty(table.Data)

	old, err = table.Delete(&types.DeleteItemInput{TableName: &table.Name, Key: key, ReturnValues: new("ALL_OLD")})
	c.NoError(err)
	c.Empty(old)
}

func TestDeleteInvalidReturnValues(t *testing.T) {
	c := require.New(t)

	for _, rv := range []string{"ALL_NEW", "UPDATED_OLD", "UPDATED_NEW", "BOGUS"} {
		table := newDeleteTestTable(t)

		old, err := table.Delete(&types.DeleteItemInput{
			TableName:    &table.Name,
			Key:          map[string]*types.Item{"id": {S: new("025")}},
			ReturnValues: new(rv),
		})
		c.Error(err, rv)
		c.Nil(old)
		c.Contains(err.Error(), "Return values set to invalid value")
		c.Len(table.Data, 1)
	}
}

func TestDeleteInvalidReturnValuesWinsOverOtherFailures(t *testing.T) {
	c := require.New(t)
	table := newDeleteTestTable(t)

	// An invalid key and a failing condition must not mask the return values error.
	_, err := table.Delete(&types.DeleteItemInput{
		TableName:    &table.Name,
		Key:          map[string]*types.Item{"nope": {S: new("025")}},
		ReturnValues: new("ALL_NEW"),
	})
	c.ErrorContains(err, "Return values set to invalid value")

	_, err = table.Delete(&types.DeleteItemInput{
		TableName:                 &table.Name,
		Key:                       map[string]*types.Item{"id": {S: new("025")}},
		ConditionExpression:       new("#n = :n"),
		ExpressionAttributeNames:  map[string]*string{"#n": new("name")},
		ExpressionAttributeValues: map[string]*types.Item{":n": {S: new("Raichu")}},
		ReturnValues:              new("ALL_NEW"),
	})
	c.ErrorContains(err, "Return values set to invalid value")
	c.Len(table.Data, 1)
}
