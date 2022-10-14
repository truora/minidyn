package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

var tableName = "pokemons"

type pokemon struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	SecondType string   `json:"second_type"`
	Name       string   `json:"name"`
	Level      int64    `json:"lvl"`
	Moves      []string `json:"moves" dynamodbav:"moves,stringset,omitempty"`
	Local      []string `json:"local"`
}

func createPokemon(creature pokemon) map[string]types.Item {
	item := map[string]types.Item{
		"id":   {S: creature.ID},
		"type": {S: creature.Type},
		"name": {S: creature.Name},
	}

	return item
}

func createPokemonTable() (*Table, error) {
	table := NewTable(tableName)

	table.AttributesDef = map[string]string{"id": "S", "name": "S"}
	table.KeySchema = keySchema{"id", "name", false}
	invertStr, allStr := "invert", "ALL"
	globalSecondaryIndexes := []*types.GlobalSecondaryIndex{
		{
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  1,
				WriteCapacityUnits: 1,
			},
			IndexName: &invertStr,
			KeySchema: []*types.KeySchemaElement{
				{
					AttributeName: "id",
					KeyType:       "HASH",
				},
				{
					AttributeName: "name",
					KeyType:       "RANGE",
				},
			},
			Projection: &types.Projection{
				ProjectionType: &allStr,
			},
		},
	}

	globalSecondaryIndexes[0].ProvisionedThroughput = &types.ProvisionedThroughput{
		ReadCapacityUnits:  1,
		WriteCapacityUnits: 1,
	}

	err := table.AddGlobalIndexes(globalSecondaryIndexes)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func TestCreateTableIndexes(t *testing.T) {
	c := require.New(t)

	newTable := NewTable(tableName)
	newTable.AttributesDef = map[string]string{"partition": "S", "range": "S"}
	c.Equal(newTable.Name, tableName)

	invertStr, allStr := "invert", "ALL"
	globalSecondaryIndexes := []*types.GlobalSecondaryIndex{
		{
			IndexName: &invertStr,
			KeySchema: []*types.KeySchemaElement{
				{
					AttributeName: "range",
					KeyType:       "HASH",
				},
				{
					AttributeName: "no_defined",
					KeyType:       "RANGE",
				},
			},
			Projection: &types.Projection{
				ProjectionType: &allStr,
			},
		},
	}

	err := newTable.AddGlobalIndexes(globalSecondaryIndexes)
	c.Contains(err.Error(), "No provisioned throughput specified for the global secondary index")

	globalSecondaryIndexes[0].ProvisionedThroughput = &types.ProvisionedThroughput{
		ReadCapacityUnits:  1,
		WriteCapacityUnits: 1,
	}

	err = newTable.AddGlobalIndexes(globalSecondaryIndexes)
	c.Contains(err.Error(), "Global Secondary Index range key not specified in Attribute Definitions")

	globalSecondaryIndexes[0].KeySchema[1].AttributeName = "partition"

	err = newTable.AddGlobalIndexes(globalSecondaryIndexes)
	c.NoError(err)
}

func TestDescriptionTable(t *testing.T) {
	c := require.New(t)

	newTable := NewTable(tableName)
	newTable.AttributesDef = map[string]string{"partition": "S", "range": "S"}
	newTable.KeySchema = keySchema{"range", "HAS", false}

	invertStr, allStr := "invert", "ALL"
	globalSecondaryIndexes := []*types.GlobalSecondaryIndex{
		{
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  1,
				WriteCapacityUnits: 1,
			},
			IndexName: &invertStr,
			KeySchema: []*types.KeySchemaElement{
				{
					AttributeName: "range",
					KeyType:       "HASH",
				},
				{
					AttributeName: "partition",
					KeyType:       "RANGE",
				},
			},
			Projection: &types.Projection{
				ProjectionType: &allStr,
			},
		},
	}

	err := newTable.AddGlobalIndexes(globalSecondaryIndexes)
	c.NoError(err)

	d := newTable.Description(tableName)
	c.Equal(d.TableName, d.TableName)
}

func TestGetKey(t *testing.T) {
	c := require.New(t)

	newTable := NewTable(tableName)
	newTable.AttributesDef = map[string]string{"HASH": "S", "range": "S"}
	newTable.KeySchema = keySchema{"range", "HASH", false}

	k, err := newTable.KeySchema.GetKey(map[string]string{"HASH": "S", "range": "S"}, map[string]types.Item{"range": {S: "range"}, "HASH": {S: "HASH"}})
	c.NoError(err)
	c.Equal("range.HASH", k)

	_, err = newTable.KeySchema.GetKey(map[string]string{"incorrect": "S", "range": "S"}, map[string]types.Item{"range": {S: "range"}, "HASH": {S: "HASH"}})
	c.EqualError(err, `invalid attribute value type; field "HASH"`)

	_, err = newTable.KeySchema.GetKey(map[string]string{"HASH": "S", "": "S"}, map[string]types.Item{"range": {S: "range"}, "HASH": {S: "HASH"}})
	c.EqualError(err, `invalid attribute value type; field "range"`)

	newTable.KeySchema = keySchema{"", "", true}
	_, err = newTable.KeySchema.GetKey(map[string]string{"incorrect": "S", "range": "S"}, map[string]types.Item{"range": {S: "range"}, "HASH": {S: "HASH"}})
	c.NoError(err)
}

func TestGetKeys(t *testing.T) {
	c := require.New(t)

	val := GetKeyAt([]string{"gk"}, 0, 0, true)
	c.Equal(val, "gk")

	val = GetKeyAt([]string{"gk"}, 1, 0, false)
	c.Equal(val, "gk")

	val = GetKeyAt([]string{"gk"}, 0, 0, true)
	c.Equal(val, "gk")

	val = GetKeyAt([]string{"gk"}, 1, 0, false)
	c.Equal(val, "gk")
}

func TestDeleteItem(t *testing.T) {
	c := require.New(t)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	newTable, err := createPokemonTable()
	c.NoError(err)

	input := &types.PutItemInput{
		Item:      item,
		TableName: &newTable.Name,
	}

	_, err = newTable.Put(input)
	c.NoError(err)

	item = createPokemon(pokemon{
		ID:   "002",
		Type: "grass",
		Name: "Ivysaur",
	})
	input.Item = item
	_, err = newTable.Put(input)
	c.NoError(err)

	item = createPokemon(pokemon{
		ID:   "003",
		Type: "grass",
		Name: "Venusaur",
	})
	input.Item = item
	_, err = newTable.Put(input)
	c.NoError(err)
	c.Len(newTable.Data, 3)

	inp := &types.DeleteItemInput{
		Key: map[string]types.Item{
			"id":   {S: "002"},
			"name": {S: "Ivysaur"}},
		TableName: &newTable.Name,
	}

	_, err = newTable.Delete(inp)
	c.NoError(err)
	c.Len(newTable.Data, 2)

	_, err = newTable.Delete(inp)
	c.NoError(err)
	c.Len(newTable.Data, 2)

	inp = &types.DeleteItemInput{
		Key: map[string]types.Item{
			"id": {S: "123"}},
		TableName: &newTable.Name,
	}

	_, err = newTable.Delete(inp)
	c.EqualError(err, `ValidationException: number of conditions on the keys is invalid; field: "name"`)
}

func TestPutItem(t *testing.T) {
	c := require.New(t)

	newTable := NewTable(tableName)

	item := map[string]types.Item{
		"id":        {S: "123"},
		"name":      {S: "Lili"},
		"last_name": {S: "Cruz"},
	}

	input := &types.PutItemInput{
		Item:      item,
		TableName: &newTable.Name,
	}

	_, err := newTable.Put(input)
	c.EqualError(err, `ValidationException: number of conditions on the keys is invalid; field: ""`)

	newTable.AttributesDef = map[string]string{"id": "S", "name": "S"}
	newTable.KeySchema = keySchema{"id", "name", false}

	condExp := "attribute_not_exists(#id)"
	input.ConditionExpression = &condExp
	input.ExpressionAttributeNames = map[string]string{
		"#id": "id",
	}

	input.ExpressionAttributeValues = map[string]types.Item{
		":id": {S: "456"},
	}

	_, err = newTable.Put(input)
	c.NoError(err)
}
