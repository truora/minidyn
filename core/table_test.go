package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/interpreter"
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

func createPokemon(creature pokemon) map[string]*types.Item {
	item := map[string]*types.Item{
		"id":   {S: types.ToString(creature.ID)},
		"type": {S: types.ToString(creature.Type)},
		"name": {S: types.ToString(creature.Name)},
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

func createGlobalSecondaryIndex() *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{
		Create: &types.CreateGlobalSecondaryIndexAction{},
		Delete: &types.DeleteGlobalSecondaryIndexAction{},
		Update: &types.UpdateGlobalSecondaryIndexAction{},
	}
}

func createLocalSecondaryIndex() []*types.LocalSecondaryIndex {
	return []*types.LocalSecondaryIndex{
		{
			IndexName: types.ToString("indexname"),
		},
	}
}

func TestAddLocalIndexes(t *testing.T) {
	c := require.New(t)

	localIndexes := createLocalSecondaryIndex()
	newTable := NewTable(tableName)

	err := newTable.AddLocalIndexes(localIndexes)
	c.Contains(err.Error(), " No Hash Key specified in schema.")

	localIndexes[0].KeySchema = []*types.KeySchemaElement{
		{
			AttributeName: "id",
			KeyType:       "HASH",
		},
	}

	err = newTable.AddLocalIndexes(localIndexes)
	c.Contains(err.Error(), "Local Secondary Index Hash Key not specified in Attribute Definitions.")

	newTable, err = createPokemonTable()
	c.NoError(err)

	err = newTable.AddLocalIndexes(localIndexes)
	c.NoError(err)

	err = newTable.AddLocalIndexes([]*types.LocalSecondaryIndex{})
	c.Contains(err.Error(), "ValidationException: LSI list is empty/invalid")
}

func TestApplyIndexChange(t *testing.T) {
	c := require.New(t)

	globalSecondaryIndex := createGlobalSecondaryIndex()
	newTable := NewTable(tableName)

	err := newTable.ApplyIndexChange(globalSecondaryIndex)
	c.Contains(err.Error(), "No provisioned throughput specified for the global secondary index")

	newTable.BillingMode = types.ToString("PAY_PER_REQUEST")
	err = newTable.ApplyIndexChange(globalSecondaryIndex)
	c.Contains(err.Error(), "No Hash Key specified in schema.")

	newTable, err = createPokemonTable()
	c.NoError(err)

	newTable.BillingMode = types.ToString("PAY_PER_REQUEST")
	createTableInput := &types.CreateTableInput{
		KeySchema: []*types.KeySchemaElement{
			{
				AttributeName: "id",
				KeyType:       "HASH",
			},
		},
	}

	err = newTable.CreatePrimaryIndex(createTableInput)
	c.NoError(err)

	globalSecondaryIndex.Create = &types.CreateGlobalSecondaryIndexAction{
		KeySchema: []*types.KeySchemaElement{
			{
				AttributeName: "id",
				KeyType:       "HASH",
			},
		},
		IndexName: types.ToString("indexname"),
	}

	err = newTable.ApplyIndexChange(globalSecondaryIndex)
	c.NoError(err)

	globalSecondaryIndex.Create = nil

	globalSecondaryIndex.Delete = &types.DeleteGlobalSecondaryIndexAction{
		IndexName: types.ToString("indexname"),
	}

	err = newTable.ApplyIndexChange(globalSecondaryIndex)
	c.NoError(err)

	newTable.Indexes = map[string]*index{}
	err = newTable.ApplyIndexChange(globalSecondaryIndex)
	c.Contains(err.Error(), "Requested resource not found")

	globalSecondaryIndex.Delete = nil

	globalSecondaryIndex.Update = &types.UpdateGlobalSecondaryIndexAction{
		IndexName: types.ToString("indexname"),
	}

	err = newTable.ApplyIndexChange(globalSecondaryIndex)
	c.NoError(err)

	globalSecondaryIndex.Update = nil

	err = newTable.ApplyIndexChange(globalSecondaryIndex)
	c.NoError(err)

	err = newTable.AddGlobalIndexes([]*types.GlobalSecondaryIndex{})
	c.Contains(err.Error(), "GSI list is empty/invalid")
}

func TestCreatePrimaryIndex(t *testing.T) {
	c := require.New(t)

	newTable := NewTable(tableName)
	createTableInput := &types.CreateTableInput{}

	err := newTable.CreatePrimaryIndex(createTableInput)
	c.Contains(err.Error(), "No Hash Key specified in schema.")

	createTableInput.KeySchema = []*types.KeySchemaElement{
		{
			AttributeName: "id",
			KeyType:       "HASH",
		},
	}

	err = newTable.CreatePrimaryIndex(createTableInput)
	c.Contains(err.Error(), "Hash Key not specified in Attribute Definitions")

	newTable, err = createPokemonTable()
	c.NoError(err)

	err = newTable.CreatePrimaryIndex(createTableInput)
	c.Contains(err.Error(), "No provisioned throughput specified for the table")

	newTable.BillingMode = types.ToString("PAY_PER_REQUEST")
	err = newTable.CreatePrimaryIndex(createTableInput)
	c.NoError(err)
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
	c.Contains(err.Error(), "Global Secondary Index Range Key not specified in Attribute Definitions")

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

	k, err := newTable.KeySchema.GetKey(map[string]string{"HASH": "S", "range": "S"}, map[string]*types.Item{"range": {S: types.ToString("range")}, "HASH": {S: types.ToString("HASH")}})
	c.NoError(err)
	c.Equal("range.HASH", k)

	_, err = newTable.KeySchema.GetKey(map[string]string{"incorrect": "S", "range": "S"}, map[string]*types.Item{"range": {S: types.ToString("range")}, "HASH": {S: types.ToString("HASH")}})
	c.EqualError(err, `invalid attribute value type; field "HASH"`)

	_, err = newTable.KeySchema.GetKey(map[string]string{"HASH": "S", "": "S"}, map[string]*types.Item{"range": {S: types.ToString("range")}, "HASH": {S: types.ToString("HASH")}})
	c.EqualError(err, `invalid attribute value type; field "range"`)

	newTable.KeySchema = keySchema{"", "", true}
	_, err = newTable.KeySchema.GetKey(map[string]string{"incorrect": "S", "range": "S"}, map[string]*types.Item{"range": {S: types.ToString("range")}, "HASH": {S: types.ToString("HASH")}})
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
		Key: map[string]*types.Item{
			"id":   {S: types.ToString("002")},
			"name": {S: types.ToString("Ivysaur")}},
		TableName: &newTable.Name,
	}

	_, err = newTable.Delete(inp)
	c.NoError(err)
	c.Len(newTable.Data, 2)

	_, err = newTable.Delete(inp)
	c.NoError(err)
	c.Len(newTable.Data, 2)

	inp = &types.DeleteItemInput{
		Key: map[string]*types.Item{
			"id": {S: types.ToString("123")}},
		TableName: &newTable.Name,
	}

	_, err = newTable.Delete(inp)
	c.EqualError(err, `ValidationException: number of conditions on the keys is invalid; field: "name"`)
}

func TestDeleteIndex(t *testing.T) {
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
		Key: map[string]*types.Item{
			"id":   {S: types.ToString("002")},
			"name": {S: types.ToString("Ivysaur")}},
		TableName: &newTable.Name,
	}

	index := newTable.Indexes["invert"]
	index.sortedKeys = []string{"key1", "key2"}
	newTable.Indexes["invert"] = index

	_, err = newTable.Delete(inp)
	c.NoError(err)
}

func TestSearchData(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	queryInput := QueryInput{}

	result, lastItem := newTable.SearchData(queryInput)
	c.Equal([]map[string]*types.Item{}, result)
	c.Equal(map[string]*types.Item{}, lastItem)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	input := &types.PutItemInput{
		Item:      item,
		TableName: &newTable.Name,
	}

	_, err = newTable.Put(input)
	c.NoError(err)

	queryInput = QueryInput{
		ExpressionAttributeValues: map[string]*types.Item{
			":id": {S: types.ToString("001")},
		},
	}

	result, lastItem = newTable.SearchData(queryInput)
	c.Equal([]map[string]*types.Item{}, result)
	c.Equal(map[string]*types.Item{}, lastItem)

	queryInput.Aliases = input.ExpressionAttributeNames
	queryInput.Limit = 1
	queryInput.ConditionExpression = input.ConditionExpression
	queryInput.ScanIndexForward = true

	newIndex := index{
		sortedRefs: [][2]string{{"ref1", "ref2"}},
		sortedKeys: []string{"ref2", "ref1"},
		refs:       map[string]string{"ref1": "ref2"},
		Table:      newTable,
	}

	newTable.Indexes = map[string]*index{
		"indice": &newIndex,
	}

	queryInput.Index = "indice"
	queryInput.started = true

	result, lastItem = newTable.SearchData(queryInput)
	c.Equal([]map[string]*types.Item{{}}, result)
	c.Equal(map[string]*types.Item{}, lastItem)

	queryInput.ExclusiveStartKey = item
	result, lastItem = newTable.SearchData(queryInput)
	c.Equal([]map[string]*types.Item{}, result)
	c.Equal(map[string]*types.Item{}, lastItem)

	newIndex.Clear()
}

func TestUpdate(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	input := &types.PutItemInput{
		Item:      item,
		TableName: &newTable.Name,
	}

	_, err = newTable.Put(input)
	c.NoError(err)

	updateInput := &types.UpdateItemInput{}
	_, err = newTable.Update(updateInput)
	c.Contains(err.Error(), "number of conditions on the keys is invalid;")

	updateInput = &types.UpdateItemInput{
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
		ExpressionAttributeValues: map[string]*types.Item{
			":id": {S: types.ToString("002")},
		},
		Key: item,
	}

	_, err = newTable.Update(updateInput)
	c.Contains(err.Error(), "invalid update expression")

	updateInput.ConditionExpression = types.ToString("attribute_not_exists(#id)")

	_, err = newTable.Update(updateInput)
	c.Contains(err.Error(), "conditional request failed")

	updateInput.ConditionExpression = types.ToString("attribute_exists(id)")
	updateInput.UpdateExpression = "SET id = :id"

	result, err := newTable.Update(updateInput)
	c.NoError(err)
	c.Equal("002", types.StringValue(result["id"].S))

	newTable.Clear()
}

func TestPutItem(t *testing.T) {
	c := require.New(t)

	newTable := NewTable(tableName)

	item := map[string]*types.Item{
		"id":        {S: types.ToString("123")},
		"name":      {S: types.ToString("Lili")},
		"last_name": {S: types.ToString("Cruz")},
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

	input.ExpressionAttributeValues = map[string]*types.Item{
		":id": {S: types.ToString("456")},
	}

	_, err = newTable.Put(input)
	c.NoError(err)
}

func TestGetItem(t *testing.T) {
	c := require.New(t)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	newTable, err := createPokemonTable()
	c.NoError(err)

	newTable.Data = map[string]map[string]*types.Item{
		"item": item,
	}

	resultItem := newTable.getItem("item")
	c.Equal(item, resultItem)
}

func TestGetLastKey(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	newIndex := &index{
		sortedRefs: [][2]string{{"ref1", "ref2"}},
		sortedKeys: []string{"ref2", "ref1"},
		refs:       map[string]string{"ref1": "ref2"},
		Table:      newTable,
	}

	result := newTable.getLastKey(item, "", 1, 1, 1, newIndex)
	c.Equal(item["id"], result["id"])
}

func TestInterpreterMatch(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	newTable.UseNativeInterpreter = true

	err = newTable.interpreterUpdate(interpreter.UpdateInput{})
	c.Contains(err.Error(), "unsupported expression or attribute type:")

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	newTable.UseNativeInterpreter = true
	matchInput := interpreter.MatchInput{
		TableName:  tableName,
		Expression: "#id = :id",
		Item:       item,
		Attributes: map[string]*types.Item{
			":id": {
				S: types.ToString("001"),
			},
		},
		ExpressionType: interpreter.ExpressionTypeConditional,
	}

	newTable.interpreterMatch(matchInput)

	matchInput = interpreter.MatchInput{
		TableName: tableName,
	}

	c.Panics(func() { newTable.interpreterMatch(matchInput) })

	newTable.UseNativeInterpreter = false
	matchInput.Expression = "bad_expression(id)"

	c.Panics(func() { newTable.interpreterMatch(matchInput) })
}

func TestMatchKey(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})

	queryInput := QueryInput{
		ExpressionAttributeValues: map[string]*types.Item{
			":id": {S: types.ToString("001")},
		},
		KeyConditionExpression: "#id = :id",
		FilterExpression:       "#id = :id",
		ConditionExpression:    types.ToString("attribute_exists(id)"),
	}

	expresionType, ok := newTable.matchKey(queryInput, item)
	c.True(ok)
	c.NotNil(expresionType)
}

func TestSetAttributeDefinition(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	newAttributesDef := []*types.AttributeDefinition{
		{
			AttributeName: types.ToString("name"),
			AttributeType: types.ToString("type"),
		},
	}

	newTable.SetAttributeDefinition(newAttributesDef)
	c.Equal(types.StringValue(newAttributesDef[0].AttributeType), newTable.AttributesDef["name"])
}

func TestFetchQueryData(t *testing.T) {
	c := require.New(t)

	newTable, err := createPokemonTable()
	c.NoError(err)

	item := createPokemon(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
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

	item = createPokemon(pokemon{
		ID:   "005",
		Type: "grass",
		Name: "Oddish",
	})
	input.Item = item
	_, err = newTable.Put(input)
	c.NoError(err)

	item = createPokemon(pokemon{
		ID:   "004",
		Type: "grass",
		Name: "Gloom",
	})
	input.Item = item
	_, err = newTable.Put(input)
	c.NoError(err)

	item = createPokemon(pokemon{
		ID:   "006",
		Type: "grass",
		Name: "Bellsprout",
	})
	input.Item = item
	_, err = newTable.Put(input)
	c.NoError(err)
	c.Len(newTable.Data, 6)

	index, sortedKeys := newTable.fetchQueryData(QueryInput{
		Index: "invert",
	})

	c.NotEmpty(sortedKeys)
	c.Equal(index.sortedRefs[0][0], "006.Bellsprout")
}
