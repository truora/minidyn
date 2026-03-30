package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

const tableName = "pokemons"

type pokemon struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	SecondType string   `json:"second_type"`
	Name       string   `json:"name"`
	Level      int64    `json:"lvl"`
	Moves      []string `json:"moves" dynamodbav:"moves,stringset,omitempty"`
	Local      []string `json:"local"`
}

func ensurePokemonTable(client dynamodbiface.DynamoDBAPI) error {
	err := AddTable(client, tableName, "id", "")

	var aerr awserr.Error
	ok := errors.As(err, &aerr)

	if !ok || aerr.Code() != dynamodb.ErrCodeResourceInUseException {
		return err
	}

	return nil
}

func ensurePokemonTypeIndex(client dynamodbiface.DynamoDBAPI) error {
	err := AddIndex(client, tableName, "by-type", "type", "id")

	var aerr awserr.Error
	ok := errors.As(err, &aerr)

	if !ok || aerr.Code() != "ValidationException" {
		return err
	}

	return nil
}

func createPokemon(client dynamodbiface.DynamoDBAPI, creature pokemon) error {
	item, err := dynamodbattribute.MarshalMap(creature)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: new(tableName),
	}

	_, err = client.PutItemWithContext(context.Background(), input)

	return err
}

func getPokemon(client dynamodbiface.DynamoDBAPI, id string) (map[string]*dynamodb.AttributeValue, error) {
	key := map[string]*dynamodb.AttributeValue{
		"id": {
			S: new(id),
		},
	}

	getInput := &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key:       key,
	}

	out, err := client.GetItemWithContext(context.Background(), getInput)

	return out.Item, err
}

func getPokemonsByType(client dynamodbiface.DynamoDBAPI, typ string) ([]map[string]*dynamodb.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":type": {
				S: new(typ),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#type": new("type"),
		},
		KeyConditionExpression: new("#type = :type"),
		TableName:              new(tableName),
		IndexName:              new("by-type"),
	}

	items := []map[string]*dynamodb.AttributeValue{}

	out, err := client.QueryWithContext(context.Background(), input)
	if err == nil {
		items = out.Items
	}

	return items, err
}

func setupClient(table string) dynamodbiface.DynamoDBAPI {
	dynamodbEndpoint := os.Getenv("LOCAL_DYNAMODB_ENDPOINT")
	if dynamodbEndpoint != "" {
		return setupDynamoDBLocal(dynamodbEndpoint)
	}

	client := NewClient()

	return client
}

func setupDynamoDBLocal(endpoint string) dynamodbiface.DynamoDBAPI {
	creds := credentials.NewStaticCredentials("dummy", "dummy", "dummy")

	config := &aws.Config{
		Credentials: creds,
		Region:      new("us-east-1"),
	}

	// this allow us to test with dynamodb-local
	config.Endpoint = new(endpoint)
	config.MaxRetries = new(1)

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("settings dynamodb-local tables failed:", err)
		}
	}()

	client := dynamodb.New(session.Must(session.NewSession(config)))

	return client
}

func setupNativeInterpreter(native *interpreter.Native, table string) {
	native.AddUpdater(table, "SET second_type = :ntype", func(item map[string]*types.Item, updates map[string]*types.Item) {
		item["second_type"] = updates[":ntype"]
	})

	native.AddUpdater(table, "SET #type = :ntype", func(item map[string]*types.Item, updates map[string]*types.Item) {
		item["type"] = updates[":ntype"]
	})
}

func getData(client dynamodbiface.DynamoDBAPI, tn, p, r string) (map[string]*dynamodb.AttributeValue, error) {
	getInput := &dynamodb.GetItemInput{
		TableName: new(tn),
		Key: map[string]*dynamodb.AttributeValue{
			"partition": {
				S: new(p),
			},
			"range": {
				S: new(r),
			},
		},
	}

	out, err := client.GetItemWithContext(context.Background(), getInput)

	return out.Item, err
}

func getDataInIndex(client dynamodbiface.DynamoDBAPI, index, tn, p, r string) ([]map[string]*dynamodb.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":data": {
				S: new(p),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#data": new("data"),
		},
		KeyConditionExpression: new("#data = :data"),
		TableName:              new(tn),
		IndexName:              new(index),
	}

	items := []map[string]*dynamodb.AttributeValue{}

	out, err := client.QueryWithContext(context.Background(), input)
	if err == nil {
		items = out.Items
	}

	return items, err
}

func TestSetInterpreter(t *testing.T) {
	c := require.New(t)
	client := NewClient()

	c.NotPanics(func() {
		_, err := client.getTable("tests")
		c.Error(err)
	})

	err := AddTable(client, "tests", "hk", "rk")
	c.NoError(err)

	native := interpreter.NewNativeInterpreter()
	client.SetInterpreter(native)

	_, err = client.getTable("tests")
	c.NoError(err)
}

func TestActivateDebug(t *testing.T) {
	c := require.New(t)
	fake := NewClient()

	fake.ActivateDebug()

	c.True(fake.langInterpreter.Debug)
}

func TestCreateTable(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: new("partition"),
				AttributeType: new("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: new("partition"),
				KeyType:       new("HASH"),
			},
			{
				AttributeName: new("range"),
				KeyType:       new("RANGE"),
			},
		},
		TableName: new(tableName),
	}

	_, err := client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "Range Key not specified in Attribute Definitions")

	input.AttributeDefinitions = append(input.AttributeDefinitions, &dynamodb.AttributeDefinition{
		AttributeName: new("range"),
		AttributeType: new("S"),
	})

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "No provisioned throughput specified for the table")

	input.BillingMode = new("PAY_PER_REQUEST")

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.NoError(err)

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "Cannot create preexisting table")
}

func TestCreateTableWithGSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: new("partition"),
				AttributeType: new("S"),
			},
			{
				AttributeName: new("range"),
				AttributeType: new("S"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: new("partition"),
				KeyType:       new("HASH"),
			},
			{
				AttributeName: new("range"),
				KeyType:       new("RANGE"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{},
		TableName:              new(tableName + "-gsi"),
	}

	_, err := client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "GSI list is empty/invalid")

	input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, &dynamodb.GlobalSecondaryIndex{
		IndexName: new("invert"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: new("range"),
				KeyType:       new("HASH"),
			},
			{
				AttributeName: new("no_defined"),
				KeyType:       new("RANGE"),
			},
		},
		Projection: &dynamodb.Projection{
			ProjectionType: new("ALL"),
		},
	})

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "No provisioned throughput specified for the global secondary index")

	input.GlobalSecondaryIndexes[0].ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(1),
		WriteCapacityUnits: aws.Int64(1),
	}

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "Global Secondary Index Range Key not specified in Attribute Definitions")

	input.GlobalSecondaryIndexes[0].KeySchema[1].AttributeName = new("partition")

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.NoError(err)
}

func TestCreateTableWithLSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: new("partition"),
				AttributeType: new("S"),
			},
			{
				AttributeName: new("range"),
				AttributeType: new("S"),
			},
			{
				AttributeName: new("data"),
				AttributeType: new("S"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: new("partition"),
				KeyType:       new("HASH"),
			},
			{
				AttributeName: new("range"),
				KeyType:       new("RANGE"),
			},
		},
		LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{},
		TableName:             new(tableName + "-lsi"),
	}

	_, err := client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "LSI list is empty/invalid")

	input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, &dynamodb.LocalSecondaryIndex{
		IndexName: new("data"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: new("partition"),
				KeyType:       new("HASH"),
			},
			{
				AttributeName: new("no_defined"),
				KeyType:       new("RANGE"),
			},
		},
		Projection: &dynamodb.Projection{
			ProjectionType: new("ALL"),
		},
	})

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.Contains(err.Error(), "Local Secondary Index Range Key not specified in Attribute Definitions")

	input.LocalSecondaryIndexes[0].KeySchema[1].AttributeName = new("data")

	_, err = client.CreateTableWithContext(context.Background(), input)
	c.NoError(err)
}

func TestDeleteTable(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.DeleteTableInput{
		TableName: new("table-404"),
	}

	_, err := client.DeleteTableWithContext(context.Background(), input)
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	input = &dynamodb.DeleteTableInput{
		TableName: new(tableName),
	}
	out, err := client.DeleteTableWithContext(context.Background(), input)
	c.NoError(err)

	c.NotEmpty(out)

	_, err = client.DeleteTableWithContext(context.Background(), input)
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())
}

func TestUpdateTable(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.UpdateTableInput{
		BillingMode:                 new("PROVISIONED"),
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{},
		TableName:                   new("404"),
	}

	_, err := client.UpdateTableWithContext(context.Background(), input)
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	input = &dynamodb.UpdateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: new("id"),
				AttributeType: new("S"),
			},
			{
				AttributeName: new("type"),
				AttributeType: new("S"),
			},
		},
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName: new("newIndex"),
					KeySchema: []*dynamodb.KeySchemaElement{
						{
							AttributeName: new("type"),
							KeyType:       new("HASH"),
						},
						{
							AttributeName: new("id"),
							KeyType:       new("RANGE"),
						},
					},
					Projection: &dynamodb.Projection{
						ProjectionType: new("ALL"),
					},
				},
			},
		},
		TableName: new(tableName),
	}
	output, err := client.UpdateTableWithContext(context.Background(), input)
	c.NoError(err)

	c.Len(output.TableDescription.GlobalSecondaryIndexes, 1)

	input = &dynamodb.UpdateTableInput{
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			{
				Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
					IndexName: new("newIndex"),
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(1),
						WriteCapacityUnits: aws.Int64(1),
					},
				},
			},
		},
		TableName: new(tableName),
	}
	_, err = client.UpdateTableWithContext(context.Background(), input)
	c.NoError(err)

	input = &dynamodb.UpdateTableInput{
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			{
				Delete: &dynamodb.DeleteGlobalSecondaryIndexAction{
					IndexName: new("newIndex"),
				},
			},
		},
		TableName: new(tableName),
	}
	_, err = client.UpdateTableWithContext(context.Background(), input)
	c.NoError(err)

	_, err = client.UpdateTableWithContext(context.Background(), input)
	c.Equal("ResourceNotFoundException: Requested resource not found", err.Error())
}

func TestPutAndGetItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	item, err := dynamodbattribute.MarshalMap(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: new(tableName),
	}

	_, err = client.PutItemWithContext(context.Background(), input)
	c.NoError(err)

	getInput := &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("001"),
			},
		},
	}
	out, err := client.GetItemWithContext(context.Background(), getInput)
	c.NoError(err)
	c.Equal("001", *out.Item["id"].S)
	c.Equal("Bulbasaur", *out.Item["name"].S)
	c.Equal("grass", *out.Item["type"].S)

	_, err = client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key:       map[string]*dynamodb.AttributeValue{},
	})

	c.Error(err)
	c.Contains(err.Error(), "number of conditions on the keys is invalid")
}

func TestPutWithGSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	item, err := dynamodbattribute.MarshalMap(pokemon{
		ID:   "001",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: new(tableName),
	}

	_, err = client.PutItemWithContext(context.Background(), input)
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
	c.Contains(err.Error(), "value type")

	delete(item, "type")

	_, err = client.PutItemWithContext(context.Background(), input)
	c.NoError(err)

	_ = AddIndex(client, tableName, "sort-by-second-type", "id", "second_type")

	item, err = dynamodbattribute.MarshalMap(pokemon{
		ID:   "002",
		Name: "Ivysaur",
		Type: "grass",
	})
	c.NoError(err)

	input.Item = item

	_, err = client.PutItemWithContext(context.Background(), input)
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
	c.Contains(err.Error(), "value type")
}

func TestGetItemWithUnusedAttributes(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	input := &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("001"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#name": new("name"),
		},
	}

	_, err = client.GetItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)
}

func TestGetItemWithInvalidExpressionAttributeNames(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	input := &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("001"),
			},
		},
		ProjectionExpression: new("#name-1"),
		ExpressionAttributeNames: map[string]*string{
			"#name-1": new("name"),
		},
	}

	_, err = client.GetItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)
}

func TestGetItemWithProjectionExpression(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	c.NoError(err)

	out, err := client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {S: new("001")},
		},
		ProjectionExpression: new("#n"),
		ExpressionAttributeNames: map[string]*string{
			"#n": new("name"),
		},
	})
	c.NoError(err)
	c.Len(out.Item, 1)

	c.Equal("Bulbasaur", *out.Item["name"].S)
}

func TestGetItemWithProjectionInvalidExpression(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	c.NoError(err)

	_, err = client.GetItemWithContext(context.Background(), &dynamodb.GetItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {S: new("001")},
		},
		ProjectionExpression: new("("),
	})
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
}

func TestPutItemWithConditions(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)
	err := ensurePokemonTable(client)
	c.NoError(err)

	item, err := dynamodbattribute.MarshalMap(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.PutItemInput{
		Item:                item,
		TableName:           new(tableName),
		ConditionExpression: new("attribute_not_exists(#type)"),
		ExpressionAttributeNames: map[string]*string{
			"#type": new("type"),
		},
	}

	_, err = client.PutItemWithContext(context.Background(), input)
	c.NoError(err)

	_, err = client.PutItemWithContext(context.Background(), input)
	c.Error(err)

	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":not_used": {NULL: new(true)},
	}

	_, err = client.PutItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeValuesMsg)

	input.ConditionExpression = new("attribute_not_exists(#invalid-name)")

	input.ExpressionAttributeNames = map[string]*string{
		"#invalid-name": new("hello"),
	}

	_, err = client.PutItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.ConditionExpression = new("#valid_name = :invalid-value")

	input.ExpressionAttributeNames = map[string]*string{
		"#valid_name": new("hello"),
	}

	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":invalid-value": {NULL: new(true)},
	}

	_, err = client.PutItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)
}

func TestUpdateItemWithContext(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	expr := map[string]*dynamodb.AttributeValue{
		":ntype": {
			S: new(string("poison")),
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("001"),
			},
		},
		ReturnValues:              new("UPDATED_NEW"),
		UpdateExpression:          new("SET second_type = :ntype"),
		ExpressionAttributeValues: expr,
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.NoError(err)

	item, err := getPokemon(client, "001")
	c.NoError(err)
	c.Equal("poison", aws.StringValue(item["second_type"].S))

	input.Key["id"] = &dynamodb.AttributeValue{
		S: new("404"),
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.NoError(err)

	item, err = getPokemon(client, "404")
	c.NoError(err)
	c.Equal("poison", aws.StringValue(item["second_type"].S))

	minidynClient, ok := client.(*Client)
	if !ok {
		return
	}

	setupNativeInterpreter(minidynClient.GetNativeInterpreter(), tableName)
	minidynClient.ActivateNativeInterpreter()

	input.Key = map[string]*dynamodb.AttributeValue{
		"id": {
			S: new("001"),
		},
	}
	expr[":ntype"].S = new(string("water"))

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.NoError(err)

	item, err = getPokemon(client, "001")
	c.NoError(err)
	c.Equal("water", aws.StringValue(item["second_type"].S))
}

func TestUpdateItemReturnValuesMatrix(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	c.NoError(ensurePokemonTable(client))

	putAndUpdate := func(pk string, rv *string) map[string]*dynamodb.AttributeValue {
		t.Helper()

		_, err := client.PutItemWithContext(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]*dynamodb.AttributeValue{
				"id":   {S: aws.String(pk)},
				"type": {S: aws.String("grass")},
				"name": {S: aws.String("Bulbasaur")},
			},
		})
		c.NoError(err)

		out, err := client.UpdateItemWithContext(context.Background(), &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]*dynamodb.AttributeValue{
				"id": {S: aws.String(pk)},
			},
			UpdateExpression: aws.String("SET second_type = :st"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":st": {S: aws.String("poison")},
			},
			ReturnValues: rv,
		})
		c.NoError(err)

		return out.Attributes
	}

	c.Nil(putAndUpdate("v1-urv-none", aws.String("NONE")))

	allOld := putAndUpdate("v1-urv-all-old", aws.String("ALL_OLD"))
	c.Len(allOld, 3)
	c.Equal("Bulbasaur", aws.StringValue(allOld["name"].S))
	c.Equal("grass", aws.StringValue(allOld["type"].S))

	updatedOld := putAndUpdate("v1-urv-upd-old", aws.String("UPDATED_OLD"))
	c.Empty(updatedOld)

	updatedNew := putAndUpdate("v1-urv-upd-new", aws.String("UPDATED_NEW"))
	c.Len(updatedNew, 1)
	c.Equal("poison", aws.StringValue(updatedNew["second_type"].S))

	allNew := putAndUpdate("v1-urv-all-new", aws.String("ALL_NEW"))
	c.Len(allNew, 4)
	c.Equal("poison", aws.StringValue(allNew["second_type"].S))
}

func TestUpdateItemWithConditionalExpression(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "002",
		Type: "grass",
		Name: "Ivysaur",
	})
	c.NoError(err)

	uexpr := "SET second_type = :ntype"
	expr := map[string]*dynamodb.AttributeValue{
		":ntyp": {
			S: new(string("poison")),
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("404"),
			},
		},
		ConditionExpression:       new("attribute_exists(id)"),
		ReturnValues:              new("UPDATED_NEW"),
		UpdateExpression:          new(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]*string{
			"#id": new("id"),
		},
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	input.ConditionExpression = new("attribute_exists(#invalid-name)")

	input.ExpressionAttributeNames = map[string]*string{
		"#invalid-name": new("type"),
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.ConditionExpression = new("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]*string{
		"#t": new("type"),
	}

	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":invalid-value": {NULL: new(true)},
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)

	input = &dynamodb.UpdateItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("404"),
			},
		},
		ConditionExpression:       new("attribute_exists(#id)"),
		ReturnValues:              new("UPDATED_NEW"),
		UpdateExpression:          new(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]*string{
			"#id": new("id"),
		},
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.Contains(err.Error(), dynamodb.ErrCodeConditionalCheckFailedException)
}

func TestUpdateItemWithGSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	c.NoError(err)

	items, err := getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Len(items, 1)

	uexpr := "SET #type = :ntype"
	expr := map[string]*dynamodb.AttributeValue{
		":ntype": {
			S: new(string("poison")),
		},
	}
	names := map[string]*string{"#type": new("type")}

	input := &dynamodb.UpdateItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("001"),
			},
		},
		ReturnValues:              new("UPDATED_NEW"),
		UpdateExpression:          new(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames:  names,
	}

	_, err = client.UpdateItem(input)
	c.NoError(err)

	items, err = getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Empty(items)

	items, err = getPokemonsByType(client, "poison")
	c.NoError(err)
	c.Len(items, 1)
}

func TestUpdateItemError(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	c.NoError(err)

	expr := map[string]*dynamodb.AttributeValue{
		":second_type": {
			S: new(string("poison")),
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"foo": {
				S: new("a"),
			},
		},
		ReturnValues:              new("UPDATED_NEW"),
		UpdateExpression:          new("SET second_type = :second_type"),
		ExpressionAttributeValues: expr,
	}

	_, err = client.UpdateItem(input)
	c.Contains(err.Error(), "One of the required keys was not given a value")

	ActiveForceFailure(client)
	defer DeactiveForceFailure(client)

	output, err := client.UpdateItemWithContext(context.Background(), input)
	c.EqualError(err, "forced failure response")
	c.Nil(output)
}

func TestUpdateExpressions(t *testing.T) {
	c := require.New(t)
	db := []pokemon{
		{
			ID:    "001",
			Type:  "grass",
			Name:  "Bulbasaur",
			Moves: []string{"Growl", "Tackle", "Vine Whip", "Growth"},
			Local: []string{"001 (Red/Blue/Yellow)", "226 (Gold/Silver/Crystal)", "001 (FireRed/LeafGreen)", "001 (Let's Go Pikachu/Let's Go Eevee)"},
		},
	}

	tests := map[string]struct {
		input  *dynamodb.UpdateItemInput
		verify func(tc *testing.T, client dynamodbiface.DynamoDBAPI)
	}{
		"add": {
			input: &dynamodb.UpdateItemInput{
				TableName: new(tableName),
				Key: map[string]*dynamodb.AttributeValue{
					"id": {
						S: new("001"),
					},
				},
				ReturnValues:              new("UPDATED_NEW"),
				UpdateExpression:          new("ADD lvl :one"),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":one": {N: new("1")}},
			},
			verify: func(tc *testing.T, client dynamodbiface.DynamoDBAPI) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.Equal("1", *item["lvl"].N)
			},
		},
		"remove": {
			input: &dynamodb.UpdateItemInput{
				TableName: new(tableName),
				Key: map[string]*dynamodb.AttributeValue{
					"id": {
						S: new("001"),
					},
				},
				ReturnValues:     new("UPDATED_NEW"),
				UpdateExpression: new("REMOVE #l[0],#l[1],#l[2],#l[3]"),
				ExpressionAttributeNames: map[string]*string{
					"#l": new("local"),
				},
			},
			verify: func(tc *testing.T, client dynamodbiface.DynamoDBAPI) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.Empty(item["local"].L)
			},
		},
		"delete": {
			input: &dynamodb.UpdateItemInput{
				TableName: new(tableName),
				Key: map[string]*dynamodb.AttributeValue{
					"id": {
						S: new("001"),
					},
				},
				ReturnValues:     new("UPDATED_NEW"),
				UpdateExpression: new("DELETE moves :move"),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":move": {
						SS: []*string{new("Growl")},
					},
				},
			},
			verify: func(tc *testing.T, client dynamodbiface.DynamoDBAPI) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.Len(item["moves"].SS, 3)
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(tc *testing.T) {
			a := assert.New(tc)
			client := setupClient(tableName)

			err := ensurePokemonTable(client)
			a.NoError(err)

			for _, item := range db {
				err = createPokemon(client, item)
				c.NoError(err)
			}

			_, err = client.UpdateItemWithContext(context.Background(), tt.input)
			a.NoError(err)

			tt.verify(tc, client)
		})
	}
}

func TestQueryWithContext(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "004",
		Type: "fire",
		Name: "Charmander",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "007",
		Type: "water",
		Name: "Squirtle",
	})
	c.NoError(err)

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": {
				S: new("004"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#id": new("id"),
		},
		KeyConditionExpression: new("#id = :id"),
		TableName:              new(tableName),
	}

	out, err := client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Empty(out.LastEvaluatedKey)

	pokemonQueried := pokemon{}

	err = dynamodbattribute.UnmarshalMap(out.Items[0], &pokemonQueried)
	c.NoError(err)
	c.Equal(pokemonQueried.ID, "004")
	c.Equal(pokemonQueried.Type, "fire")
	c.Equal(pokemonQueried.Name, "Charmander")

	input.FilterExpression = new("#type = :type")

	input.ExpressionAttributeNames["#type"] = new("type")
	input.ExpressionAttributeValues[":type"] = &dynamodb.AttributeValue{
		S: new("fire"),
	}

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)

	input.ExpressionAttributeValues[":type"] = &dynamodb.AttributeValue{
		S: new("grass"),
	}

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Empty(out.Items)

	input.ExpressionAttributeNames["#not_used"] = new("hello")

	_, err = client.QueryWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	input.KeyConditionExpression = new("#invalid-name = :id")
	input.ExpressionAttributeNames = map[string]*string{
		"#invalid-name": new("id"),
	}

	_, err = client.QueryWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.KeyConditionExpression = new("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]*string{
		"#t": new("type"),
	}

	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":invalid-value": {NULL: new(true)},
	}

	_, err = client.QueryWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)

	minidynClient, ok := client.(*Client)
	if !ok {
		return
	}

	setupNativeInterpreter(minidynClient.GetNativeInterpreter(), tableName)
	minidynClient.ActivateNativeInterpreter()

	input = &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": {
				S: new("004"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#id": new("id"),
		},
		KeyConditionExpression: new("#id = :id"),
		TableName:              new(tableName),
	}

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Empty(out.LastEvaluatedKey)
}

func TestQueryWithContextPagination(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "002",
		Type: "grass",
		Name: "Ivysaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "003",
		Type: "grass",
		Name: "Venusaur",
	})
	c.NoError(err)

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":type": {
				S: new("grass"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#type": new("type"),
		},
		KeyConditionExpression: new("#type = :type"),
		TableName:              new(tableName),
		IndexName:              new("by-type"),
		Limit:                  aws.Int64(1),
	}

	out, err := client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal("001", aws.StringValue(out.Items[0]["id"].S))

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal("002", aws.StringValue(out.Items[0]["id"].S))

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal("003", aws.StringValue(out.Items[0]["id"].S))

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Empty(out.Items)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = nil
	input.ExclusiveStartKey = nil

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = aws.Int64(4)
	input.ExclusiveStartKey = nil

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = aws.Int64(2)
	input.ExclusiveStartKey = nil

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 2)
	c.NotEmpty(out.LastEvaluatedKey)
	input.ExclusiveStartKey = out.LastEvaluatedKey

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = aws.Int64(4)
	input.ExclusiveStartKey = nil

	err = createPokemon(client, pokemon{
		ID:   "004",
		Type: "fire",
		Name: "Charmander",
	})
	c.NoError(err)

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)
	c.Empty(out.LastEvaluatedKey)

	input.ScanIndexForward = new(false)

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Equal("003", aws.StringValue(out.Items[0]["id"].S))

	// Query with FilterExpression
	input.ScanIndexForward = nil
	input.ExclusiveStartKey = nil
	input.FilterExpression = new("begins_with(#name, :letter)")
	input.Limit = aws.Int64(2)
	input.ExpressionAttributeValues[":letter"] = &dynamodb.AttributeValue{
		S: new("V"),
	}
	input.ExpressionAttributeNames["#name"] = new("name")

	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Empty(out.Items)
	c.NotEmpty(out.LastEvaluatedKey)

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.QueryWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal("003", aws.StringValue(out.Items[0]["id"].S))
}

func TestQuerySyntaxError(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":partition": {
				S: new("a"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#partition": new("partition"),
		},
		// Syntax Error
		KeyConditionExpression: new("#partition != :partition"),
		TableName:              new(tableName),
	}

	_, err = client.QueryWithContext(context.Background(), input)
	c.Error(err)
	var aerr awserr.Error
	c.True(errors.As(err, &aerr))
	c.Equal("ValidationException", aerr.Code())
}

func TestScanWithContext(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "002",
		Type: "grass",
		Name: "Ivysaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "003",
		Type: "grass",
		Name: "Venusaur",
	})
	c.NoError(err)

	input := &dynamodb.ScanInput{
		TableName: new(tableName),
	}

	out, err := client.ScanWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)

	input.Limit = aws.Int64(1)
	out, err = client.ScanWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.NotEmpty(out.LastEvaluatedKey)

	input.FilterExpression = new("#invalid-name = Raichu")
	input.ExpressionAttributeNames = map[string]*string{
		"#invalid-name": new("Name"),
	}

	_, err = client.ScanWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.FilterExpression = new("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]*string{
		"#t": new("type"),
	}

	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":invalid-value": {NULL: new(true)},
	}

	_, err = client.ScanWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)

	input.Limit = nil
	input.FilterExpression = new("#name = :name")
	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":name": {
			S: new("Venusaur"),
		},
	}
	input.ExpressionAttributeNames = map[string]*string{
		"#name": new("name"),
	}

	out, err = client.ScanWithContext(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)

	input.ExpressionAttributeNames["#not_used"] = new("hello")

	_, err = client.ScanWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	delete(input.ExpressionAttributeNames, "#not_used")

	if fclient, isFake := client.(*Client); isFake {
		ActiveForceFailure(fclient)

		out, err = client.ScanWithContext(context.Background(), input)
		c.Equal(ErrForcedFailure, err)
		c.Empty(out)

		DeactiveForceFailure(fclient)
	}
}

func TestDeleteItemWithContext(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "002",
		Type: "grass",
		Name: "Ivysaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "003",
		Type: "grass",
		Name: "Venusaur",
	})
	c.NoError(err)

	key := map[string]*dynamodb.AttributeValue{
		"id": {S: new("003")},
	}

	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: new(tableName),
	}

	items, err := getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Len(items, 3)

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.NoError(err)

	item, err := getPokemon(client, "003")
	c.NoError(err)

	c.Empty(item)

	items, err = getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Len(items, 2)

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.NoError(err)

	if _, ok := client.(*Client); ok {
		EmulateFailure(client, FailureConditionInternalServerError)

		defer func() { EmulateFailure(client, FailureConditionNone) }()

		output, forcedError := client.DeleteItemWithContext(context.Background(), input)
		c.Nil(output)

		var aerr awserr.Error
		ok := errors.As(forcedError, &aerr)

		c.True(ok)
		c.Equal(dynamodb.ErrCodeInternalServerError, aerr.Code())
	}
}

func TestDeleteItemWithConditions(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {S: new("001")},
		},
		TableName:           new(tableName),
		ConditionExpression: new("attribute_exists(id)"),
	}

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.NoError(err)

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.Contains(err.Error(), dynamodb.ErrCodeConditionalCheckFailedException)

	input.ExpressionAttributeNames = map[string]*string{
		"#not_used": new("hello"),
	}

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	input.ConditionExpression = new("#invalid-name = Squirtle")

	input.ExpressionAttributeNames = map[string]*string{
		"#invalid-name": new("hello"),
	}

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.ConditionExpression = new("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]*string{
		"#t": new("type"),
	}

	input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
		":invalid-value": {NULL: new(true)},
	}

	_, err = client.DeleteItemWithContext(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)
}

func TestDeleteItemWithContextWithReturnValues(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {S: new("001")},
		},
		TableName:    new(tableName),
		ReturnValues: new("ALL_OLD"),
	}

	output, err := client.DeleteItemWithContext(context.Background(), input)
	c.NoError(err)

	c.Equal("Bulbasaur", *output.Attributes["name"].S)
}

func TestDescribeTable(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: new(tableName),
	}

	output, err := client.DescribeTableWithContext(aws.BackgroundContext(), describeTableInput)
	c.NoError(err)
	c.NotNil(output)
	c.Len(output.Table.KeySchema, 1)
	c.Equal(aws.StringValue(output.Table.TableName), tableName)
	c.Equal(aws.StringValue(output.Table.KeySchema[0].KeyType), "HASH")
	c.Equal(aws.StringValue(output.Table.KeySchema[0].AttributeName), "id")
}

func TestDescribeTableFail(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	expected := "ResourceNotFoundException: Cannot do operations on a non-existent table"
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: new("non_existing"),
	}

	output, err := client.DescribeTableWithContext(aws.BackgroundContext(), describeTableInput)
	c.Error(err)
	c.Equal(expected, err.Error())
	c.Empty(output)
}

func TestBatchWriteItemWithContext(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	m := pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	}

	item, err := dynamodbattribute.MarshalMap(m)
	c.NoError(err)

	requests := []*dynamodb.WriteRequest{
		{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		},
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: requests,
		},
	}

	itemCollectionMetrics := map[string][]*dynamodb.ItemCollectionMetrics{
		"table": {
			{
				ItemCollectionKey: map[string]*dynamodb.AttributeValue{
					"report_id": {
						S: new("1234"),
					},
				},
			},
		},
	}

	SetItemCollectionMetrics(client, itemCollectionMetrics)

	output, err := client.BatchWriteItemWithContext(context.Background(), input)
	c.NoError(err)

	c.Equal(itemCollectionMetrics, output.ItemCollectionMetrics)

	c.NotEmpty(getPokemon(client, "001"))

	_, err = client.BatchWriteItemWithContext(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: {
				{
					DeleteRequest: &dynamodb.DeleteRequest{Key: map[string]*dynamodb.AttributeValue{
						"id": {S: new("001")},
					}},
				},
			},
		},
	})
	c.NoError(err)

	c.Empty(getPokemon(client, "001"))

	delete(item, "id")

	_, err = client.BatchWriteItemWithContext(context.Background(), input)
	c.Contains(err.Error(), "One of the required keys was not given a value")

	_, err = client.BatchWriteItemWithContext(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: {
				{
					DeleteRequest: &dynamodb.DeleteRequest{Key: map[string]*dynamodb.AttributeValue{
						"id":   {S: new("001")},
						"type": {S: new("grass")},
					}},
					PutRequest: &dynamodb.PutRequest{Item: item},
				},
			},
		},
	})

	c.Contains(err.Error(), "ValidationException: Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")

	_, err = client.BatchWriteItemWithContext(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: {
				{},
			},
		},
	})
	c.Contains(err.Error(), "ValidationException: Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")

	item, err = dynamodbattribute.MarshalMap(m)
	c.NoError(err)

	for range batchRequestsLimit {
		requests = append(requests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: item},
		})
	}

	_, err = client.BatchWriteItemWithContext(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: requests,
		},
	})
	c.Contains(err.Error(), "ValidationException: Too many items requested for the BatchWriteItem call")
}

func TestBatchWriteItemWithFailingDatabase(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	m := pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	}

	item, err := dynamodbattribute.MarshalMap(m)
	c.NoError(err)

	requests := []*dynamodb.WriteRequest{
		{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		},
	}

	EmulateFailure(client, FailureConditionInternalServerError)
	defer EmulateFailure(client, FailureConditionNone)

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: requests,
		},
	}

	output, err := client.BatchWriteItemWithContext(context.Background(), input)
	c.NoError(err)

	c.NotEmpty(output.UnprocessedItems)
}

func TestTransactWriteItemsWithContext(t *testing.T) {
	c := require.New(t)
	client := NewClient()

	err := ensurePokemonTable(client)
	c.NoError(err)

	transactItems := []*dynamodb.TransactWriteItem{
		{
			Update: &dynamodb.Update{
				Key: map[string]*dynamodb.AttributeValue{
					"id":     {S: new("001")},
					":ntype": {S: new("poison")},
				},
				TableName:        new(tableName),
				UpdateExpression: new("SET second_type = :ntype"),
				ExpressionAttributeNames: map[string]*string{
					"#id": new("id"),
				},
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":update": {S: new(time.Now().Format(time.RFC3339))},
					":incr": {
						N: new("1"),
					},
					":initial": {
						N: new("0"),
					},
				},
			},
		},
	}

	writeItemsInput := &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	output, err := client.TransactWriteItemsWithContext(context.Background(), writeItemsInput)
	c.NoError(err)
	c.NotNil(output)

	ActiveForceFailure(client)
	defer DeactiveForceFailure(client)

	_, err = client.TransactWriteItemsWithContext(context.Background(), writeItemsInput)
	c.Equal(ErrForcedFailure, err)
}

func TestCheckTableName(t *testing.T) {
	c := require.New(t)

	fclient := NewClient()

	_, err := fclient.getTable(tableName)
	expected := "ResourceNotFoundException: Cannot do operations on a non-existent table"
	c.Equal(expected, err.Error())

	err = AddTable(fclient, "new-table", "partition", "range")
	c.NoError(err)

	_, err = fclient.getTable("notATable")
	c.Equal(expected, err.Error())
}

func TestForceFailure(t *testing.T) {
	c := require.New(t)

	client := NewClient()

	err := ensurePokemonTable(client)
	c.NoError(err)

	ActiveForceFailure(client)
	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.Equal(ErrForcedFailure, err)

	DeactiveForceFailure(client)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)
	c.NoError(err)

	actualClient := dynamodb.New(session.Must(session.NewSession(&aws.Config{})))

	c.Panics(func() {
		ActiveForceFailure(actualClient)
	})
	c.Panics(func() {
		DeactiveForceFailure(actualClient)
	})
	c.Panics(func() {
		SetItemCollectionMetrics(actualClient, nil)
	})
}

func TestUpdateItemAndQueryAfterUpsert(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	expr := map[string]*dynamodb.AttributeValue{
		":type": {
			S: new(string("water")),
		},
		":name": {
			S: new(string("piplup")),
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: new(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: new("001"),
			},
		},
		ReturnValues:              new("UPDATED_NEW"),
		UpdateExpression:          new("SET #type = :type, #name = :name"),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]*string{
			"#type": new("type"),
			"#name": new("name"),
		},
	}

	_, err = client.UpdateItemWithContext(context.Background(), input)
	c.NoError(err)

	items, err := getPokemonsByType(client, "water")
	c.NoError(err)

	c.Len(items, 1)
}

func BenchmarkQuery(b *testing.B) {
	c := require.New(b)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "002",
		Type: "grass",
		Name: "Ivysaur",
	})
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "003",
		Type: "grass",
		Name: "Venusaur",
	})
	c.NoError(err)

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":type": {
				S: new("grass"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#type": new("type"),
		},
		KeyConditionExpression: new("#type = :type"),
		TableName:              new(tableName),
		IndexName:              new("by-type"),
		Limit:                  aws.Int64(1),
	}

	for n := 0; n < b.N; n++ {
		out, err := client.QueryWithContext(context.Background(), input)
		c.NoError(err)
		c.Len(out.Items, 1)
	}
}

func BenchmarkQueryWithContext(b *testing.B) {
	c := require.New(b)
	client := NewClient()

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)

	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": {
				S: new("001"),
			},
		},
		KeyConditionExpression:   new("#id = :id"),
		ExpressionAttributeNames: map[string]*string{"#id": new("id")},
		TableName:                new(tableName),
		Limit:                    aws.Int64(1),
	}

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		_, err := client.QueryWithContext(ctx, input)
		if err != nil {
			b.Error(err)
		}
	}
}
