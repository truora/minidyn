package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
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

func ensurePokemonTable(client FakeClient) error {
	err := AddTable(context.Background(), client, tableName, "id", "")

	var oe smithy.APIError
	var errResourceInUseException *dynamodbtypes.ResourceInUseException

	if !errors.As(err, &oe) || !errors.As(err, &errResourceInUseException) {
		return err
	}

	return nil
}

func ensurePokemonTypeIndex(client FakeClient) error {
	err := AddIndex(context.Background(), client, tableName, "by-type", "type", "id")

	var oe smithy.APIError

	if !errors.As(err, &oe) || oe.ErrorCode() != "ValidationException" {
		return err
	}

	return nil
}

func createPokemon(client FakeClient, creature pokemon) error {
	opt := func(opt *attributevalue.EncoderOptions) {
		opt.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(creature, opt)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tableName),
	}

	_, err = client.PutItem(context.Background(), input)

	return err
}

func getPokemon(client FakeClient, id string) (map[string]dynamodbtypes.AttributeValue, error) {
	key := map[string]dynamodbtypes.AttributeValue{
		"id": &dynamodbtypes.AttributeValueMemberS{
			Value: id,
		},
	}

	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	}

	out, err := client.GetItem(context.Background(), getInput)

	return out.Item, err
}

func getPokemonsByType(client FakeClient, typ string) ([]map[string]dynamodbtypes.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":type": &dynamodbtypes.AttributeValueMemberS{
				Value: typ,
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		KeyConditionExpression: aws.String("#type = :type"),
		TableName:              aws.String(tableName),
		IndexName:              aws.String("by-type"),
	}

	items := []map[string]dynamodbtypes.AttributeValue{}

	out, err := client.Query(context.Background(), input)
	if err == nil {
		items = out.Items
	}

	return items, err
}

func setupClient(table string) FakeClient {
	dynamodbEndpoint := os.Getenv("LOCAL_DYNAMODB_ENDPOINT")
	if dynamodbEndpoint != "" {
		return setupDynamoDBLocal(dynamodbEndpoint)
	}

	client := NewClient()

	return client
}

func setupDynamoDBLocal(endpoint string) FakeClient {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("localhost"),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
		config.WithRetryer(func() aws.Retryer {
			return aws.NopRetryer{}
		}),
	)
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("settings dynamodb-local tables failed:", err)
		}
	}()

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func setupNativeInterpreter(native *interpreter.Native, table string) {
	native.AddUpdater(table, "SET second_type = :ntype", func(item map[string]*types.Item, updates map[string]*types.Item) {
		item["second_type"] = updates[":ntype"]
	})

	native.AddUpdater(table, "SET #type = :ntype", func(item map[string]*types.Item, updates map[string]*types.Item) {
		item["type"] = updates[":ntype"]
	})
}

func getData(client FakeClient, tn, p, r string) (map[string]dynamodbtypes.AttributeValue, error) {
	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(tn),
		Key: map[string]dynamodbtypes.AttributeValue{
			"partition": &dynamodbtypes.AttributeValueMemberS{
				Value: p,
			},
			"range": &dynamodbtypes.AttributeValueMemberS{
				Value: r,
			},
		},
	}

	out, err := client.GetItem(context.Background(), getInput)

	return out.Item, err
}

func getDataInIndex(client FakeClient, index, tn, p, r string) ([]map[string]dynamodbtypes.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":data": &dynamodbtypes.AttributeValueMemberS{
				Value: p,
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#data": "data",
		},
		KeyConditionExpression: aws.String("#data = :data"),
		TableName:              aws.String(tn),
		IndexName:              aws.String(index),
	}

	items := []map[string]dynamodbtypes.AttributeValue{}

	out, err := client.Query(context.Background(), input)
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

	err := AddTable(context.Background(), client, "tests", "hk", "rk")
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
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{
				AttributeName: aws.String("partition"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
		},
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{
				AttributeName: aws.String("partition"),
				KeyType:       dynamodbtypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("range"),
				KeyType:       dynamodbtypes.KeyTypeRange,
			},
		},
		TableName: aws.String(tableName),
	}

	_, err := client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "Range Key not specified in Attribute Definitions")

	input.AttributeDefinitions = append(input.AttributeDefinitions, dynamodbtypes.AttributeDefinition{
		AttributeName: aws.String("range"),
		AttributeType: dynamodbtypes.ScalarAttributeTypeS,
	})

	_, err = client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "No provisioned throughput specified for the table")

	input.BillingMode = dynamodbtypes.BillingModePayPerRequest

	_, err = client.CreateTable(context.Background(), input)
	c.NoError(err)

	_, err = client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "Cannot create preexisting table")
}

func TestCreateTableWithGSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{
				AttributeName: aws.String("partition"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("range"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
		},
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{
				AttributeName: aws.String("partition"),
				KeyType:       dynamodbtypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("range"),
				KeyType:       dynamodbtypes.KeyTypeRange,
			},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{},
		TableName:              aws.String(tableName + "-gsi"),
	}

	_, err := client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "GSI list is empty/invalid")

	input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, dynamodbtypes.GlobalSecondaryIndex{
		IndexName: aws.String("invert"),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{
				AttributeName: aws.String("range"),
				KeyType:       dynamodbtypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("no_defined"),
				KeyType:       dynamodbtypes.KeyTypeRange,
			},
		},
		Projection: &dynamodbtypes.Projection{
			ProjectionType: dynamodbtypes.ProjectionTypeAll,
		},
	})

	_, err = client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "No provisioned throughput specified for the global secondary index")

	input.GlobalSecondaryIndexes[0].ProvisionedThroughput = &dynamodbtypes.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(1),
		WriteCapacityUnits: aws.Int64(1),
	}

	_, err = client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "Global Secondary Index Range Key not specified in Attribute Definitions")

	input.GlobalSecondaryIndexes[0].KeySchema[1].AttributeName = aws.String("partition")

	_, err = client.CreateTable(context.Background(), input)
	c.NoError(err)
}

func TestCreateTableWithLSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{
				AttributeName: aws.String("partition"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("range"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("data"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
		},
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{
				AttributeName: aws.String("partition"),
				KeyType:       dynamodbtypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("range"),
				KeyType:       dynamodbtypes.KeyTypeRange,
			},
		},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{},
		TableName:             aws.String(tableName + "-lsi"),
	}

	_, err := client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "LSI list is empty/invalid")

	input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, dynamodbtypes.LocalSecondaryIndex{
		IndexName: aws.String("data"),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{
				AttributeName: aws.String("partition"),
				KeyType:       dynamodbtypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("no_defined"),
				KeyType:       dynamodbtypes.KeyTypeRange,
			},
		},
		Projection: &dynamodbtypes.Projection{
			ProjectionType: dynamodbtypes.ProjectionTypeAll,
		},
	})

	_, err = client.CreateTable(context.Background(), input)
	c.Contains(err.Error(), "Local Secondary Index Range Key not specified in Attribute Definitions")

	input.LocalSecondaryIndexes[0].KeySchema[1].AttributeName = aws.String("data")

	_, err = client.CreateTable(context.Background(), input)
	c.NoError(err)
}

func TestDeleteTable(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.DeleteTableInput{
		TableName: aws.String("table-404"),
	}

	_, err := client.DeleteTable(context.Background(), input)
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	input = &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	out, err := client.DeleteTable(context.Background(), input)
	c.NoError(err)

	c.NotEmpty(out)

	_, err = client.DeleteTable(context.Background(), input)
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())
}

func TestUpdateTable(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	input := &dynamodb.UpdateTableInput{
		BillingMode:                 dynamodbtypes.BillingModeProvisioned,
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{},
		TableName:                   aws.String("404"),
	}

	_, err := client.UpdateTable(context.Background(), input)
	c.Equal("ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())

	err = ensurePokemonTable(client)
	c.NoError(err)

	input = &dynamodb.UpdateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("type"),
				AttributeType: dynamodbtypes.ScalarAttributeTypeS,
			},
		},
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{
				Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("newIndex"),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{
							AttributeName: aws.String("type"),
							KeyType:       dynamodbtypes.KeyTypeHash,
						},
						{
							AttributeName: aws.String("id"),
							KeyType:       dynamodbtypes.KeyTypeRange,
						},
					},
					Projection: &dynamodbtypes.Projection{
						ProjectionType: dynamodbtypes.ProjectionTypeAll,
					},
				},
			},
		},
		TableName: aws.String(tableName),
	}
	output, err := client.UpdateTable(context.Background(), input)
	c.NoError(err)

	c.Len(output.TableDescription.GlobalSecondaryIndexes, 1)

	input = &dynamodb.UpdateTableInput{
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{
				Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{
					IndexName: aws.String("newIndex"),
					ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(1),
						WriteCapacityUnits: aws.Int64(1),
					},
				},
			},
		},
		TableName: aws.String(tableName),
	}
	_, err = client.UpdateTable(context.Background(), input)
	c.NoError(err)

	input = &dynamodb.UpdateTableInput{
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{
				Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{
					IndexName: aws.String("newIndex"),
				},
			},
		},
		TableName: aws.String(tableName),
	}
	_, err = client.UpdateTable(context.Background(), input)
	c.NoError(err)

	_, err = client.UpdateTable(context.Background(), input)
	c.Equal("ResourceNotFoundException: Requested resource not found", err.Error())
}

func TestPutAndGetItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	opt := func(opt *attributevalue.EncoderOptions) {
		opt.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	}, opt)
	c.NoError(err)

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tableName),
	}

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)

	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
	}
	out, err := client.GetItem(context.Background(), getInput)
	c.NoError(err)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "001"}, out.Item["id"])
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, out.Item["name"])
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "grass"}, out.Item["type"])

	_, err = client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       map[string]dynamodbtypes.AttributeValue{},
	})

	c.Error(err)
	c.Contains(err.Error(), "number of conditions on the keys is invalid")
}

func TestPutAndGetBatchItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	opt := func(opt *attributevalue.EncoderOptions) {
		opt.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	}, opt)
	c.NoError(err)

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tableName),
	}

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)

	item, err = attributevalue.MarshalMapWithOptions(pokemon{
		ID:   "002",
		Type: "fire",
		Name: "Sharmander",
	}, opt)
	c.NoError(err)

	input = &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tableName),
	}

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)

	getInput := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
			tableName: {
				Keys: []map[string]dynamodbtypes.AttributeValue{
					{
						"id": &dynamodbtypes.AttributeValueMemberS{
							Value: "001",
						},
					},
					{
						"id": &dynamodbtypes.AttributeValueMemberS{
							Value: "002",
						},
					},
					{
						"t1": &dynamodbtypes.AttributeValueMemberS{
							Value: "003",
						},
					},
					{
						"id": &dynamodbtypes.AttributeValueMemberS{
							Value: "004",
						},
					},
				},
			},
		},
	}

	ActiveForceFailure(client)

	out, err := client.BatchGetItem(context.Background(), getInput)
	c.Nil(out)
	c.EqualError(err, ErrForcedFailure.Error())

	DeactiveForceFailure(client)

	out, err = client.BatchGetItem(context.Background(), getInput)
	c.NoError(err)
	c.Len(out.Responses[tableName], 2)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "001"}, out.Responses[tableName][0]["id"])
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "002"}, out.Responses[tableName][1]["id"])
	c.Len(out.UnprocessedKeys[tableName].Keys, 2)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "003"}, out.UnprocessedKeys[tableName].Keys[0]["t1"])
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "004"}, out.UnprocessedKeys[tableName].Keys[1]["id"])
}

func TestPutWithGSI(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	item := map[string]dynamodbtypes.AttributeValue{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: "001"},
		"name": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
		"type": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tableName),
	}

	_, err = client.PutItem(context.Background(), input)
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
	c.Contains(err.Error(), "value type")

	delete(item, "type")

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)

	_ = AddIndex(context.Background(), client, tableName, "sort-by-second-type", "id", "second_type")

	item, err = attributevalue.MarshalMapWithOptions(pokemon{
		ID:   "002",
		Name: "Ivysaur",
		Type: "grass",
	}, func(eo *attributevalue.EncoderOptions) {
		eo.TagKey = "json"
	})
	c.NoError(err)

	input.Item = item

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)
}

func TestGetItemWithUnusedAttributes(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#name": "name",
		},
	}

	_, err = client.GetItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)
}

func TestGetItemWithInvalidExpressionAttributeNames(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
		ProjectionExpression: aws.String("#name-1"),
		ExpressionAttributeNames: map[string]string{
			"#name-1": "name",
		},
	}

	_, err = client.GetItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)
}

func TestGetItemWithProjectionExpression(t *testing.T) {
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

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
		},
		ProjectionExpression: aws.String("#n"),
		ExpressionAttributeNames: map[string]string{
			"#n": "name",
		},
	})
	c.NoError(err)
	c.Len(out.Item, 1)

	name, ok := out.Item["name"].(*dynamodbtypes.AttributeValueMemberS)
	c.True(ok)
	c.Equal("Bulbasaur", name.Value)
}

func TestGetItemWithProjectionInvalidExpression(t *testing.T) {
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

	_, err = client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
		},
		ProjectionExpression: aws.String("("),
	})
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
}

func TestPutItemWithConditions(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)
	err := ensurePokemonTable(client)
	c.NoError(err)

	opt := func(opt *attributevalue.EncoderOptions) {
		opt.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	}, opt)
	c.NoError(err)

	input := &dynamodb.PutItemInput{
		Item:                item,
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("attribute_not_exists(#type)"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
	}

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":not_used": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.PutItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeValuesMsg)

	input.ConditionExpression = aws.String("attribute_not_exists(#invalid-name)")

	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "hello",
	}

	_, err = client.PutItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.ConditionExpression = aws.String("#valid_name = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#valid_name": "hello",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.PutItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)
}

func TestUpdateItem(t *testing.T) {
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

	expr := map[string]dynamodbtypes.AttributeValue{
		":ntype": &dynamodbtypes.AttributeValueMemberS{
			Value: "poison",
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String("SET second_type = :ntype"),
		ExpressionAttributeValues: expr,
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NoError(err)

	item, err := getPokemon(client, "001")
	c.NoError(err)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "poison"}, item["second_type"])

	input.Key["id"] = &dynamodbtypes.AttributeValueMemberS{
		Value: "404",
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NoError(err)

	item, err = getPokemon(client, "404")
	c.NoError(err)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "poison"}, item["second_type"])

	minidynClient, ok := client.(*Client)
	if !ok {
		return
	}

	setupNativeInterpreter(minidynClient.GetNativeInterpreter(), tableName)
	minidynClient.ActivateNativeInterpreter()

	input.Key = map[string]dynamodbtypes.AttributeValue{
		"id": &dynamodbtypes.AttributeValueMemberS{
			Value: "001",
		},
	}
	expr[":ntype"] = &dynamodbtypes.AttributeValueMemberS{
		Value: "water",
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NoError(err)

	item, err = getPokemon(client, "001")
	c.NoError(err)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "water"}, item["second_type"])
}

func TestUpdateItemReturnValuesMatrix(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	c.NoError(ensurePokemonTable(client))

	putAndUpdate := func(pk string, rv dynamodbtypes.ReturnValue) map[string]dynamodbtypes.AttributeValue {
		t.Helper()

		_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamodbtypes.AttributeValue{
				"id":   &dynamodbtypes.AttributeValueMemberS{Value: pk},
				"type": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
				"name": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
			},
		})
		c.NoError(err)

		out, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"id": &dynamodbtypes.AttributeValueMemberS{Value: pk},
			},
			UpdateExpression: aws.String("SET second_type = :st"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":st": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
			},
			ReturnValues: rv,
		})
		c.NoError(err)

		return out.Attributes
	}

	c.Nil(putAndUpdate("urv-none", dynamodbtypes.ReturnValueNone))

	allOld := putAndUpdate("urv-all-old", dynamodbtypes.ReturnValueAllOld)
	c.Len(allOld, 3)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, allOld["name"])
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "grass"}, allOld["type"])

	updatedOld := putAndUpdate("urv-upd-old", dynamodbtypes.ReturnValueUpdatedOld)
	c.Empty(updatedOld)

	updatedNew := putAndUpdate("urv-upd-new", dynamodbtypes.ReturnValueUpdatedNew)
	c.Len(updatedNew, 1)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "poison"}, updatedNew["second_type"])

	allNew := putAndUpdate("urv-all-new", dynamodbtypes.ReturnValueAllNew)
	c.Len(allNew, 4)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "poison"}, allNew["second_type"])
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
	expr := map[string]dynamodbtypes.AttributeValue{
		":ntyp": &dynamodbtypes.AttributeValueMemberS{
			Value: "poison",
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "404",
			},
		},
		ConditionExpression:       aws.String("attribute_exists(id)"),
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	input.ConditionExpression = aws.String("attribute_exists(#invalid-name)")

	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "type",
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.ConditionExpression = aws.String("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#t": "type",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)

	input = &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "404",
			},
		},
		ConditionExpression:       aws.String("attribute_exists(#id)"),
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
	}

	var errConditionalCheckFailedException *dynamodbtypes.ConditionalCheckFailedException

	_, err = client.UpdateItem(context.Background(), input)
	c.True(errors.As(err, &errConditionalCheckFailedException))

	input = &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
		ConditionExpression:       aws.String("attribute_not_exists(#id)"),
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
		ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
	}

	errConditionalCheckFailedException = &dynamodbtypes.ConditionalCheckFailedException{}

	_, err = client.UpdateItem(context.Background(), input)
	c.True(errors.As(err, &errConditionalCheckFailedException))
	c.NotEmpty(errConditionalCheckFailedException.Item)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "001"}, errConditionalCheckFailedException.Item["id"])
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, errConditionalCheckFailedException.Item["name"])
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
	expr := map[string]dynamodbtypes.AttributeValue{
		":ntype": &dynamodbtypes.AttributeValueMemberS{
			Value: "poison",
		},
	}
	names := map[string]string{"#type": "type"}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String(uexpr),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames:  names,
	}

	_, err = client.UpdateItem(context.Background(), input)
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

	expr := map[string]dynamodbtypes.AttributeValue{
		":second_type": &dynamodbtypes.AttributeValueMemberS{
			Value: "poison",
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"foo": &dynamodbtypes.AttributeValueMemberS{
				Value: "a",
			},
		},
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String("SET second_type = :second_type"),
		ExpressionAttributeValues: expr,
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.Contains(err.Error(), "One of the required keys was not given a value")

	ActiveForceFailure(client)
	defer DeactiveForceFailure(client)

	output, err := client.UpdateItem(context.Background(), input)
	c.EqualError(err, "forced failure response")
	c.Nil(output)
}

func TestUpdateExpressions(t *testing.T) {
	c := require.New(t)
	db := []pokemon{
		{
			ID:         "001",
			Type:       "grass",
			Name:       "Bulbasaur",
			SecondType: "type",
			Moves:      []string{"Growl", "Tackle", "Vine Whip", "Growth"},
			Local:      []string{"001 (Red/Blue/Yellow)", "226 (Gold/Silver/Crystal)", "001 (FireRed/LeafGreen)", "001 (Let's Go Pikachu/Let's Go Eevee)"},
		},
	}

	tests := map[string]struct {
		input  *dynamodb.UpdateItemInput
		verify func(tc *testing.T, client FakeClient)
	}{
		"add": {
			input: &dynamodb.UpdateItemInput{
				TableName: aws.String(tableName),
				Key: map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{
						Value: "001",
					},
				},
				ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
				UpdateExpression:          aws.String("ADD lvl :one"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":one": &dynamodbtypes.AttributeValueMemberN{Value: "1"}},
			},
			verify: func(tc *testing.T, client FakeClient) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.Equal(&dynamodbtypes.AttributeValueMemberN{Value: "1"}, item["lvl"])
			},
		},
		"remove": {
			input: &dynamodb.UpdateItemInput{
				TableName: aws.String(tableName),
				Key: map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{
						Value: "001",
					},
				},
				ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
				UpdateExpression: aws.String("REMOVE #l[0],#l[1],#l[2],#l[3]"),
				ExpressionAttributeNames: map[string]string{
					"#l": "local",
				},
			},
			verify: func(tc *testing.T, client FakeClient) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				local, ok := item["local"].(*dynamodbtypes.AttributeValueMemberNULL)
				a.True(ok)
				a.True(local.Value)
			},
		},
		"delete": {
			input: &dynamodb.UpdateItemInput{
				TableName: aws.String(tableName),
				Key: map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{
						Value: "001",
					},
				},
				ReturnValues:     dynamodbtypes.ReturnValueUpdatedNew,
				UpdateExpression: aws.String("DELETE moves :move"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":move": &dynamodbtypes.AttributeValueMemberSS{
						Value: []string{"Growl"},
					},
				},
			},
			verify: func(tc *testing.T, client FakeClient) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				moves, ok := item["moves"].(*dynamodbtypes.AttributeValueMemberSS)
				a.True(ok)

				a.Len(moves.Value, 3)
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

			_, err = client.UpdateItem(context.Background(), tt.input)
			a.NoError(err)

			tt.verify(tc, client)
		})
	}
}

func TestQuery(t *testing.T) {
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
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":id": &dynamodbtypes.AttributeValueMemberS{
				Value: "004",
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
		KeyConditionExpression: aws.String("#id = :id"),
		TableName:              aws.String(tableName),
	}

	out, err := client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Empty(out.LastEvaluatedKey)

	input.FilterExpression = aws.String("#type = :type")

	input.ExpressionAttributeNames["#type"] = "type"
	input.ExpressionAttributeValues[":type"] = &dynamodbtypes.AttributeValueMemberS{
		Value: "fire",
	}

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)

	input.ExpressionAttributeValues[":type"] = &dynamodbtypes.AttributeValueMemberS{
		Value: "grass",
	}

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Empty(out.Items)

	input.ExpressionAttributeNames["#not_used"] = "hello"

	_, err = client.Query(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	input.KeyConditionExpression = aws.String("#invalid-name = :id")
	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "id",
	}

	_, err = client.Query(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.KeyConditionExpression = aws.String("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#t": "type",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.Query(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)

	minidynClient, ok := client.(*Client)
	if !ok {
		return
	}

	setupNativeInterpreter(minidynClient.GetNativeInterpreter(), tableName)
	minidynClient.ActivateNativeInterpreter()

	input = &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":id": &dynamodbtypes.AttributeValueMemberS{
				Value: "004",
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
		KeyConditionExpression: aws.String("#id = :id"),
		TableName:              aws.String(tableName),
	}

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Empty(out.LastEvaluatedKey)
}

func TestQueryPagination(t *testing.T) {
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
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":type": &dynamodbtypes.AttributeValueMemberS{
				Value: "grass",
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		KeyConditionExpression: aws.String("#type = :type"),
		TableName:              aws.String(tableName),
		IndexName:              aws.String("by-type"),
		Limit:                  aws.Int32(1),
	}

	out, err := client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "001"}, out.Items[0]["id"])

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "002"}, out.Items[0]["id"])

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "003"}, out.Items[0]["id"])
	c.NotEmpty(out.LastEvaluatedKey)

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Empty(out.Items)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = aws.Int32(4)
	input.ExclusiveStartKey = nil

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = aws.Int32(2)
	input.ExclusiveStartKey = nil

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 2)
	c.NotEmpty(out.LastEvaluatedKey)
	input.ExclusiveStartKey = out.LastEvaluatedKey

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = nil
	input.ExclusiveStartKey = nil

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)
	c.Empty(out.LastEvaluatedKey)

	input.Limit = aws.Int32(4)
	input.ExclusiveStartKey = nil

	err = createPokemon(client, pokemon{
		ID:   "004",
		Type: "fire",
		Name: "Charmander",
	})
	c.NoError(err)

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)
	c.Empty(out.LastEvaluatedKey)

	input.ScanIndexForward = aws.Bool(false)

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "003"}, out.Items[0]["id"])

	// Query with FilterExpression
	input.ScanIndexForward = nil
	input.ExclusiveStartKey = nil
	input.FilterExpression = aws.String("begins_with(#name, :letter)")
	input.Limit = aws.Int32(2)
	input.ExpressionAttributeValues[":letter"] = &dynamodbtypes.AttributeValueMemberS{
		Value: "V",
	}
	input.ExpressionAttributeNames["#name"] = "name"

	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Empty(out.Items)
	c.NotEmpty(out.LastEvaluatedKey)

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "003"}, out.Items[0]["id"])
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
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":partition": &dynamodbtypes.AttributeValueMemberS{
				Value: "a",
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#partition": "partition",
		},
		// Syntax Error
		KeyConditionExpression: aws.String("#partition != :partition"),
		TableName:              aws.String(tableName),
	}

	_, err = client.Query(context.Background(), input)
	c.Error(err)
	var apiErr smithy.APIError
	c.True(errors.As(err, &apiErr))
	c.Equal("ValidationException", apiErr.ErrorCode())
}

func TestScan(t *testing.T) {
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
		TableName: aws.String(tableName),
	}

	out, err := client.Scan(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 3)

	input.Limit = aws.Int32(1)
	out, err = client.Scan(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.NotEmpty(out.LastEvaluatedKey)

	input.FilterExpression = aws.String("#invalid-name = Raichu")
	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "Name",
	}

	_, err = client.Scan(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.FilterExpression = aws.String("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#t": "type",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.Scan(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)

	input.Limit = nil
	input.FilterExpression = aws.String("#name = :name")
	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":name": &dynamodbtypes.AttributeValueMemberS{
			Value: "Venusaur",
		},
	}
	input.ExpressionAttributeNames = map[string]string{
		"#name": "name",
	}

	out, err = client.Scan(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)

	input.ExpressionAttributeNames["#not_used"] = "hello"

	_, err = client.Scan(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	delete(input.ExpressionAttributeNames, "#not_used")

	if fclient, isFake := client.(*Client); isFake {
		ActiveForceFailure(fclient)

		out, err = client.Scan(context.Background(), input)
		c.Equal(ErrForcedFailure, err)
		c.Empty(out)

		DeactiveForceFailure(fclient)
	}
}

func TestDeleteItem(t *testing.T) {
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

	key := map[string]dynamodbtypes.AttributeValue{
		"id": &dynamodbtypes.AttributeValueMemberS{Value: "003"},
	}

	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(tableName),
	}

	items, err := getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Len(items, 3)

	_, err = client.DeleteItem(context.Background(), input)
	c.NoError(err)

	item, err := getPokemon(client, "003")
	c.NoError(err)

	c.Empty(item)

	items, err = getPokemonsByType(client, "grass")
	c.NoError(err)
	c.Len(items, 2)

	_, err = client.DeleteItem(context.Background(), input)
	c.NoError(err)

	if _, ok := client.(*Client); ok {
		EmulateFailure(client, FailureConditionInternalServerError)

		defer func() { EmulateFailure(client, FailureConditionNone) }()

		output, forcedError := client.DeleteItem(context.Background(), input)
		c.Nil(output)

		var errInternalServerError *dynamodbtypes.InternalServerError

		c.True(errors.As(forcedError, &errInternalServerError))
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
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
		},
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("attribute_exists(id)"),
	}

	_, err = client.DeleteItem(context.Background(), input)
	c.NoError(err)

	var errConditionalCheckFailedException *dynamodbtypes.ConditionalCheckFailedException

	_, err = client.DeleteItem(context.Background(), input)
	c.True(errors.As(err, &errConditionalCheckFailedException))

	input.ExpressionAttributeNames = map[string]string{
		"#not_used": "hello",
	}

	_, err = client.DeleteItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), unusedExpressionAttributeNamesMsg)

	input.ConditionExpression = aws.String("#invalid-name = Squirtle")

	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "hello",
	}

	_, err = client.DeleteItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeName)

	input.ConditionExpression = aws.String("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#t": "type",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.DeleteItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), invalidExpressionAttributeValue)
}

func TestDeleteItemWithReturnValues(t *testing.T) {
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
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
		},
		TableName:    aws.String(tableName),
		ReturnValues: dynamodbtypes.ReturnValueAllOld,
	}

	output, err := client.DeleteItem(context.Background(), input)
	c.NoError(err)

	c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, output.Attributes["name"])
}

func TestDescribeTable(t *testing.T) {
	c := require.New(t)

	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	output, err := client.DescribeTable(context.Background(), describeTableInput)
	c.NoError(err)
	c.NotNil(output)
	c.Len(output.Table.KeySchema, 1)
	c.Equal(aws.ToString(output.Table.TableName), tableName)
	c.Equal(output.Table.KeySchema[0].KeyType, dynamodbtypes.KeyTypeHash)
	c.Equal(aws.ToString(output.Table.KeySchema[0].AttributeName), "id")
}

func TestDescribeTableFail(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	expected := "ResourceNotFoundException: Cannot do operations on a non-existent table"
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String("non_existing"),
	}

	output, err := client.DescribeTable(context.Background(), describeTableInput)
	c.Error(err)
	c.Equal(expected, err.Error())
	c.Empty(output)
}

func TestBatchWriteItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	err := ensurePokemonTable(client)
	c.NoError(err)

	m := pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	}

	opt := func(opt *attributevalue.EncoderOptions) {
		opt.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(m, opt)
	c.NoError(err)

	requests := []dynamodbtypes.WriteRequest{
		{
			PutRequest: &dynamodbtypes.PutRequest{
				Item: item,
			},
		},
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: requests,
		},
	}

	itemCollectionMetrics := map[string][]dynamodbtypes.ItemCollectionMetrics{
		"table": {
			{
				ItemCollectionKey: map[string]dynamodbtypes.AttributeValue{
					"report_id": &dynamodbtypes.AttributeValueMemberS{
						Value: "1234",
					},
				},
			},
		},
	}

	SetItemCollectionMetrics(client, itemCollectionMetrics)

	output, err := client.BatchWriteItem(context.Background(), input)
	c.NoError(err)

	c.Equal(itemCollectionMetrics, output.ItemCollectionMetrics)

	c.NotEmpty(getPokemon(client, "001"))

	_, err = client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {
				{
					DeleteRequest: &dynamodbtypes.DeleteRequest{Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					}},
				},
			},
		},
	})
	c.NoError(err)

	c.Empty(getPokemon(client, "001"))

	delete(item, "id")

	_, err = client.BatchWriteItem(context.Background(), input)
	c.Contains(err.Error(), "One of the required keys was not given a value")

	_, err = client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {
				{
					DeleteRequest: &dynamodbtypes.DeleteRequest{Key: map[string]dynamodbtypes.AttributeValue{
						"id":   &dynamodbtypes.AttributeValueMemberS{Value: "001"},
						"type": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
					}},
					PutRequest: &dynamodbtypes.PutRequest{Item: item},
				},
			},
		},
	})

	c.Contains(err.Error(), "ValidationException: Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")

	_, err = client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {{}},
		},
	})
	c.Contains(err.Error(), "ValidationException: Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")

	item, err = attributevalue.MarshalMap(m)
	c.NoError(err)

	for range batchRequestsLimit {
		requests = append(requests, dynamodbtypes.WriteRequest{
			PutRequest: &dynamodbtypes.PutRequest{Item: item},
		})
	}

	_, err = client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
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

	opt := func(opt *attributevalue.EncoderOptions) {
		opt.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(m, opt)
	c.NoError(err)

	requests := []dynamodbtypes.WriteRequest{
		{
			PutRequest: &dynamodbtypes.PutRequest{
				Item: item,
			},
		},
	}

	EmulateFailure(client, FailureConditionInternalServerError)
	defer EmulateFailure(client, FailureConditionNone)

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: requests,
		},
	}

	output, err := client.BatchWriteItem(context.Background(), input)
	c.EqualError(err, "InternalServerError: emulated error")
	c.Nil(output)
}

func TestHandleBatchWriteRequestError(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		unprocessed := map[string][]dynamodbtypes.WriteRequest{}
		req := dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{}}
		err := handleBatchWriteRequestError("t", req, unprocessed, nil)
		require.NoError(t, err)
		require.Empty(t, unprocessed)
	})

	t.Run("non smithy API error", func(t *testing.T) {
		unprocessed := map[string][]dynamodbtypes.WriteRequest{}
		req := dynamodbtypes.WriteRequest{}
		in := errors.New("plain failure")
		err := handleBatchWriteRequestError("t", req, unprocessed, in)
		require.ErrorIs(t, err, in)
		require.Empty(t, unprocessed)
	})

	t.Run("smithy API error not retriable", func(t *testing.T) {
		unprocessed := map[string][]dynamodbtypes.WriteRequest{}
		req := dynamodbtypes.WriteRequest{}
		in := &smithy.GenericAPIError{Code: "ValidationException", Message: "invalid"}
		err := handleBatchWriteRequestError("t", req, unprocessed, in)
		require.ErrorIs(t, err, in)
		require.Empty(t, unprocessed)
	})

	t.Run("internal server error adds to unprocessed", func(t *testing.T) {
		unprocessed := map[string][]dynamodbtypes.WriteRequest{}
		req := dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{}}
		in := &dynamodbtypes.InternalServerError{Message: aws.String("emulated")}
		err := handleBatchWriteRequestError("mytable", req, unprocessed, in)
		require.NoError(t, err)
		require.Len(t, unprocessed["mytable"], 1)
		require.Equal(t, req, unprocessed["mytable"][0])
	})

	t.Run("internal server error appends when table key already exists", func(t *testing.T) {
		first := dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{}}
		second := dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{}}
		unprocessed := map[string][]dynamodbtypes.WriteRequest{"tbl": {first}}
		in := &dynamodbtypes.InternalServerError{Message: aws.String("retry")}
		err := handleBatchWriteRequestError("tbl", second, unprocessed, in)
		require.NoError(t, err)
		require.Len(t, unprocessed["tbl"], 2)
		require.Equal(t, first, unprocessed["tbl"][0])
		require.Equal(t, second, unprocessed["tbl"][1])
	})

	t.Run("provisioned throughput exceeded adds to unprocessed", func(t *testing.T) {
		unprocessed := map[string][]dynamodbtypes.WriteRequest{}
		req := dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{}}
		in := &dynamodbtypes.ProvisionedThroughputExceededException{Message: aws.String("throttled")}
		err := handleBatchWriteRequestError("tbl", req, unprocessed, in)
		require.NoError(t, err)
		require.Len(t, unprocessed["tbl"], 1)
		require.Equal(t, req, unprocessed["tbl"][0])
	})
}

func TestTransactWriteItems(t *testing.T) {
	t.Run("atomicity", func(t *testing.T) {
		t.Run("rollback on failure", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(tableName),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id":   &dynamodbtypes.AttributeValueMemberS{Value: "rollback-me"},
								"type": &dynamodbtypes.AttributeValueMemberS{Value: "fire"},
							},
						},
					},
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "non-existent"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})
			c.Error(err)

			item, err := getPokemon(client, "rollback-me")
			c.NoError(err)
			c.Empty(item)
		})

		t.Run("rollback across tables", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			const secondTable = "items"

			err = AddTable(context.Background(), client, secondTable, "id", "")
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(secondTable),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "should-rollback"},
							},
						},
					},
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "non-existent"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})

			var tce *dynamodbtypes.TransactionCanceledException
			c.True(errors.As(err, &tce))

			out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
				TableName: aws.String(secondTable),
				Key: map[string]dynamodbtypes.AttributeValue{
					"id": &dynamodbtypes.AttributeValueMemberS{Value: "should-rollback"},
				},
			})
			c.NoError(err)
			c.Empty(out.Item)
		})
	})

	t.Run("validation", func(t *testing.T) {
		t.Run("empty item rejected", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{{}},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("duplicate item rejected", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(tableName),
							Item:      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
						},
					},
					{
						Update: &dynamodbtypes.Update{
							TableName:                 aws.String(tableName),
							Key:                       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression:          aws.String("SET #n = :n"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}},
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})
	})

	t.Run("error injection", func(t *testing.T) {
		t.Run("emulated server error", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			EmulateFailure(client, FailureConditionInternalServerError)
			defer EmulateFailure(client, FailureConditionNone)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(tableName),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
							},
						},
					},
				},
			})

			var internalErr *dynamodbtypes.InternalServerError
			c.True(errors.As(err, &internalErr))
		})

		t.Run("force failure", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			ActiveForceFailure(client)
			defer DeactiveForceFailure(client)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(tableName),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
							},
						},
					},
				},
			})
			c.Equal(ErrForcedFailure, err)
		})
	})

	t.Run("put", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			output, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(tableName),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id":   &dynamodbtypes.AttributeValueMemberS{Value: "001"},
								"type": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
								"name": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
							},
						},
					},
				},
			})
			c.NoError(err)
			c.NotNil(output)

			item, err := getPokemon(client, "001")
			c.NoError(err)
			c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, item["name"])
		})

		t.Run("non-existent table", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String("non-existent"),
							Item:      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
						},
					},
				},
			})

			var notFound *dynamodbtypes.ResourceNotFoundException
			c.True(errors.As(err, &notFound))
		})

		t.Run("unused expression attribute", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName: aws.String(tableName),
							Item: map[string]dynamodbtypes.AttributeValue{
								"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
							},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
								":unused": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
							},
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("failing condition", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Put: &dynamodbtypes.Put{
							TableName:           aws.String(tableName),
							Item:                map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("attribute_not_exists(id)"),
						},
					},
				},
			})

			var tce *dynamodbtypes.TransactionCanceledException
			c.True(errors.As(err, &tce))
			c.Equal("ConditionalCheckFailed", aws.ToString(tce.CancellationReasons[0].Code))
		})
	})

	t.Run("update", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Update: &dynamodbtypes.Update{
							TableName:        aws.String(tableName),
							Key:              map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression: aws.String("SET second_type = :stype"),
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
								":stype": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
							},
						},
					},
				},
			})
			c.NoError(err)

			item, err := getPokemon(client, "001")
			c.NoError(err)
			c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "poison"}, item["second_type"])
		})

		t.Run("non-existent table", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Update: &dynamodbtypes.Update{
							TableName:                 aws.String("non-existent"),
							Key:                       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
							UpdateExpression:          aws.String("SET #n = :n"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberS{Value: "x"}},
						},
					},
				},
			})

			var notFound *dynamodbtypes.ResourceNotFoundException
			c.True(errors.As(err, &notFound))
		})

		t.Run("unused expression attribute", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Update: &dynamodbtypes.Update{
							TableName:                aws.String(tableName),
							Key:                      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression:         aws.String("SET #n = :n"),
							ExpressionAttributeNames: map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
								":n":      &dynamodbtypes.AttributeValueMemberS{Value: "Venusaur"},
								":unused": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
							},
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("syntax error", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Update: &dynamodbtypes.Update{
							TableName:                 aws.String(tableName),
							Key:                       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression:          aws.String("SET #n = :n SET #n = :n"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberS{Value: "Venusaur"}},
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("failing condition", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Update: &dynamodbtypes.Update{
							TableName:                 aws.String(tableName),
							Key:                       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							UpdateExpression:          aws.String("SET #n = :n"),
							ConditionExpression:       aws.String("attribute_not_exists(id)"),
							ExpressionAttributeNames:  map[string]string{"#n": "name"},
							ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberS{Value: "Venusaur"}},
						},
					},
				},
			})

			var tce *dynamodbtypes.TransactionCanceledException
			c.True(errors.As(err, &tce))
			c.Equal("ConditionalCheckFailed", aws.ToString(tce.CancellationReasons[0].Code))
		})
	})

	t.Run("delete", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Delete: &dynamodbtypes.Delete{
							TableName: aws.String(tableName),
							Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
						},
					},
				},
			})
			c.NoError(err)

			item, err := getPokemon(client, "001")
			c.NoError(err)
			c.Empty(item)
		})

		t.Run("non-existent table", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Delete: &dynamodbtypes.Delete{
							TableName: aws.String("non-existent"),
							Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
						},
					},
				},
			})

			var notFound *dynamodbtypes.ResourceNotFoundException
			c.True(errors.As(err, &notFound))
		})

		t.Run("unused expression attribute", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Delete: &dynamodbtypes.Delete{
							TableName:                aws.String(tableName),
							Key:                      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ExpressionAttributeNames: map[string]string{"#unused": "name"},
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("failing condition", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						Delete: &dynamodbtypes.Delete{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("attribute_not_exists(id)"),
						},
					},
				},
			})

			var tce *dynamodbtypes.TransactionCanceledException
			c.True(errors.As(err, &tce))
			c.Equal("ConditionalCheckFailed", aws.ToString(tce.CancellationReasons[0].Code))
		})
	})

	t.Run("condition check", func(t *testing.T) {
		t.Run("pass", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})
			c.NoError(err)
		})

		t.Run("fail", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "999"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})

			var tce *dynamodbtypes.TransactionCanceledException
			c.True(errors.As(err, &tce))
			c.Equal("ConditionalCheckFailed", aws.ToString(tce.CancellationReasons[0].Code))
		})

		t.Run("returns old item on fail", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:                           aws.String(tableName),
							Key:                                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression:                 aws.String("attribute_not_exists(id)"),
							ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
						},
					},
				},
			})

			var tce *dynamodbtypes.TransactionCanceledException
			c.True(errors.As(err, &tce))

			reason := tce.CancellationReasons[0]
			c.Equal("ConditionalCheckFailed", aws.ToString(reason.Code))
			c.NotEmpty(reason.Item)
			c.Equal(&dynamodbtypes.AttributeValueMemberS{Value: "001"}, reason.Item["id"])
		})

		t.Run("non-existent table", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String("non-existent"),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})

			var notFound *dynamodbtypes.ResourceNotFoundException
			c.True(errors.As(err, &notFound))
		})

		t.Run("wrong key attributes", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"wrong_attr": &dynamodbtypes.AttributeValueMemberS{Value: "1"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("wrong key type", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberN{Value: "1"}},
							ConditionExpression: aws.String("attribute_exists(id)"),
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})

		t.Run("invalid expression", func(t *testing.T) {
			c := require.New(t)
			client := NewClient()

			err := ensurePokemonTable(client)
			c.NoError(err)

			err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
			c.NoError(err)

			_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{
						ConditionCheck: &dynamodbtypes.ConditionCheck{
							TableName:           aws.String(tableName),
							Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
							ConditionExpression: aws.String("UNKNOWN_FUNCTION(id)"),
						},
					},
				},
			})

			var apiErr smithy.APIError
			c.True(errors.As(err, &apiErr))
			c.Equal("ValidationException", apiErr.ErrorCode())
		})
	})
}

func TestCheckTableName(t *testing.T) {
	c := require.New(t)

	fclient := NewClient()

	_, err := fclient.getTable(tableName)
	expected := "ResourceNotFoundException: Cannot do operations on a non-existent table"
	c.Equal(expected, err.Error())

	err = AddTable(context.Background(), fclient, "new-table", "partition", "range")
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

	cfg, err := config.LoadDefaultConfig(context.Background())
	c.NoError(err)

	actualClient := dynamodb.NewFromConfig(cfg)

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

	expr := map[string]dynamodbtypes.AttributeValue{
		":type": &dynamodbtypes.AttributeValueMemberS{
			Value: string("water"),
		},
		":name": &dynamodbtypes.AttributeValueMemberS{
			Value: string("piplup"),
		},
	}
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{
				Value: "001",
			},
		},
		ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
		UpdateExpression:          aws.String("SET #type = :type, #name = :name"),
		ExpressionAttributeValues: expr,
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
			"#name": "name",
		},
	}

	_, err = client.UpdateItem(context.Background(), input)
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
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":type": &dynamodbtypes.AttributeValueMemberS{
				Value: "grass",
			},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		KeyConditionExpression: aws.String("#type = :type"),
		TableName:              aws.String(tableName),
		IndexName:              aws.String("by-type"),
		Limit:                  aws.Int32(1),
	}

	for n := 0; n < b.N; n++ {
		out, err := client.Query(context.Background(), input)
		c.NoError(err)
		c.Len(out.Items, 1)
	}
}
