package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

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

func ensurePokemonTable(client *dynamodb.Client) error {
	err := AddTable(context.Background(), client, tableName, "id", "")

	var oe smithy.APIError
	var errResourceInUseException *dynamodbtypes.ResourceInUseException

	if !errors.As(err, &oe) || !errors.As(err, &errResourceInUseException) {
		return err
	}

	return nil
}

func ensurePokemonTypeIndex(client *dynamodb.Client) error {
	err := AddIndex(context.Background(), client, tableName, "by-type", "type", "id")

	var oe smithy.APIError

	if !errors.As(err, &oe) || oe.ErrorCode() != "ValidationException" {
		return err
	}

	return nil
}

func createPokemon(client *dynamodb.Client, creature pokemon) error {
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

func getPokemon(client *dynamodb.Client, id string) (map[string]dynamodbtypes.AttributeValue, error) {
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

func getPokemonsByType(client *dynamodb.Client, typ string) ([]map[string]dynamodbtypes.AttributeValue, error) {
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

func newClient(endpoint string) *dynamodb.Client {
	dynamodbEndpoint := os.Getenv("LOCAL_DYNAMODB_ENDPOINT")
	if dynamodbEndpoint != "" {
		setupDynamoClient(dynamodbEndpoint)
	}

	return setupDynamoClient(endpoint)
}

func setupDynamoClient(endpoint string) *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("localhost"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			})),
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

	return dynamodb.NewFromConfig(cfg)
}

func setupNativeInterpreter(native *interpreter.Native, table string) {
	native.AddUpdater(table, "SET second_type = :ntype", func(item map[string]*types.Item, updates map[string]*types.Item) {
		item["second_type"] = updates[":ntype"]
	})

	native.AddUpdater(table, "SET #type = :ntype", func(item map[string]*types.Item, updates map[string]*types.Item) {
		item["type"] = updates[":ntype"]
	})
}

func getData(client *dynamodb.Client, tn, p, r string) (map[string]dynamodbtypes.AttributeValue, error) {
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

func getDataInIndex(client *dynamodb.Client, index, tn, p, r string) ([]map[string]dynamodbtypes.AttributeValue, error) {
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

// func TestActivateDebug(t *testing.T) {
// 	c := require.New(t)
// 	fake := newClient()

// 	fake.ActivateDebug()

// 	c.True(fake.langInterpreter.Debug)
// }

func TestCreateTable(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	c.Equal("001", out.Item["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
	c.Equal("Bulbasaur", out.Item["name"].(*dynamodbtypes.AttributeValueMemberS).Value)
	c.Equal("grass", out.Item["type"].(*dynamodbtypes.AttributeValueMemberS).Value)

	_, err = client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       map[string]dynamodbtypes.AttributeValue{},
	})

	c.Error(err)
	c.Contains(err.Error(), "number of conditions on the keys is invalid")
}

func TestPutWithGSI(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	item, err = attributevalue.MarshalMap(pokemon{
		ID:   "002",
		Name: "Ivysaur",
		Type: "grass",
	})
	c.NoError(err)

	input.Item = item

	_, err = client.PutItem(context.Background(), input)
	c.NoError(err)
}

func TestGetItemWithUnusedAttributes(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	c.Contains(err.Error(), "") // TODO: FIXME
}

func TestGetItemWithInvalidExpressionAttributeNames(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	c.Contains(err.Error(), "") // TODO: FIXME
}

func TestPutItemWithConditions(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	c.Contains(err.Error(), "") // TODO: FIXME

	input.ConditionExpression = aws.String("attribute_not_exists(#invalid-name)")

	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "hello",
	}

	_, err = client.PutItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), "") // TODO: FIXME

	input.ConditionExpression = aws.String("#valid_name = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#valid_name": "hello",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.PutItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), "") // TODO: FIXME
}

func TestUpdateItem(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	c.Equal("poison", item["second_type"].(*dynamodbtypes.AttributeValueMemberS).Value)

	input.Key["id"] = &dynamodbtypes.AttributeValueMemberS{
		Value: "404",
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NoError(err)

	item, err = getPokemon(client, "404")
	c.NoError(err)
	c.Equal("poison", item["second_type"].(*dynamodbtypes.AttributeValueMemberS).Value)
}

func TestUpdateItemWithConditionalExpression(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
			Value: string("poison"),
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
	c.Contains(err.Error(), "") // TODO: FIXME

	input.ConditionExpression = aws.String("attribute_exists(#invalid-name)")

	input.ExpressionAttributeNames = map[string]string{
		"#invalid-name": "type",
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), "") // TODO: FIXME

	input.ConditionExpression = aws.String("#t = :invalid-value")

	input.ExpressionAttributeNames = map[string]string{
		"#t": "type",
	}

	input.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{
		":invalid-value": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
	}

	_, err = client.UpdateItem(context.Background(), input)
	c.NotNil(err)
	c.Contains(err.Error(), "") // TODO: FIXME

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
	c.Equal("001", errConditionalCheckFailedException.Item["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
	c.Equal("Bulbasaur", errConditionalCheckFailedException.Item["name"].(*dynamodbtypes.AttributeValueMemberS).Value)
}

func TestUpdateItemWithGSI(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
			Value: string("poison"),
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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

	err := ensurePokemonTable(client)
	c.NoError(err)

	err = ensurePokemonTypeIndex(client)
	c.NoError(err)

	err = createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	c.NoError(err)

	expr := map[string]dynamodbtypes.AttributeValue{
		":second_type": &dynamodbtypes.AttributeValueMemberS{
			Value: string("poison"),
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
	c.Contains(err.Error(), "number of conditions on the keys is invalid")

	srv.ActiveForceFailure()
	defer srv.DeactiveForceFailure()

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
		verify func(tc *testing.T, client *dynamodb.Client)
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
			verify: func(tc *testing.T, client *dynamodb.Client) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.Equal("1", item["lvl"].(*dynamodbtypes.AttributeValueMemberN).Value)
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
			verify: func(tc *testing.T, client *dynamodb.Client) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.True(item["local"].(*dynamodbtypes.AttributeValueMemberNULL).Value)
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
			verify: func(tc *testing.T, client *dynamodb.Client) {
				a := assert.New(tc)

				item, err := getPokemon(client, "001")
				a.NoError(err)

				a.Len(item["moves"].(*dynamodbtypes.AttributeValueMemberSS).Value, 3)
			},
		},
	}

	for n, tt := range tests {
		t.Run(n, func(tc *testing.T) {
			a := assert.New(tc)

			srv := ConnectTestServer()
			defer srv.DisconnectTestServer()

			client := newClient(srv.URL)

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
}

func TestQueryPagination(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	c.Equal("001", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal("002", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = client.Query(context.Background(), input)
	c.NoError(err)
	c.Len(out.Items, 1)
	c.Equal("003", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
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
	c.Equal("003", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)

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
	c.Equal("003", out.Items[0]["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
}

func TestQuerySyntaxError(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	c.Panics(func() {
		_, _ = client.Query(context.Background(), input)
	})
}

func TestScan(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	// if fclient, isFake := client.(*Client); isFake {
	// 	ActiveForceFailure(fclient)

	// 	out, err = client.Scan(context.Background(), input)
	// 	c.Equal(ErrForcedFailure, err)
	// 	c.Empty(out)

	// 	DeactiveForceFailure(fclient)
	// }
}

func TestDeleteItem(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	// if _, ok := client.(*Client); ok {
	// 	EmulateFailure(client, FailureConditionInternalServerError)

	// 	defer func() { EmulateFailure(client, FailureConditionNone) }()

	// 	output, forcedError := client.DeleteItem(context.Background(), input)
	// 	c.Nil(output)

	// 	var errInternalServerError *dynamodbtypes.InternalServerError

	// 	c.True(errors.As(forcedError, &errInternalServerError))
	// }
}

func TestDeleteItemWithConditions(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	c.Equal("Bulbasaur", output.Attributes["name"].(*dynamodbtypes.AttributeValueMemberS).Value)
}

func TestDescribeTable(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	// SetItemCollectionMetrics(client, itemCollectionMetrics)

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
	c.Contains(err.Error(), "number of conditions on the keys is invalid")

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

	for i := 0; i < batchRequestsLimit; i++ {
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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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

	srv.EmulateFailure(FailureConditionInternalServerError)
	defer srv.EmulateFailure(FailureConditionNone)

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: requests,
		},
	}

	output, err := client.BatchWriteItem(context.Background(), input)
	c.NoError(err)

	c.NotEmpty(output.UnprocessedItems)
}

func TestTransactWriteItems(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

	err := ensurePokemonTable(client)
	c.NoError(err)

	transactItems := []dynamodbtypes.TransactWriteItem{
		{
			Update: &dynamodbtypes.Update{
				Key: map[string]dynamodbtypes.AttributeValue{
					"id":     &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					":ntype": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
				},
				TableName:        aws.String(tableName),
				UpdateExpression: aws.String("SET second_type = :ntype"),
				ExpressionAttributeNames: map[string]string{
					"#id": "id",
				},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":update": &dynamodbtypes.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339)},
					":incr": &dynamodbtypes.AttributeValueMemberN{
						Value: "1",
					},
					":initial": &dynamodbtypes.AttributeValueMemberN{
						Value: "0",
					},
				},
			},
		},
	}

	writeItemsInput := &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	}

	output, err := client.TransactWriteItems(context.Background(), writeItemsInput)
	c.NoError(err)
	c.NotNil(output)

	srv.ActiveForceFailure()
	defer srv.DeactiveForceFailure()

	_, err = client.TransactWriteItems(context.Background(), writeItemsInput)
	c.Equal(ErrForcedFailure, err)
}

func TestCheckTableName(t *testing.T) {
	c := require.New(t)

	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

	_, err := srv.getTable(tableName)
	expected := "ResourceNotFoundException: Cannot do operations on a non-existent table"
	c.Equal(expected, err.Error())

	err = AddTable(context.Background(), client, "new-table", "partition", "range")
	c.NoError(err)

	_, err = srv.getTable("notATable")
	c.Equal(expected, err.Error())
}

func TestForceFailure(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

	err := ensurePokemonTable(client)
	c.NoError(err)

	srv.ActiveForceFailure()

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.EqualError(err, ErrForcedFailure.Error())

	srv.DeactiveForceFailure()

	err = createPokemon(client, pokemon{
		ID:   "001",
		Type: "grass",
		Name: "Bulbasaur",
	})
	c.NoError(err)
	c.NoError(err)
}

func TestUpdateItemAndQueryAfterUpsert(t *testing.T) {
	c := require.New(t)
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
	srv := ConnectTestServer()

	client := newClient(srv.URL)
	defer srv.DisconnectTestServer()

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
