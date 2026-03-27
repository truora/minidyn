package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/require"
)

func newTestDynamoClient(t *testing.T, url string) *dynamodb.Client {
	t.Helper()

	ctx := context.Background()
	resolver := dynamodb.EndpointResolverFromURL(url)
	httpClient := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true, // avoid reuse warnings
			DisableCompression:  true,
			MaxIdleConns:        1,
			MaxIdleConnsPerHost: 1,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 5 * time.Second,
			}).DialContext,
		},
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...any) (aws.Endpoint, error) {
					if service == dynamodb.ServiceID {
						return resolver.ResolveEndpoint(region, dynamodb.EndpointResolverOptions{})
					}
					return aws.Endpoint{}, &aws.EndpointNotFoundError{}
				},
			),
		),
		config.WithHTTPClient(httpClient),
		config.WithLogger(logging.Nop{}),
		config.WithClientLogMode(0),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				SessionToken:    "test",
				Source:          "test",
			},
		}),
		config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	require.NoError(t, err)

	return dynamodb.NewFromConfig(cfg)
}

func TestServerCRUDWithSDKv2(t *testing.T) {
	srv := NewServer()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	// put item
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "25"},
			"n":  &ddbtypes.AttributeValueMemberS{Value: "pikachu"},
		},
	})
	require.NoError(t, err)

	// get item
	out, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "25"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "pikachu", out.Item["n"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestServerQueryGSIWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	put := func(id, typ string) {
		_, putErr := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("pokemons"),
			Item: map[string]ddbtypes.AttributeValue{
				"id":   &ddbtypes.AttributeValueMemberS{Value: id},
				"type": &ddbtypes.AttributeValueMemberS{Value: typ},
			},
		})
		require.NoError(t, putErr)
	}
	put("25", "electric")
	put("26", "electric")

	qOut, err := cli.Query(context.Background(), &dynamodb.QueryInput{
		TableName: aws.String("pokemons"),
		IndexName: aws.String("by-type"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":type": &ddbtypes.AttributeValueMemberS{Value: "electric"},
		},
		KeyConditionExpression: aws.String("#type = :type"),
	})
	require.NoError(t, err)
	require.Len(t, qOut.Items, 2)
}

func TestServerClearTable(t *testing.T) {
	c := require.New(t)
	srv := NewServer()
	ctx := context.Background()
	tableName := "pokemons"

	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
	})
	c.NoError(err)

	_, err = srv.client.PutItem(ctx, &PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]*AttributeValue{
			"id":   {S: aws.String("001")},
			"type": {S: aws.String("grass")},
			"name": {S: aws.String("Bulbasaur")},
		},
	})
	c.NoError(err)

	getOut, err := srv.client.GetItem(ctx, &GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*AttributeValue{
			"id": {S: aws.String("001")},
		},
	})
	c.NoError(err)
	c.Equal("Bulbasaur", aws.ToString(getOut.Item["name"].S))

	queryOut, err := srv.client.Query(ctx, &QueryInput{
		TableName: aws.String(tableName),
		IndexName: aws.String("by-type"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		ExpressionAttributeValues: map[string]*AttributeValue{
			":type": {S: aws.String("grass")},
		},
		KeyConditionExpression: aws.String("#type = :type"),
	})
	c.NoError(err)
	c.Len(queryOut.Items, 1)

	err = srv.ClearTable(tableName)
	c.NoError(err)

	getOut, err = srv.client.GetItem(ctx, &GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*AttributeValue{
			"id": {S: aws.String("001")},
		},
	})
	c.NoError(err)
	c.Empty(getOut.Item)

	queryOut, err = srv.client.Query(ctx, &QueryInput{
		TableName: aws.String(tableName),
		IndexName: aws.String("by-type"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		ExpressionAttributeValues: map[string]*AttributeValue{
			":type": {S: aws.String("grass")},
		},
		KeyConditionExpression: aws.String("#type = :type"),
	})
	c.NoError(err)
	c.Empty(queryOut.Items)
}

func TestServerClearAllTables(t *testing.T) {
	c := require.New(t)
	srv := NewServer()
	ctx := context.Background()

	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	_, err = srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("trainers"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	_, err = srv.client.PutItem(ctx, &PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]*AttributeValue{
			"id":   {S: aws.String("001")},
			"type": {S: aws.String("grass")},
			"name": {S: aws.String("Bulbasaur")},
		},
	})
	c.NoError(err)

	_, err = srv.client.PutItem(ctx, &PutItemInput{
		TableName: aws.String("trainers"),
		Item: map[string]*AttributeValue{
			"id":   {S: aws.String("ash")},
			"name": {S: aws.String("Ash Ketchum")},
		},
	})
	c.NoError(err)

	err = srv.ClearAllTables()
	c.NoError(err)

	c.Len(srv.client.tables, 2)

	getOut, err := srv.client.GetItem(ctx, &GetItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]*AttributeValue{
			"id": {S: aws.String("001")},
		},
	})
	c.NoError(err)
	c.Empty(getOut.Item)

	queryOut, err := srv.client.Query(ctx, &QueryInput{
		TableName: aws.String("pokemons"),
		IndexName: aws.String("by-type"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		ExpressionAttributeValues: map[string]*AttributeValue{
			":type": {S: aws.String("grass")},
		},
		KeyConditionExpression: aws.String("#type = :type"),
	})
	c.NoError(err)
	c.Empty(queryOut.Items)

	getOut, err = srv.client.GetItem(ctx, &GetItemInput{
		TableName: aws.String("trainers"),
		Key: map[string]*AttributeValue{
			"id": {S: aws.String("ash")},
		},
	})
	c.NoError(err)
	c.Empty(getOut.Item)
}

func TestServerReset(t *testing.T) {
	c := require.New(t)
	srv := NewServer()
	ctx := context.Background()

	// create table with GSI and data
	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	_, err = srv.client.PutItem(ctx, &PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]*AttributeValue{
			"id":   {S: aws.String("001")},
			"type": {S: aws.String("grass")},
		},
	})
	c.NoError(err)
	c.Equal(1, len(srv.client.tables))

	err = srv.Reset()
	c.NoError(err)
	c.Empty(srv.client.tables)
}

func TestServerConditionalPutFailsWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	_, err = cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		ConditionExpression: aws.String("attribute_not_exists(id)"),
	})
	var condErr *ddbtypes.ConditionalCheckFailedException
	require.ErrorAs(t, err, &condErr)
}

func TestServerBatchWriteWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbtypes.WriteRequest{
			"pokemons": {
				{PutRequest: &ddbtypes.PutRequest{Item: map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
				}}},
				{PutRequest: &ddbtypes.PutRequest{Item: map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{Value: "2"},
				}}},
			},
		},
	})
	require.NoError(t, err)

	out, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "2"},
		},
	})

	require.NoError(t, err)
	require.Equal(t, "2", out.Item["id"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestServerUpdateItemWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id":  &ddbtypes.AttributeValueMemberS{Value: "1"},
			"lvl": &ddbtypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	upd, err := cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String("pokemons"),
		Key:              map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
		UpdateExpression: aws.String("SET lvl = :n"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":n": &ddbtypes.AttributeValueMemberN{Value: "2"},
		},
		ReturnValues: ddbtypes.ReturnValueAllNew,
	})
	require.NoError(t, err)

	require.Equal(t, "2", upd.Attributes["lvl"].(*ddbtypes.AttributeValueMemberN).Value)
}

func TestServerDeleteItemReturnsOldWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id":   &ddbtypes.AttributeValueMemberS{Value: "1"},
			"name": &ddbtypes.AttributeValueMemberS{Value: "pikachu"},
		},
	})
	require.NoError(t, err)

	del, err := cli.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName:    aws.String("pokemons"),
		Key:          map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
		ReturnValues: ddbtypes.ReturnValueAllOld,
	})
	require.NoError(t, err)
	require.Equal(t, "pikachu", del.Attributes["name"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestServerScanWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	for i := range 3 {
		id := aws.String(fmt.Sprintf("%d", i))
		_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("pokemons"),
			Item: map[string]ddbtypes.AttributeValue{
				"id": &ddbtypes.AttributeValueMemberS{Value: *id},
			},
		})
		require.NoError(t, err)
	}

	scan, err := cli.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String("pokemons"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), scan.Count)
}

func TestServerDescribeAndUpdateTableWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String("pokemons"),
	})
	require.NoError(t, err)

	_, err = cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("pokemons"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{
			{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			}},
		},
	})
	require.NoError(t, err)
}

func TestServerAddLSIWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("ts"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("ts"), AttributeType: ddbtypes.ScalarAttributeTypeN},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		LocalSecondaryIndexes: []ddbtypes.LocalSecondaryIndex{
			{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)
}

func TestServerDeleteTableWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String("pokemons"),
	})
	require.NoError(t, err)

	_, err = cli.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String("pokemons"),
	})
	var notFound *ddbtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

func TestServerEmulateFailureWithSDKv2(t *testing.T) {
	s := NewServer()

	ts := httptest.NewServer(s)
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	s.EmulateFailure(FailureConditionInternalServerError)

	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	var internal *ddbtypes.InternalServerError
	require.ErrorAs(t, err, &internal)

	s.EmulateFailure(FailureConditionNone)
	_, err = cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "2"},
		},
	})
	require.NoError(t, err)
}

func TestServerGetItemAttributeNameValidation(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id":   &ddbtypes.AttributeValueMemberS{Value: "1"},
			"name": &ddbtypes.AttributeValueMemberS{Value: "pikachu"},
		},
	})
	require.NoError(t, err)

	out, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		ExpressionAttributeNames: map[string]string{
			"#n": "name",
		},
		ProjectionExpression: aws.String("#n"),
	})
	require.NoError(t, err)
	require.Equal(t, "pikachu", out.Item["name"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestServerPutItemWithConditionsValidation(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	// first put ok
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	// second with unused expression attribute value
	_, err = cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		ConditionExpression: aws.String("attribute_not_exists(#type)"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":not_used": &ddbtypes.AttributeValueMemberNULL{Value: true},
		},
	})
	require.NoError(t, err)
}

func TestServerUpdateExpressionsAddRemoveDelete(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id":    &ddbtypes.AttributeValueMemberS{Value: "1"},
			"lvl":   &ddbtypes.AttributeValueMemberN{Value: "0"},
			"moves": &ddbtypes.AttributeValueMemberSS{Value: []string{"Growl", "Tackle"}},
			"local": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
				&ddbtypes.AttributeValueMemberS{Value: "a"},
				&ddbtypes.AttributeValueMemberS{Value: "b"},
			}},
		},
	})
	require.NoError(t, err)

	// ADD
	_, err = cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String("pokemons"),
		Key:              map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
		UpdateExpression: aws.String("ADD lvl :one"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":one": &ddbtypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	// REMOVE
	_, err = cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String("pokemons"),
		Key:              map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
		UpdateExpression: aws.String("REMOVE #l[0],#l[1]"),
		ExpressionAttributeNames: map[string]string{
			"#l": "local",
		},
	})
	require.NoError(t, err)

	// DELETE
	_, err = cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String("pokemons"),
		Key:              map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
		UpdateExpression: aws.String("DELETE moves :move"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":move": &ddbtypes.AttributeValueMemberSS{Value: []string{"Growl"}},
		},
	})
	require.NoError(t, err)
}

func TestServerQueryPaginationAndFilters(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	// table + GSI
	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("by-type"),
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeRange},
				},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	put := func(id, typ string) {
		_, putErr := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("pokemons"),
			Item: map[string]ddbtypes.AttributeValue{
				"id":   &ddbtypes.AttributeValueMemberS{Value: id},
				"type": &ddbtypes.AttributeValueMemberS{Value: typ},
				"name": &ddbtypes.AttributeValueMemberS{Value: "n-" + id},
			},
		})
		require.NoError(t, putErr)
	}
	put("001", "grass")
	put("002", "grass")
	put("003", "grass")

	input := &dynamodb.QueryInput{
		TableName:              aws.String("pokemons"),
		IndexName:              aws.String("by-type"),
		KeyConditionExpression: aws.String("#type = :type"),
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":type": &ddbtypes.AttributeValueMemberS{Value: "grass"},
		},
		Limit: aws.Int32(1),
	}
	out, err := cli.Query(context.Background(), input)
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotEmpty(t, out.LastEvaluatedKey)

	input.ExclusiveStartKey = out.LastEvaluatedKey
	out, err = cli.Query(context.Background(), input)
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	// filter expression reduces results
	input.FilterExpression = aws.String("#name = :name")
	input.ExpressionAttributeNames["#name"] = "name"
	input.ExpressionAttributeValues[":name"] = &ddbtypes.AttributeValueMemberS{Value: "n-003"}
	out, err = cli.Query(context.Background(), input)
	require.NoError(t, err)
	require.Len(t, out.Items, 0) // because start key advanced
}

func TestServerServeHTTPMethodNotAllowed(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestServerServeHTTPUnsupportedOperation(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UnknownOperation")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

type errCloseReader struct{ *strings.Reader }

func (e *errCloseReader) Close() error { return errors.New("close failed") }

var _ io.ReadCloser = (*errCloseReader)(nil)

func TestServerServeHTTPBodyCloseError(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodPost, "/", &errCloseReader{Reader: strings.NewReader("{}")})
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UnknownOperation")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestServerServeHTTPInvalidJSONBody(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"TableName":`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

type failResponseWriter struct {
	hdr  http.Header
	code int
}

func (f *failResponseWriter) Header() http.Header {
	if f.hdr == nil {
		f.hdr = make(http.Header)
	}
	return f.hdr
}

func (f *failResponseWriter) WriteHeader(code int) {
	f.code = code
}

func (f *failResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestServerServeHTTPJSONEncodeError(t *testing.T) {
	srv := NewServer()
	cli := srv.client
	tableName := "encode-fail-table"
	_, err := cli.CreateTable(context.Background(), &CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`{"TableName":"`+tableName+`"}`,
	))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")
	rec := &failResponseWriter{}
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.code)
}

func TestServerNilServerGuards(t *testing.T) {
	var s *Server

	s.EmulateFailure(FailureConditionNone)
	require.ErrorIs(t, s.ClearTable("any"), ErrServerNotInitialized)
	require.ErrorIs(t, s.ClearAllTables(), ErrServerNotInitialized)
	require.ErrorIs(t, s.Reset(), ErrServerNotInitialized)
}

func TestServerEmulateFailureDeprecated(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s)
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	s.EmulateFailure(FailureConditionDeprecated)
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "InternalFailure", apiErr.ErrorCode())
	require.Contains(t, apiErr.ErrorMessage(), ErrForcedFailure.Error())

	s.EmulateFailure(FailureConditionNone)
	_, err = cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "2"},
		},
	})
	require.NoError(t, err)
}

func TestServerCreateTableAlreadyExists(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	var inUse *ddbtypes.ResourceInUseException
	require.ErrorAs(t, err, &inUse)
}

func TestServerPutItemMissingPartitionKeyValidation(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"name": &ddbtypes.AttributeValueMemberS{Value: "no-id"},
		},
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
}

func TestServerUpdateItemConditionalFailureReturnsAllOldItem(t *testing.T) {
	// In-memory client: HTTP error responses only encode __type and message, so Item is
	// not round-tripped through the SDK; mapTypesMapToDDBAttributeValue is exercised here.
	srv := NewServer()
	ctx := context.Background()

	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = srv.client.PutItem(ctx, &PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]*AttributeValue{
			"id":   {S: aws.String("001")},
			"name": {S: aws.String("Bulbasaur")},
			"lvl":  {N: aws.String("5")},
			"tags": {SS: []*string{aws.String("grass"), aws.String("poison")}},
			"meta": {M: map[string]*AttributeValue{
				"region": {S: aws.String("kanto")},
			}},
		},
	})
	require.NoError(t, err)

	_, err = srv.client.UpdateItem(ctx, &UpdateItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]*AttributeValue{
			"id": {S: aws.String("001")},
		},
		ConditionExpression:                 aws.String("attribute_not_exists(#id)"),
		UpdateExpression:                    aws.String("SET #lvl = :lvl"),
		ReturnValues:                        ddbtypes.ReturnValueUpdatedNew,
		ReturnValuesOnConditionCheckFailure: ddbtypes.ReturnValuesOnConditionCheckFailureAllOld,
		ExpressionAttributeNames: map[string]string{
			"#id":  "id",
			"#lvl": "lvl",
		},
		ExpressionAttributeValues: map[string]*AttributeValue{
			":lvl": {N: aws.String("99")},
		},
	})

	var cond *ddbtypes.ConditionalCheckFailedException
	require.ErrorAs(t, err, &cond)
	require.NotNil(t, cond.Item)
	require.Equal(t, "001", cond.Item["id"].(*ddbtypes.AttributeValueMemberS).Value)
	require.Equal(t, "Bulbasaur", cond.Item["name"].(*ddbtypes.AttributeValueMemberS).Value)
	require.Equal(t, "5", cond.Item["lvl"].(*ddbtypes.AttributeValueMemberN).Value)
	require.ElementsMatch(t, []string{"grass", "poison"}, cond.Item["tags"].(*ddbtypes.AttributeValueMemberSS).Value)
	require.Equal(t, "kanto", cond.Item["meta"].(*ddbtypes.AttributeValueMemberM).Value["region"].(*ddbtypes.AttributeValueMemberS).Value)
}

func TestServerBatchWriteItemUnprocessedOnEmulatedInternalError(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s)
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	s.EmulateFailure(FailureConditionInternalServerError)

	out, err := cli.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbtypes.WriteRequest{
			"pokemons": {
				{PutRequest: &ddbtypes.PutRequest{Item: map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{Value: "a"},
				}}},
				{PutRequest: &ddbtypes.PutRequest{Item: map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{Value: "b"},
				}}},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.UnprocessedItems["pokemons"], 2)

	s.EmulateFailure(FailureConditionNone)

	_, err = cli.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbtypes.WriteRequest{
			"pokemons": {
				{PutRequest: &ddbtypes.PutRequest{Item: map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{Value: "ok"},
				}}},
			},
		},
	})
	require.NoError(t, err)
}

func TestServerGetItemKeyValidationError(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"wrong": &ddbtypes.AttributeValueMemberS{Value: "x"},
		},
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
}

func TestServerDeleteItemMissingTable(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	_, err := cli.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("nope"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	var notFound *ddbtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

func TestServerDeleteItemKeyValidationError(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"wrong": &ddbtypes.AttributeValueMemberS{Value: "x"},
		},
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
}

func TestServerDeleteItemEmulateInternalError(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s)
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	s.EmulateFailure(FailureConditionInternalServerError)

	_, err := cli.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	var internal *ddbtypes.InternalServerError
	require.ErrorAs(t, err, &internal)

	s.EmulateFailure(FailureConditionNone)
}

func TestServerDeleteItemWithExpressionAttributeNames(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	_, err = cli.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
		ConditionExpression: aws.String("attribute_exists(#id)"),
		ExpressionAttributeNames: map[string]string{
			"#id": "id",
		},
	})
	require.NoError(t, err)
}

func TestServerUpdateTableDeleteMissingGSI(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("pokemons"),
		GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{
			{Delete: &ddbtypes.DeleteGlobalSecondaryIndexAction{
				IndexName: aws.String("no-such-index"),
			}},
		},
	})
	var notFound *ddbtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

func TestServerCreateTableWithProvisionedThroughput(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("counters"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModeProvisioned,
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)
}

func TestServerBatchWriteItemDeleteRequest(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)

	_, err = cli.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbtypes.WriteRequest{
			"pokemons": {
				{DeleteRequest: &ddbtypes.DeleteRequest{
					Key: map[string]ddbtypes.AttributeValue{
						"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	out, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("pokemons"),
		Key: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.Empty(t, out.Item)
}

func TestServerScanFiltersAndErrors(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	for i := range 2 {
		_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String("pokemons"),
			Item: map[string]ddbtypes.AttributeValue{
				"id":   &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("%d", i)},
				"type": &ddbtypes.AttributeValueMemberS{Value: "grass"},
			},
		})
		require.NoError(t, err)
	}

	// scan ok
	_, err := cli.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String("pokemons"),
	})
	require.NoError(t, err)
}

func TestClientGetItemMissingTableEmulatedErrorAndInvalidKeyMap(t *testing.T) {
	ctx := context.Background()
	s := NewServer()
	cli := s.client

	_, err := cli.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("items"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = cli.GetItem(ctx, &GetItemInput{
		TableName: aws.String("no-such-table"),
		Key:       map[string]*AttributeValue{"id": {S: aws.String("1")}},
	})
	var notFound *ddbtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)

	s.EmulateFailure(FailureConditionInternalServerError)
	_, err = cli.GetItem(ctx, &GetItemInput{
		TableName: aws.String("items"),
		Key:       map[string]*AttributeValue{"id": {S: aws.String("1")}},
	})
	var internal *ddbtypes.InternalServerError
	require.ErrorAs(t, err, &internal)
	s.EmulateFailure(FailureConditionNone)

	dupA, dupB := "x", "x"
	_, err = cli.GetItem(ctx, &GetItemInput{
		TableName: aws.String("items"),
		Key: map[string]*AttributeValue{
			"id": {SS: []*string{&dupA, &dupB}},
		},
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
	require.Contains(t, apiErr.ErrorMessage(), "duplicates")
}

func TestClientQueryScanMissingTableEmulatedErrorAndInvalidExprValues(t *testing.T) {
	ctx := context.Background()
	s := NewServer()
	cli := s.client

	_, err := cli.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("items"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = cli.Query(ctx, &QueryInput{
		TableName: aws.String("missing"),
	})
	var notFoundQ *ddbtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFoundQ)

	_, err = cli.Scan(ctx, &ScanInput{
		TableName: aws.String("missing"),
	})
	var notFoundS *ddbtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFoundS)

	s.EmulateFailure(FailureConditionInternalServerError)
	_, err = cli.Query(ctx, &QueryInput{TableName: aws.String("items")})
	var internalQ *ddbtypes.InternalServerError
	require.ErrorAs(t, err, &internalQ)

	_, err = cli.Scan(ctx, &ScanInput{TableName: aws.String("items")})
	var internalS *ddbtypes.InternalServerError
	require.ErrorAs(t, err, &internalS)
	s.EmulateFailure(FailureConditionNone)

	dupA, dupB := "p", "p"
	_, err = cli.Query(ctx, &QueryInput{
		TableName: aws.String("items"),
		ExpressionAttributeValues: map[string]*AttributeValue{
			":v": {SS: []*string{&dupA, &dupB}},
		},
	})
	var valErrQ smithy.APIError
	require.ErrorAs(t, err, &valErrQ)
	require.Equal(t, "ValidationException", valErrQ.ErrorCode())

	_, err = cli.Scan(ctx, &ScanInput{
		TableName: aws.String("items"),
		ExpressionAttributeValues: map[string]*AttributeValue{
			":v": {SS: []*string{&dupA, &dupB}},
		},
	})
	var valErrS smithy.APIError
	require.ErrorAs(t, err, &valErrS)
	require.Equal(t, "ValidationException", valErrS.ErrorCode())
}

func TestClientQueryScanIndexForwardFalseAndScanWithLimitAndStartKey(t *testing.T) {
	ctx := context.Background()
	cli := NewClient()

	_, err := cli.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pk"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("h"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("r"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("h"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("r"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	for _, r := range []string{"a", "b", "c"} {
		_, putErr := cli.PutItem(ctx, &PutItemInput{
			TableName: aws.String("pk"),
			Item: map[string]*AttributeValue{
				"h": {S: aws.String("1")},
				"r": {S: aws.String(r)},
			},
		})
		require.NoError(t, putErr)
	}

	qOut, err := cli.Query(ctx, &QueryInput{
		TableName:              aws.String("pk"),
		KeyConditionExpression: aws.String("h = :h"),
		ExpressionAttributeValues: map[string]*AttributeValue{
			":h": {S: aws.String("1")},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, qOut.Items, 1)
	require.Equal(t, "c", aws.ToString(qOut.Items[0]["r"].S))
	require.NotEmpty(t, qOut.LastEvaluatedKey)

	scanOut, err := cli.Scan(ctx, &ScanInput{
		TableName: aws.String("pk"),
		Limit:     aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, scanOut.Items, 2)
	require.Equal(t, int32(2), scanOut.Count)
	require.NotEmpty(t, scanOut.LastEvaluatedKey)

	scan2, err := cli.Scan(ctx, &ScanInput{
		TableName:         aws.String("pk"),
		Limit:             aws.Int32(10),
		ExclusiveStartKey: scanOut.LastEvaluatedKey,
	})
	require.NoError(t, err)
	require.Len(t, scan2.Items, 1)
}

// helpers

func makeBasicTable(t *testing.T, cli *dynamodb.Client, table, hashKey string) {
	t.Helper()
	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String(hashKey), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String(hashKey), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}
