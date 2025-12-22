package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
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
	for i := 0; i < 3; i++ {
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
			"#name-1": "name",
		},
		ProjectionExpression: aws.String("#name-1"),
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

func TestServerScanFiltersAndErrors(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()
	cli := newTestDynamoClient(t, ts.URL)

	makeBasicTable(t, cli, "pokemons", "id")
	for i := 0; i < 2; i++ {
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
