package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func createDescribedServerTable(t *testing.T, cli *dynamodb.Client, name string, in *dynamodb.CreateTableInput) {
	t.Helper()

	in.TableName = aws.String(name)
	in.KeySchema = []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}}
	in.AttributeDefinitions = []ddbtypes.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
	}

	_, err := cli.CreateTable(context.Background(), in)
	require.NoError(t, err)
}

func TestServerTableDescriptionFields(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	createDescribedServerTable(t, cli, "provisioned", &dynamodb.CreateTableInput{
		BillingMode:           ddbtypes.BillingModeProvisioned,
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(3), WriteCapacityUnits: aws.Int64(2)},
	})
	createDescribedServerTable(t, cli, "ondemand", &dynamodb.CreateTableInput{BillingMode: ddbtypes.BillingModePayPerRequest})

	prov, err := cli.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("provisioned")})
	require.NoError(t, err)
	require.Equal(t, ddbtypes.TableStatusActive, prov.Table.TableStatus)
	require.Len(t, prov.Table.AttributeDefinitions, 1)
	require.Equal(t, "pk", aws.ToString(prov.Table.AttributeDefinitions[0].AttributeName))
	require.Equal(t, ddbtypes.ScalarAttributeTypeS, prov.Table.AttributeDefinitions[0].AttributeType)
	require.Nil(t, prov.Table.BillingModeSummary)
	require.Equal(t, int64(3), aws.ToInt64(prov.Table.ProvisionedThroughput.ReadCapacityUnits))
	require.Equal(t, int64(0), aws.ToInt64(prov.Table.ProvisionedThroughput.NumberOfDecreasesToday))
	require.NotNil(t, prov.Table.ProvisionedThroughput.NumberOfDecreasesToday)
	require.Equal(t, int64(2), aws.ToInt64(prov.Table.ProvisionedThroughput.WriteCapacityUnits))
	require.NotNil(t, prov.Table.DeletionProtectionEnabled)
	require.False(t, *prov.Table.DeletionProtectionEnabled)

	od, err := cli.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("ondemand")})
	require.NoError(t, err)
	require.Equal(t, ddbtypes.BillingModePayPerRequest, od.Table.BillingModeSummary.BillingMode)
	require.Equal(t, int64(0), aws.ToInt64(od.Table.ProvisionedThroughput.ReadCapacityUnits))
	require.Equal(t, int64(0), aws.ToInt64(od.Table.ProvisionedThroughput.WriteCapacityUnits))

	deleted, err := cli.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("provisioned")})
	require.NoError(t, err)
	require.Equal(t, ddbtypes.TableStatusActive, deleted.TableDescription.TableStatus)
	require.Equal(t, int64(3), aws.ToInt64(deleted.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
}

func TestServerUpdateTableDeleteIndexListsDeletingIndex(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")
	createServerTestGlobalSecondaryIndex(t, cli, "pokemons", "by-type")

	out, err := cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("pokemons"),
		GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{
			{Delete: &ddbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("by-type")}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.TableDescription.GlobalSecondaryIndexes, 1)
	require.Equal(t, "by-type", aws.ToString(out.TableDescription.GlobalSecondaryIndexes[0].IndexName))
	require.Equal(t, ddbtypes.IndexStatusDeleting, out.TableDescription.GlobalSecondaryIndexes[0].IndexStatus)

	desc, err := cli.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String("pokemons")})
	require.NoError(t, err)
	require.Empty(t, desc.Table.GlobalSecondaryIndexes)
}

func TestServerTableDescriptionOmitsUnsetFields(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	req, err := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader(`{"TableName":"pokemons"}`))
	require.NoError(t, err)

	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotContains(t, string(body), "null")
	require.Contains(t, string(body), `"TableStatus":"ACTIVE"`)
}

func TestUnsetFieldsOmittedMarshalError(t *testing.T) {
	_, err := json.Marshal(unsetFieldsOmitted{make(chan int)})
	require.Error(t, err)
}

func TestDescribeTableJSONNil(t *testing.T) {
	require.Nil(t, describeTableJSON(nil))
}

func TestUnsetFieldsOmittedKeepsSetValues(t *testing.T) {
	out, err := json.Marshal(unsetFieldsOmitted{map[string]any{
		"zero":  0,
		"no":    false,
		"gone":  nil,
		"blank": "",
		"list":  []any{map[string]any{"keep": "x", "drop": nil}},
	}})
	require.NoError(t, err)
	require.JSONEq(t, `{"zero":0,"no":false,"list":[{"keep":"x"}]}`, string(out))
}
