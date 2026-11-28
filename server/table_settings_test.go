package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func settingsThroughput(read, write int64) *ddbtypes.ProvisionedThroughput {
	return &ddbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(read), WriteCapacityUnits: aws.Int64(write)}
}

func settingsLimits(read, write int64) *ddbtypes.OnDemandThroughput {
	return &ddbtypes.OnDemandThroughput{MaxReadRequestUnits: aws.Int64(read), MaxWriteRequestUnits: aws.Int64(write)}
}

func describeSettings(t *testing.T, cli *dynamodb.Client, name string) *ddbtypes.TableDescription {
	t.Helper()

	out, err := cli.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	require.NoError(t, err)

	return out.Table
}

func requireSettingsError(t *testing.T, err error, code, message string) {
	t.Helper()

	var apiErr interface {
		ErrorCode() string
		ErrorMessage() string
	}

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.ErrorCode())
	require.Equal(t, message, apiErr.ErrorMessage())
}

func TestServerCreateTableStoresDeletionProtectionAndLimits(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	createDescribedServerTable(t, cli, "settings", &dynamodb.CreateTableInput{
		BillingMode:               ddbtypes.BillingModePayPerRequest,
		DeletionProtectionEnabled: aws.Bool(true),
		OnDemandThroughput:        settingsLimits(100, 50),
	})

	desc := describeSettings(t, cli, "settings")
	require.True(t, aws.ToBool(desc.DeletionProtectionEnabled))
	require.Equal(t, settingsLimits(100, 50), desc.OnDemandThroughput)

	_, err := cli.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("settings")})
	requireSettingsError(t, err, "ValidationException",
		"Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first.")
	describeSettings(t, cli, "settings")

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("settings"), DeletionProtectionEnabled: aws.Bool(false)})
	require.NoError(t, err)

	_, err = cli.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("settings")})
	require.NoError(t, err)

	// a limit outside the range is rejected and leaves no table behind
	_, err = cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("limits"),
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		OnDemandThroughput:   settingsLimits(0, 5),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
	})
	requireSettingsError(t, err, "ValidationException",
		"One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range")

	_, err = cli.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("limits")})
	require.Error(t, err)
}

func TestServerUpdateTableSettings(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()
	name := aws.String("settings")

	createDescribedServerTable(t, cli, "settings", &dynamodb.CreateTableInput{
		BillingMode:           ddbtypes.BillingModeProvisioned,
		ProvisionedThroughput: settingsThroughput(5, 5),
	})

	out, err := cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, ProvisionedThroughput: settingsThroughput(6, 4)})
	require.NoError(t, err)
	require.Equal(t, int64(6), aws.ToInt64(out.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
	require.Equal(t, int64(4), aws.ToInt64(describeSettings(t, cli, "settings").ProvisionedThroughput.WriteCapacityUnits))

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, ProvisionedThroughput: settingsThroughput(6, 4)})
	requireSettingsError(t, err, "ValidationException", "The requested throughput value equals the current value")

	out, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: name, BillingMode: ddbtypes.BillingModePayPerRequest, DeletionProtectionEnabled: aws.Bool(true), OnDemandThroughput: settingsLimits(10, 20),
	})
	require.NoError(t, err)
	require.Equal(t, ddbtypes.BillingModePayPerRequest, out.TableDescription.BillingModeSummary.BillingMode)
	require.Equal(t, settingsLimits(10, 20), out.TableDescription.OnDemandThroughput)

	desc := describeSettings(t, cli, "settings")
	require.Equal(t, ddbtypes.BillingModePayPerRequest, desc.BillingModeSummary.BillingMode)
	require.Equal(t, int64(0), aws.ToInt64(desc.ProvisionedThroughput.ReadCapacityUnits))
	require.True(t, aws.ToBool(desc.DeletionProtectionEnabled))
	require.Equal(t, settingsLimits(10, 20), desc.OnDemandThroughput)

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, BillingMode: ddbtypes.BillingModeProvisioned})
	requireSettingsError(t, err, "ValidationException", "No provisioned throughput specified for the table")

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, BillingMode: ddbtypes.BillingModeProvisioned, ProvisionedThroughput: settingsThroughput(2, 3)})
	require.NoError(t, err)

	desc = describeSettings(t, cli, "settings")
	require.Equal(t, ddbtypes.BillingModeProvisioned, desc.BillingModeSummary.BillingMode)
	require.Equal(t, int64(2), aws.ToInt64(desc.ProvisionedThroughput.ReadCapacityUnits))
	require.Nil(t, desc.OnDemandThroughput)

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, ProvisionedThroughput: settingsThroughput(0, 0)})
	requireSettingsError(t, err, "ValidationException", "Table read and write throughput must be at least 1, and cannot be null")

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, BillingMode: "FOO"})
	requireSettingsError(t, err, "ValidationException",
		"1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]")
}

func TestServerUpdateTableNothingToUpdate(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	makeBasicTable(t, cli, "pokemons", "id")

	_, err := cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("pokemons")})
	requireSettingsError(t, err, "ValidationException", "Nothing to update")

	// a setting minidyn does not model is still something to update
	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("pokemons"), TableClass: ddbtypes.TableClassStandardInfrequentAccess})
	require.NoError(t, err)

	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("nope"), DeletionProtectionEnabled: aws.Bool(true)})
	requireSettingsError(t, err, "ResourceNotFoundException", "Cannot do operations on a non-existent table")
}

func TestServerUpdateTableIndexThroughput(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:             aws.String("indexed"),
		BillingMode:           ddbtypes.BillingModeProvisioned,
		ProvisionedThroughput: settingsThroughput(5, 5),
		KeySchema:             []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{{
			IndexName:             aws.String("by-gk"),
			KeySchema:             []ddbtypes.KeySchemaElement{{AttributeName: aws.String("gk"), KeyType: ddbtypes.KeyTypeHash}},
			Projection:            &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			ProvisionedThroughput: settingsThroughput(5, 5),
		}},
	})
	require.NoError(t, err)

	update := func(pt *ddbtypes.ProvisionedThroughput, table *ddbtypes.ProvisionedThroughput) error {
		_, err := cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:             aws.String("indexed"),
			ProvisionedThroughput: table,
			GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{
				{Update: &ddbtypes.UpdateGlobalSecondaryIndexAction{IndexName: aws.String("by-gk"), ProvisionedThroughput: pt}},
			},
		})

		return err
	}

	require.NoError(t, update(settingsThroughput(8, 9), nil))

	gsi := describeSettings(t, cli, "indexed").GlobalSecondaryIndexes[0]
	require.Equal(t, int64(8), aws.ToInt64(gsi.ProvisionedThroughput.ReadCapacityUnits))
	require.Equal(t, int64(9), aws.ToInt64(gsi.ProvisionedThroughput.WriteCapacityUnits))

	requireSettingsError(t, update(settingsThroughput(8, 9), nil), "ValidationException", "The requested throughput value equals the current value | index: by-gk; table: indexed")
	requireSettingsError(t, update(settingsThroughput(0, 9), nil), "ValidationException",
		"Global Secondary Index's read and write throughput must be at least 1, and cannot be null | index: by-gk; table: indexed")

	// the index problem comes first, and nothing of the request applies
	requireSettingsError(t, update(settingsThroughput(0, 9), settingsThroughput(7, 7)), "ValidationException",
		"Global Secondary Index's read and write throughput must be at least 1, and cannot be null | index: by-gk; table: indexed")
	require.Equal(t, int64(5), aws.ToInt64(describeSettings(t, cli, "indexed").ProvisionedThroughput.ReadCapacityUnits))
}

func TestServerCreateTableBillingMode(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	create := func(name string, mode ddbtypes.BillingMode) error {
		_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:             aws.String(name),
			BillingMode:           mode,
			ProvisionedThroughput: settingsThroughput(5, 5),
			KeySchema:             []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
			AttributeDefinitions:  []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		})

		return err
	}

	// an unknown mode is rejected and leaves no table (DynamoDB Local answers a 500)
	requireSettingsError(t, create("bad", "FOO"), "ValidationException",
		"1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]")

	_, err := cli.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("bad")})
	require.Error(t, err)

	// no mode is a provisioned table, with no billing mode summary
	require.NoError(t, create("none", ""))
	require.Nil(t, describeSettings(t, cli, "none").BillingModeSummary)
}

func TestServerOneSidedOnDemandThroughput(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	// the API reference: "you must specify MaxReadRequestUnits, MaxWriteRequestUnits, or both"
	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("one-sided"),
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		OnDemandThroughput:   &ddbtypes.OnDemandThroughput{MaxReadRequestUnits: aws.Int64(5)},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
	})
	require.NoError(t, err)
	require.Equal(t, settingsLimits(5, -1), describeSettings(t, cli, "one-sided").OnDemandThroughput)

	// the side the request gives is replaced, the other one stays
	_, err = cli.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:          aws.String("one-sided"),
		OnDemandThroughput: &ddbtypes.OnDemandThroughput{MaxWriteRequestUnits: aws.Int64(7)},
	})
	require.NoError(t, err)
	require.Equal(t, settingsLimits(5, 7), describeSettings(t, cli, "one-sided").OnDemandThroughput)
}
