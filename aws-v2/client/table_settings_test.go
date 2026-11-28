package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func settingsThroughput(read, write int64) *dynamodbtypes.ProvisionedThroughput {
	return &dynamodbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(read), WriteCapacityUnits: aws.Int64(write)}
}

func settingsLimits(read, write int64) *dynamodbtypes.OnDemandThroughput {
	return &dynamodbtypes.OnDemandThroughput{MaxReadRequestUnits: aws.Int64(read), MaxWriteRequestUnits: aws.Int64(write)}
}

func describeSettings(t *testing.T, client FakeClient, name string) *dynamodbtypes.TableDescription {
	t.Helper()

	out, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
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

func TestCreateTableStoresDeletionProtectionAndLimits(t *testing.T) {
	client := NewClient()
	ctx := context.Background()

	created := createDescribedTable(t, client, "settings", &dynamodb.CreateTableInput{
		BillingMode:               dynamodbtypes.BillingModePayPerRequest,
		DeletionProtectionEnabled: aws.Bool(true),
		OnDemandThroughput:        settingsLimits(100, 50),
	})
	require.True(t, aws.ToBool(created.TableDescription.DeletionProtectionEnabled))
	require.Equal(t, settingsLimits(100, 50), created.TableDescription.OnDemandThroughput)

	desc := describeSettings(t, client, "settings")
	require.True(t, aws.ToBool(desc.DeletionProtectionEnabled))
	require.Equal(t, settingsLimits(100, 50), desc.OnDemandThroughput)

	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("settings")})
	requireSettingsError(t, err, "ValidationException",
		"Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first.")
	describeSettings(t, client, "settings")

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("settings"), DeletionProtectionEnabled: aws.Bool(false)})
	require.NoError(t, err)

	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("settings")})
	require.NoError(t, err)

	// a limit outside the range is rejected and leaves no table behind
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("limits"),
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		OnDemandThroughput:   settingsLimits(0, 5),
		KeySchema:            []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
	})
	requireSettingsError(t, err, "ValidationException",
		"One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range")

	_, err = client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("limits")})
	require.Error(t, err)
}

func TestUpdateTableSettings(t *testing.T) {
	client := NewClient()
	ctx := context.Background()
	name := aws.String("settings")

	createDescribedTable(t, client, "settings", &dynamodb.CreateTableInput{
		BillingMode:           dynamodbtypes.BillingModeProvisioned,
		ProvisionedThroughput: settingsThroughput(5, 5),
	})

	out, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, ProvisionedThroughput: settingsThroughput(6, 4)})
	require.NoError(t, err)
	require.Equal(t, int64(6), aws.ToInt64(out.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
	require.Equal(t, int64(4), aws.ToInt64(describeSettings(t, client, "settings").ProvisionedThroughput.WriteCapacityUnits))

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, ProvisionedThroughput: settingsThroughput(6, 4)})
	requireSettingsError(t, err, "ValidationException", "The requested throughput value equals the current value")

	out, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: name, BillingMode: dynamodbtypes.BillingModePayPerRequest, DeletionProtectionEnabled: aws.Bool(true), OnDemandThroughput: settingsLimits(10, 20),
	})
	require.NoError(t, err)
	require.Equal(t, dynamodbtypes.BillingModePayPerRequest, out.TableDescription.BillingModeSummary.BillingMode)
	require.Equal(t, settingsLimits(10, 20), out.TableDescription.OnDemandThroughput)

	desc := describeSettings(t, client, "settings")
	require.Equal(t, dynamodbtypes.BillingModePayPerRequest, desc.BillingModeSummary.BillingMode)
	require.Equal(t, int64(0), aws.ToInt64(desc.ProvisionedThroughput.ReadCapacityUnits))
	require.True(t, aws.ToBool(desc.DeletionProtectionEnabled))
	require.Equal(t, settingsLimits(10, 20), desc.OnDemandThroughput)

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, BillingMode: dynamodbtypes.BillingModeProvisioned})
	requireSettingsError(t, err, "ValidationException", "No provisioned throughput specified for the table")

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, BillingMode: dynamodbtypes.BillingModeProvisioned, ProvisionedThroughput: settingsThroughput(2, 3)})
	require.NoError(t, err)

	desc = describeSettings(t, client, "settings")
	require.Equal(t, dynamodbtypes.BillingModeProvisioned, desc.BillingModeSummary.BillingMode)
	require.Equal(t, int64(2), aws.ToInt64(desc.ProvisionedThroughput.ReadCapacityUnits))
	require.Nil(t, desc.OnDemandThroughput)

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, ProvisionedThroughput: settingsThroughput(0, 0)})
	requireSettingsError(t, err, "ValidationException", "Table read and write throughput must be at least 1, and cannot be null")

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: name, BillingMode: "FOO"})
	requireSettingsError(t, err, "ValidationException",
		"1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]")
}

func TestUpdateTableNothingToUpdate(t *testing.T) {
	client := NewClient()
	ctx := context.Background()

	createDescribedTable(t, client, "pokemons", &dynamodb.CreateTableInput{BillingMode: dynamodbtypes.BillingModePayPerRequest})

	_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("pokemons")})
	requireSettingsError(t, err, "ValidationException", "Nothing to update")

	// a setting minidyn does not model is still something to update
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("pokemons"), TableClass: dynamodbtypes.TableClassStandardInfrequentAccess})
	require.NoError(t, err)

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("nope"), DeletionProtectionEnabled: aws.Bool(true)})
	requireSettingsError(t, err, "ResourceNotFoundException", "Cannot do operations on a non-existent table")
}

func TestUpdateTableIndexThroughput(t *testing.T) {
	client := NewClient()
	ctx := context.Background()

	createDescribedTable(t, client, "indexed", &dynamodb.CreateTableInput{
		BillingMode:           dynamodbtypes.BillingModeProvisioned,
		ProvisionedThroughput: settingsThroughput(5, 5),
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:             aws.String("by-gk"),
			KeySchema:             []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("gk"), KeyType: dynamodbtypes.KeyTypeHash}},
			Projection:            &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			ProvisionedThroughput: settingsThroughput(5, 5),
		}},
	})

	update := func(pt *dynamodbtypes.ProvisionedThroughput, table *dynamodbtypes.ProvisionedThroughput) error {
		_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:             aws.String("indexed"),
			ProvisionedThroughput: table,
			GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
				{Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{IndexName: aws.String("by-gk"), ProvisionedThroughput: pt}},
			},
		})

		return err
	}

	require.NoError(t, update(settingsThroughput(8, 9), nil))

	gsi := describeSettings(t, client, "indexed").GlobalSecondaryIndexes[0]
	require.Equal(t, int64(8), aws.ToInt64(gsi.ProvisionedThroughput.ReadCapacityUnits))
	require.Equal(t, int64(9), aws.ToInt64(gsi.ProvisionedThroughput.WriteCapacityUnits))

	requireSettingsError(t, update(settingsThroughput(8, 9), nil), "ValidationException", "The requested throughput value equals the current value | index: by-gk; table: indexed")
	requireSettingsError(t, update(settingsThroughput(0, 9), nil), "ValidationException",
		"Global Secondary Index's read and write throughput must be at least 1, and cannot be null | index: by-gk; table: indexed")

	// the index problem comes first, and nothing of the request applies
	requireSettingsError(t, update(settingsThroughput(0, 9), settingsThroughput(7, 7)), "ValidationException",
		"Global Secondary Index's read and write throughput must be at least 1, and cannot be null | index: by-gk; table: indexed")
	require.Equal(t, int64(5), aws.ToInt64(describeSettings(t, client, "indexed").ProvisionedThroughput.ReadCapacityUnits))
}

func TestCreateTableBillingMode(t *testing.T) {
	client := NewClient()
	ctx := context.Background()

	create := func(name string, mode dynamodbtypes.BillingMode) error {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:             aws.String(name),
			BillingMode:           mode,
			ProvisionedThroughput: settingsThroughput(5, 5),
			KeySchema:             []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
			AttributeDefinitions:  []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
		})

		return err
	}

	// an unknown mode is rejected and leaves no table (DynamoDB Local answers a 500)
	requireSettingsError(t, create("bad", "FOO"), "ValidationException",
		"1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]")

	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("bad")})
	require.Error(t, err)

	// no mode is a provisioned table, with no billing mode summary
	require.NoError(t, create("none", ""))
	require.Nil(t, describeSettings(t, client, "none").BillingModeSummary)
}

func TestOneSidedOnDemandThroughput(t *testing.T) {
	client := NewClient()
	ctx := context.Background()

	// the API reference: "you must specify MaxReadRequestUnits, MaxWriteRequestUnits, or both"
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("one-sided"),
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		OnDemandThroughput:   &dynamodbtypes.OnDemandThroughput{MaxReadRequestUnits: aws.Int64(5)},
		KeySchema:            []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
	})
	require.NoError(t, err)
	require.Equal(t, settingsLimits(5, -1), describeSettings(t, client, "one-sided").OnDemandThroughput)

	// the side the request gives is replaced, the other one stays
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:          aws.String("one-sided"),
		OnDemandThroughput: &dynamodbtypes.OnDemandThroughput{MaxWriteRequestUnits: aws.Int64(7)},
	})
	require.NoError(t, err)
	require.Equal(t, settingsLimits(5, 7), describeSettings(t, client, "one-sided").OnDemandThroughput)
}
