package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func createDescribedTable(t *testing.T, client FakeClient, name string, in *dynamodb.CreateTableInput) *dynamodb.CreateTableOutput {
	t.Helper()

	in.TableName = aws.String(name)
	in.KeySchema = []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}}

	if in.AttributeDefinitions == nil {
		in.AttributeDefinitions = []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		}
	}

	out, err := client.CreateTable(context.Background(), in)
	require.NoError(t, err)

	return out
}

func TestTableDescriptionFields(t *testing.T) {
	c := require.New(t)
	client := NewClient()
	ctx := context.Background()

	created := createDescribedTable(t, client, "provisioned", &dynamodb.CreateTableInput{
		BillingMode:           dynamodbtypes.BillingModeProvisioned,
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(3), WriteCapacityUnits: aws.Int64(2)},
	})
	createDescribedTable(t, client, "ondemand", &dynamodb.CreateTableInput{BillingMode: dynamodbtypes.BillingModePayPerRequest})

	c.Equal(dynamodbtypes.TableStatusActive, created.TableDescription.TableStatus)

	prov, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("provisioned")})
	c.NoError(err)
	c.Equal(dynamodbtypes.TableStatusActive, prov.Table.TableStatus)
	c.Len(prov.Table.AttributeDefinitions, 1)
	c.Equal("pk", aws.ToString(prov.Table.AttributeDefinitions[0].AttributeName))
	c.Equal(dynamodbtypes.ScalarAttributeTypeS, prov.Table.AttributeDefinitions[0].AttributeType)
	c.Nil(prov.Table.BillingModeSummary)
	c.Equal(int64(3), aws.ToInt64(prov.Table.ProvisionedThroughput.ReadCapacityUnits))
	c.Equal(int64(0), aws.ToInt64(prov.Table.ProvisionedThroughput.NumberOfDecreasesToday))
	c.NotNil(prov.Table.ProvisionedThroughput.NumberOfDecreasesToday)
	c.Equal(int64(2), aws.ToInt64(prov.Table.ProvisionedThroughput.WriteCapacityUnits))
	c.NotNil(prov.Table.DeletionProtectionEnabled)
	c.False(*prov.Table.DeletionProtectionEnabled)

	od, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("ondemand")})
	c.NoError(err)
	c.Equal(dynamodbtypes.BillingModePayPerRequest, od.Table.BillingModeSummary.BillingMode)
	c.Equal(int64(0), aws.ToInt64(od.Table.ProvisionedThroughput.ReadCapacityUnits))
	c.Equal(int64(0), aws.ToInt64(od.Table.ProvisionedThroughput.WriteCapacityUnits))

	deleted, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("provisioned")})
	c.NoError(err)
	c.Equal(dynamodbtypes.TableStatusActive, deleted.TableDescription.TableStatus)
	c.Equal(int64(3), aws.ToInt64(deleted.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
}

func TestGlobalSecondaryIndexDescriptionThroughput(t *testing.T) {
	c := require.New(t)
	client := NewClient()

	createDescribedTable(t, client, "withindex", &dynamodb.CreateTableInput{
		BillingMode:           dynamodbtypes.BillingModeProvisioned,
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(3), WriteCapacityUnits: aws.Int64(2)},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:             aws.String("by-gk"),
			KeySchema:             []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("gk"), KeyType: dynamodbtypes.KeyTypeHash}},
			Projection:            &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(4)},
		}},
	})

	out, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String("withindex")})
	c.NoError(err)
	c.Len(out.Table.GlobalSecondaryIndexes, 1)
	c.Equal(int64(5), aws.ToInt64(out.Table.GlobalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits))
	c.Equal(int64(4), aws.ToInt64(out.Table.GlobalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits))
}

func TestUpdateTableDeleteIndexListsDeletingIndex(t *testing.T) {
	c := require.New(t)
	client := NewClient()

	c.NoError(ensurePokemonTable(client))
	createTestGlobalSecondaryIndex(t, client, "doomed-index")

	drop := &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("doomed-index")}},
		},
	}

	out, err := client.UpdateTable(context.Background(), drop)
	c.NoError(err)
	c.Len(out.TableDescription.GlobalSecondaryIndexes, 1)
	c.Equal("doomed-index", aws.ToString(out.TableDescription.GlobalSecondaryIndexes[0].IndexName))
	c.Equal(dynamodbtypes.IndexStatusDeleting, out.TableDescription.GlobalSecondaryIndexes[0].IndexStatus)

	_, err = client.UpdateTable(context.Background(), drop)
	c.Error(err)

	desc, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	c.NoError(err)
	c.Empty(desc.Table.GlobalSecondaryIndexes)
}
