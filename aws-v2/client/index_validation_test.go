package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func requireAPIError(t *testing.T, err error, code, message string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.ErrorCode())
	require.Equal(t, message, apiErr.ErrorMessage())
}

func TestUpdateTableRejectsMalformedGSIUpdates(t *testing.T) {
	keys := []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}}
	all := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll}

	tests := []struct {
		name    string
		update  []dynamodbtypes.GlobalSecondaryIndexUpdate
		code    string
		message string
	}{
		{
			"DeleteWithoutIndexName",
			[]dynamodbtypes.GlobalSecondaryIndexUpdate{{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{}}},
			"ValidationException", "Invalid global secondary index name",
		},
		{
			"CreateWithoutIndexName",
			[]dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{KeySchema: keys, Projection: all}}},
			"ValidationException", "Invalid global secondary index name",
		},
		{
			"CreateWithoutKeySchema",
			[]dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-pk"), Projection: all}}},
			"ValidationException", "No defined key schema.  A key schema containing at least a hash key must be defined for all tables | index: by-pk",
		},
		{
			"CreateWithoutProjection",
			[]dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-pk"), KeySchema: keys}}},
			"ValidationException", "Indexes must have a projection specified | index: by-pk",
		},
		{
			"UpdateWithoutIndexName",
			[]dynamodbtypes.GlobalSecondaryIndexUpdate{{Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{}}},
			"ValidationException", "Invalid global secondary index name",
		},
		{
			"NoAction",
			[]dynamodbtypes.GlobalSecondaryIndexUpdate{{}},
			"ValidationException", "Update global secondary index action is null",
		},
		{"TwoCreates", []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-a"), KeySchema: keys, Projection: all}},
			{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-b"), KeySchema: keys, Projection: all}},
		}, "LimitExceededException", "Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table | table: tbl; changes: 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient()
			createDescribedTable(t, client, "tbl", &dynamodb.CreateTableInput{BillingMode: dynamodbtypes.BillingModePayPerRequest})

			_, err := client.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
				TableName: aws.String("tbl"), GlobalSecondaryIndexUpdates: tt.update,
			})
			requireAPIError(t, err, tt.code, tt.message)

			desc, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String("tbl")})
			require.NoError(t, err)
			require.Empty(t, desc.Table.GlobalSecondaryIndexes)
		})
	}
}

func TestCreateTableRejectsMalformedIndexes(t *testing.T) {
	keys := []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}}
	all := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll}
	invalidName := "Invalid table/index name.  Table/index names must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'"

	tests := []struct {
		name    string
		gsis    []dynamodbtypes.GlobalSecondaryIndex
		lsis    []dynamodbtypes.LocalSecondaryIndex
		message string
	}{
		{"GSIWithoutName", []dynamodbtypes.GlobalSecondaryIndex{{KeySchema: keys, Projection: all}}, nil, invalidName},
		{
			"GSIWithoutKeySchema",
			[]dynamodbtypes.GlobalSecondaryIndex{{IndexName: aws.String("by-pk"), Projection: all}},
			nil,
			"No defined key schema.  A key schema containing at least a hash key must be defined for all tables | index: by-pk",
		},
		{"GSIWithoutProjection", []dynamodbtypes.GlobalSecondaryIndex{{IndexName: aws.String("by-pk"), KeySchema: keys}}, nil, "Indexes must have a projection specified | index: by-pk"},
		{"LSIWithoutName", nil, []dynamodbtypes.LocalSecondaryIndex{{KeySchema: keys, Projection: all}}, invalidName},
		{
			"LSIWithoutKeySchema", nil,
			[]dynamodbtypes.LocalSecondaryIndex{{IndexName: aws.String("by-pk"), Projection: all}},
			"No defined key schema.  A key schema containing at least a hash key must be defined for all tables | index: by-pk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient()

			_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
				TableName:              aws.String("tbl"),
				BillingMode:            dynamodbtypes.BillingModePayPerRequest,
				KeySchema:              keys,
				AttributeDefinitions:   []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
				GlobalSecondaryIndexes: tt.gsis,
				LocalSecondaryIndexes:  tt.lsis,
			})
			requireAPIError(t, err, "ValidationException", tt.message)

			_, err = client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String("tbl")})
			require.Error(t, err)
		})
	}
}
