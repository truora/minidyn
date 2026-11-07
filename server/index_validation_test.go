package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"
)

// skipInputValidation lets a request with missing required fields reach the server, as a client
// that does not validate its input (or raw JSON) would send it.
func skipInputValidation(o *dynamodb.Options) {
	o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
		_, err := s.Initialize.Remove("OperationInputValidation")

		return err
	})
}

func requireServerAPIError(t *testing.T, err error, code, message string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.ErrorCode())
	require.Equal(t, message, apiErr.ErrorMessage())
}

func TestServerUpdateTableRejectsMalformedGSIUpdates(t *testing.T) {
	keys := []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}
	all := &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll}

	tests := []struct {
		name    string
		update  []ddbtypes.GlobalSecondaryIndexUpdate
		code    string
		message string
	}{
		{
			"DeleteWithoutIndexName",
			[]ddbtypes.GlobalSecondaryIndexUpdate{{Delete: &ddbtypes.DeleteGlobalSecondaryIndexAction{}}},
			"ValidationException", "Invalid global secondary index name",
		},
		{
			"CreateWithoutIndexName",
			[]ddbtypes.GlobalSecondaryIndexUpdate{{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{KeySchema: keys, Projection: all}}},
			"ValidationException", "Invalid global secondary index name",
		},
		{
			"CreateWithoutKeySchema",
			[]ddbtypes.GlobalSecondaryIndexUpdate{{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-id"), Projection: all}}},
			"ValidationException", "No defined key schema.  A key schema containing at least a hash key must be defined for all tables | index: by-id",
		},
		{
			"CreateWithoutProjection",
			[]ddbtypes.GlobalSecondaryIndexUpdate{{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-id"), KeySchema: keys}}},
			"ValidationException", "Indexes must have a projection specified | index: by-id",
		},
		{
			"UpdateWithoutIndexName",
			[]ddbtypes.GlobalSecondaryIndexUpdate{{Update: &ddbtypes.UpdateGlobalSecondaryIndexAction{}}},
			"ValidationException", "Invalid global secondary index name",
		},
		{
			"NoAction",
			[]ddbtypes.GlobalSecondaryIndexUpdate{{}},
			"ValidationException", "Update global secondary index action is null",
		},
		{"TwoCreates", []ddbtypes.GlobalSecondaryIndexUpdate{
			{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-a"), KeySchema: keys, Projection: all}},
			{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{IndexName: aws.String("by-b"), KeySchema: keys, Projection: all}},
		}, "LimitExceededException", "Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table | table: pokemons; changes: 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(NewServer())
			defer ts.Close()

			cli := newTestDynamoClient(t, ts.URL)
			makeBasicTable(t, cli, "pokemons", "id")

			_, err := cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
				TableName: aws.String("pokemons"), GlobalSecondaryIndexUpdates: tt.update,
			}, skipInputValidation)
			requireServerAPIError(t, err, tt.code, tt.message)

			// the connection is still up and nothing was applied
			desc, err := cli.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String("pokemons")})
			require.NoError(t, err)
			require.Empty(t, desc.Table.GlobalSecondaryIndexes)
		})
	}
}

func TestServerCreateTableRejectsMalformedIndexes(t *testing.T) {
	keys := []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}
	all := &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll}
	invalidName := "Invalid table/index name.  Table/index names must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'"

	tests := []struct {
		name    string
		gsis    []ddbtypes.GlobalSecondaryIndex
		lsis    []ddbtypes.LocalSecondaryIndex
		message string
	}{
		{"GSIWithoutName", []ddbtypes.GlobalSecondaryIndex{{KeySchema: keys, Projection: all}}, nil, invalidName},
		{
			"GSIWithoutKeySchema",
			[]ddbtypes.GlobalSecondaryIndex{{IndexName: aws.String("by-id"), Projection: all}},
			nil,
			"No defined key schema.  A key schema containing at least a hash key must be defined for all tables | index: by-id",
		},
		{"GSIWithoutProjection", []ddbtypes.GlobalSecondaryIndex{{IndexName: aws.String("by-id"), KeySchema: keys}}, nil, "Indexes must have a projection specified | index: by-id"},
		{"LSIWithoutName", nil, []ddbtypes.LocalSecondaryIndex{{KeySchema: keys, Projection: all}}, invalidName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(NewServer())
			defer ts.Close()

			cli := newTestDynamoClient(t, ts.URL)

			_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
				TableName:              aws.String("tbl"),
				BillingMode:            ddbtypes.BillingModePayPerRequest,
				KeySchema:              keys,
				AttributeDefinitions:   []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
				GlobalSecondaryIndexes: tt.gsis,
				LocalSecondaryIndexes:  tt.lsis,
			}, skipInputValidation)
			requireServerAPIError(t, err, "ValidationException", tt.message)
		})
	}
}
