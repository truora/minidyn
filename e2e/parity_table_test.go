package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestE2E_Table(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "CreateTable",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				var msgs []string

				input := &dynamodb.CreateTableInput{
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("partition"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("partition"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("range"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					TableName: aws.String(parityPokemonTable),
				}

				_, err := client.CreateTable(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				input.AttributeDefinitions = append(input.AttributeDefinitions, dynamodbtypes.AttributeDefinition{
					AttributeName: aws.String("range"),
					AttributeType: dynamodbtypes.ScalarAttributeTypeS,
				})

				_, err = client.CreateTable(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				input.BillingMode = dynamodbtypes.BillingModePayPerRequest

				_, err = client.CreateTable(ctx, input)
				require.NoError(t, err)

				_, err = client.CreateTable(ctx, input)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				return msgs
			},
		},
		{
			name: "CreateTableWithGSI",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				input := &dynamodb.CreateTableInput{
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("partition"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
						{AttributeName: aws.String("range"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(1),
						WriteCapacityUnits: aws.Int64(1),
					},
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("partition"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("range"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
						{
							IndexName: aws.String("invert"),
							KeySchema: []dynamodbtypes.KeySchemaElement{
								{AttributeName: aws.String("range"), KeyType: dynamodbtypes.KeyTypeHash},
								{AttributeName: aws.String("partition"), KeyType: dynamodbtypes.KeyTypeRange},
							},
							Projection: &dynamodbtypes.Projection{
								ProjectionType: dynamodbtypes.ProjectionTypeAll,
							},
							ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
								ReadCapacityUnits:  aws.Int64(1),
								WriteCapacityUnits: aws.Int64(1),
							},
						},
					},
					TableName: aws.String(parityPokemonTable + "-gsi"),
				}

				out, err := client.CreateTable(ctx, input)
				require.NoError(t, err)

				return aws.ToString(out.TableDescription.TableName)
			},
		},
		{
			name: "CreateTableWithLSI",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				input := &dynamodb.CreateTableInput{
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("partition"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
						{AttributeName: aws.String("range"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
						{AttributeName: aws.String("data"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(1),
						WriteCapacityUnits: aws.Int64(1),
					},
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("partition"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("range"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{
						{
							IndexName: aws.String("data"),
							KeySchema: []dynamodbtypes.KeySchemaElement{
								{AttributeName: aws.String("partition"), KeyType: dynamodbtypes.KeyTypeHash},
								{AttributeName: aws.String("data"), KeyType: dynamodbtypes.KeyTypeRange},
							},
							Projection: &dynamodbtypes.Projection{
								ProjectionType: dynamodbtypes.ProjectionTypeAll,
							},
						},
					},
					TableName: aws.String(parityPokemonTable + "-lsi"),
				}

				out, err := client.CreateTable(ctx, input)
				require.NoError(t, err)

				return aws.ToString(out.TableDescription.TableName)
			},
		},
		{
			name: "DeleteTable",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				var msgs []string

				_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("table-404")})
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				parityCreatePokemonTable(ctx, t, client)

				_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(parityPokemonTable)})
				require.NoError(t, err)

				_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(parityPokemonTable)})
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				return msgs
			},
		},
		{
			name: "UpdateTable",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				var msgs []string

				_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
					BillingMode:                 dynamodbtypes.BillingModeProvisioned,
					GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{},
					TableName:                   aws.String("404"),
				})
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				parityCreatePokemonTable(ctx, t, client)

				input := &dynamodb.UpdateTableInput{
					AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
						{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
						{AttributeName: aws.String("type"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					},
					GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
						{
							Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
								IndexName: aws.String("newIndex"),
								KeySchema: []dynamodbtypes.KeySchemaElement{
									{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeHash},
									{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeRange},
								},
								Projection: &dynamodbtypes.Projection{
									ProjectionType: dynamodbtypes.ProjectionTypeAll,
								},
							},
						},
					},
					TableName: aws.String(parityPokemonTable),
				}
				_, err = client.UpdateTable(ctx, input)
				require.NoError(t, err)

				time.Sleep(5 * time.Second)

				delInput := &dynamodb.UpdateTableInput{
					GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
						{
							Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{
								IndexName: aws.String("newIndex"),
							},
						},
					},
					TableName: aws.String(parityPokemonTable),
				}
				_, err = client.UpdateTable(ctx, delInput)
				require.NoError(t, err)

				parityWaitUntilGlobalSecondaryIndexRemoved(ctx, t, client, parityPokemonTable, "newIndex")

				_, err = client.UpdateTable(ctx, delInput)
				require.Error(t, err)
				msgs = append(msgs, normalizeSDKErrorString(err.Error()))

				return msgs
			},
		},
		{
			name: "DescribeTable",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(parityPokemonTable),
				})
				require.NoError(t, err)
				require.NotNil(t, out)

				return []any{
					len(out.Table.KeySchema),
					aws.ToString(out.Table.TableName),
					out.Table.KeySchema[0].KeyType,
					aws.ToString(out.Table.KeySchema[0].AttributeName),
				}
			},
		},
		{
			name: "DescribeTableNotFound",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreatePokemonTable(ctx, t, client)

				_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String("non_existing"),
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
