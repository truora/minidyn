package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// TestE2E_AttributeDefinitionValidation pins what DynamoDB Local answers to an AttributeDefinitions
// entry without a name or a type, in CreateTable and in UpdateTable. A missing type is only pinned
// for CreateTable: Local answers 500 for it in UpdateTable.
func TestE2E_AttributeDefinitionValidation(t *testing.T) {
	type attrs = []dynamodbtypes.AttributeDefinition

	idHash := []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}}
	validID := dynamodbtypes.AttributeDefinition{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}

	tests := []struct {
		name       string
		attrs      attrs
		createOnly bool
	}{
		{name: "NoName", attrs: attrs{{AttributeType: dynamodbtypes.ScalarAttributeTypeS}}},
		{name: "Empty", attrs: attrs{{}}},
		{name: "EmptyName", attrs: attrs{{AttributeName: aws.String(""), AttributeType: dynamodbtypes.ScalarAttributeTypeS}}},
		{name: "NameBeforeType", attrs: attrs{validID, {}}},
		{name: "NoType", attrs: attrs{{AttributeName: aws.String("id")}}, createOnly: true},
	}

	for _, tt := range tests {
		t.Run("CreateTable"+tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
					TableName:            aws.String(parityPokemonTable),
					BillingMode:          dynamodbtypes.BillingModePayPerRequest,
					KeySchema:            idHash,
					AttributeDefinitions: tt.attrs,
				}, skipClientValidation)
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})

		if tt.createOnly {
			continue
		}

		t.Run("UpdateTable"+tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
					TableName:                   aws.String(parityPokemonTable),
					AttributeDefinitions:        tt.attrs,
					GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: gsiCreateAction("fresh")}},
				}, skipClientValidation)
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})
	}
}

// TestE2E_UpdateTableGSIValidationBeforeAttributeDefinitions pins that the problems with the
// GlobalSecondaryIndexUpdates are reported before the ones with the AttributeDefinitions.
func TestE2E_UpdateTableGSIValidationBeforeAttributeDefinitions(t *testing.T) {
	type updates = []dynamodbtypes.GlobalSecondaryIndexUpdate

	tests := []struct {
		name    string
		updates updates
	}{
		{name: "MalformedDelete", updates: updates{{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{}}}},
		{name: "TwoCreates", updates: updates{{Create: gsiCreateAction("fresh")}, {Create: gsiCreateAction("other")}}},
		{name: "NoAction", updates: updates{{}}},
		{name: "DeleteMissingIndex", updates: updates{{Delete: gsiDeleteAction("missing")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
					TableName:                   aws.String(parityPokemonTable),
					AttributeDefinitions:        []dynamodbtypes.AttributeDefinition{{}},
					GlobalSecondaryIndexUpdates: tt.updates,
				}, skipClientValidation)
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})
	}
}

// TestE2E_UpdateTableRejectedKeepsAttributeDefinitions shows that the attribute definitions of a
// rejected UpdateTable are not kept: creating an index on the attribute they defined, without
// defining it again, fails. The message differs, see #212.
func TestE2E_UpdateTableRejectedKeepsAttributeDefinitions(t *testing.T) {
	RunE2EKnown(t, "#212", func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()

		ctx := context.Background()
		parityCreatePokemonTable(ctx, t, client)

		_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:                   aws.String(parityPokemonTable),
			AttributeDefinitions:        gsiAttributes(),
			GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{}}},
		}, skipClientValidation)
		require.Error(t, err)

		first := normalizeSDKErrorString(err.Error())

		_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:                   aws.String(parityPokemonTable),
			GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: gsiCreateAction("fresh")}},
		}, skipClientValidation)
		require.Error(t, err)

		return []any{first, normalizeSDKErrorString(err.Error())}
	})
}
