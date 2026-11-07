package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/stretchr/testify/require"
)

// skipClientValidation lets a request with missing required fields reach the server, as one built
// by another client (or raw JSON) would.
func skipClientValidation(o *dynamodb.Options) {
	o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
		_, err := s.Initialize.Remove("OperationInputValidation")

		return err
	})
}

func gsiKeySchema() []dynamodbtypes.KeySchemaElement {
	return []dynamodbtypes.KeySchemaElement{
		{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeHash},
	}
}

func gsiAll() *dynamodbtypes.Projection {
	return &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll}
}

func gsiAttributes() []dynamodbtypes.AttributeDefinition {
	return []dynamodbtypes.AttributeDefinition{
		{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		{AttributeName: aws.String("type"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
	}
}

func gsiCreateAction(name string) *dynamodbtypes.CreateGlobalSecondaryIndexAction {
	return &dynamodbtypes.CreateGlobalSecondaryIndexAction{
		IndexName: aws.String(name), KeySchema: gsiKeySchema(), Projection: gsiAll(),
	}
}

func gsiDeleteAction(name string) *dynamodbtypes.DeleteGlobalSecondaryIndexAction {
	return &dynamodbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(name)}
}

// gsiUpdateAction changes the throughput of an index, which only makes sense on a provisioned
// table: DynamoDB Local answers 500 for it on an on-demand one.
func gsiUpdateAction(name *string) *dynamodbtypes.UpdateGlobalSecondaryIndexAction {
	return &dynamodbtypes.UpdateGlobalSecondaryIndexAction{
		IndexName: name,
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(6), WriteCapacityUnits: aws.Int64(6),
		},
	}
}

// updateTableOutcome creates a table with one GSI named "existing", sends the (unvalidated)
// updates and returns "OK" or the normalized error, followed by the GSIs the table ends up with.
// Creating an index on a provisioned table answers 500 on DynamoDB Local when no throughput is
// given, so create cases use an on-demand table and update cases a provisioned one.
func updateTableOutcome(t *testing.T, client *dynamodb.Client, provisioned bool, updates []dynamodbtypes.GlobalSecondaryIndexUpdate) []string {
	t.Helper()

	ctx := context.Background()

	create := &dynamodb.CreateTableInput{
		TableName:            aws.String(parityPokemonTable),
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		AttributeDefinitions: gsiAttributes(),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("existing"), KeySchema: gsiKeySchema(), Projection: gsiAll(),
		}},
	}

	if provisioned {
		throughput := &dynamodbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5)}

		create.BillingMode = dynamodbtypes.BillingModeProvisioned
		create.ProvisionedThroughput = throughput
		create.GlobalSecondaryIndexes[0].ProvisionedThroughput = throughput
	}

	_, err := client.CreateTable(ctx, create)
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, parityPokemonTable, "existing")

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:                   aws.String(parityPokemonTable),
		AttributeDefinitions:        gsiAttributes(),
		GlobalSecondaryIndexUpdates: updates,
	}, skipClientValidation)

	out := []string{"OK"}
	if err != nil {
		out = []string{normalizeSDKErrorString(err.Error())}
	}

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(parityPokemonTable)})
	require.NoError(t, err)

	for _, g := range desc.Table.GlobalSecondaryIndexes {
		out = append(out, aws.ToString(g.IndexName)+":"+string(g.IndexStatus))
	}

	return out
}

// TestE2E_UpdateTableGSIValidation pins the order of checks and the messages DynamoDB Local gives
// for malformed GlobalSecondaryIndexUpdates entries, and that a rejected request changes nothing.
func TestE2E_UpdateTableGSIValidation(t *testing.T) {
	type updates = []dynamodbtypes.GlobalSecondaryIndexUpdate

	tests := []struct {
		name        string
		provisioned bool
		updates     updates
	}{
		// Delete
		{name: "DeleteWithoutIndexName", updates: updates{{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{}}}},
		{name: "DeleteEmptyIndexName", updates: updates{{Delete: gsiDeleteAction("")}}},
		{name: "DeleteMissingIndex", updates: updates{{Delete: gsiDeleteAction("missing")}}},
		// Create
		{name: "CreateWithoutIndexName", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			KeySchema: gsiKeySchema(), Projection: gsiAll(),
		}}}},
		{name: "CreateEmptyIndexName", updates: updates{{Create: gsiCreateAction("")}}},
		{name: "CreateEmptyAction", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{}}}},
		{name: "CreateWithoutKeySchema", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String("fresh"), Projection: gsiAll(),
		}}}},
		{name: "CreateEmptyKeySchema", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String("fresh"), Projection: gsiAll(), KeySchema: []dynamodbtypes.KeySchemaElement{},
		}}}},
		{name: "CreateKeyWithoutAttributeName", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String("fresh"), Projection: gsiAll(),
			KeySchema: []dynamodbtypes.KeySchemaElement{{KeyType: dynamodbtypes.KeyTypeHash}},
		}}}},
		{name: "CreateWithoutProjection", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String("fresh"), KeySchema: gsiKeySchema(),
		}}}},
		{name: "CreateEmptyProjection", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String("fresh"), KeySchema: gsiKeySchema(), Projection: &dynamodbtypes.Projection{},
		}}}},
		// the projection is checked before the key schema
		{name: "CreateNameOnly", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String("fresh"),
		}}}},
		{name: "CreateExistingName", updates: updates{{Create: gsiCreateAction("existing")}}},
		// Update
		{name: "UpdateWithoutIndexName", provisioned: true, updates: updates{{Update: gsiUpdateAction(nil)}}},
		{name: "UpdateEmptyIndexName", provisioned: true, updates: updates{{Update: gsiUpdateAction(aws.String(""))}}},
		{name: "UpdateMissingIndex", provisioned: true, updates: updates{{Update: gsiUpdateAction(aws.String("missing"))}}},
		// no action
		{name: "NoAction", updates: updates{{}}},
		{name: "TwoNoActionEntries", updates: updates{{}, {}}},
		// one create or delete per request, counted before the entries are checked
		{name: "TwoCreates", updates: updates{{Create: gsiCreateAction("fresh")}, {Create: gsiCreateAction("other")}}},
		{name: "CreateAndDeleteInOneEntry", updates: updates{{Create: gsiCreateAction("fresh"), Delete: gsiDeleteAction("existing")}}},
		{name: "LimitBeatsMalformedDelete", updates: updates{{Create: gsiCreateAction("fresh")}, {Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{}}}},
		{name: "LimitBeatsMissingDelete", updates: updates{{Create: gsiCreateAction("fresh")}, {Delete: gsiDeleteAction("missing")}}},
		{name: "LimitBeatsNoActionFirst", updates: updates{{}, {Create: gsiCreateAction("fresh")}, {Create: gsiCreateAction("other")}}},
		{name: "LimitBeatsNoActionLast", updates: updates{{Create: gsiCreateAction("fresh")}, {Create: gsiCreateAction("other")}, {}}},
		// entries are checked in order
		{name: "EntriesCheckedInOrderCreateExisting", updates: updates{{Create: gsiCreateAction("existing")}, {}}},
		{name: "EntriesCheckedInOrderBadCreate", updates: updates{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			Projection: gsiAll(),
		}}, {}}},
		// one update per index
		{name: "TwoUpdatesSameIndex", provisioned: true, updates: updates{{Update: gsiUpdateAction(aws.String("existing"))}, {Update: gsiUpdateAction(aws.String("existing"))}}},
		{name: "UpdateAfterDeleteSameIndex", provisioned: true, updates: updates{{Delete: gsiDeleteAction("existing")}, {Update: gsiUpdateAction(aws.String("existing"))}}},
		{name: "UpdateAfterCreateSameIndex", updates: updates{{Create: gsiCreateAction("fresh")}, {Update: gsiUpdateAction(aws.String("fresh"))}}},
		// a rejected request applies none of its entries
		{name: "DeleteThenUpdateMissingChangesNothing", provisioned: true, updates: updates{{Delete: gsiDeleteAction("existing")}, {Update: gsiUpdateAction(aws.String("missing"))}}},
		{name: "UpdateMissingThenDeleteChangesNothing", provisioned: true, updates: updates{{Update: gsiUpdateAction(aws.String("missing"))}, {Delete: gsiDeleteAction("existing")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				return updateTableOutcome(t, client, tt.provisioned, tt.updates)
			})
		})
	}
}

// TestE2E_CreateTableIndexValidation pins the messages for malformed GlobalSecondaryIndexes and
// LocalSecondaryIndexes entries in CreateTable.
func TestE2E_CreateTableIndexValidation(t *testing.T) {
	localKeys := []dynamodbtypes.KeySchemaElement{
		{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
		{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
	}
	noAttribute := []dynamodbtypes.KeySchemaElement{{KeyType: dynamodbtypes.KeyTypeHash}}
	name := aws.String("fresh")

	gsi := func(name *string, keys []dynamodbtypes.KeySchemaElement, projection *dynamodbtypes.Projection) []dynamodbtypes.GlobalSecondaryIndex {
		return []dynamodbtypes.GlobalSecondaryIndex{{IndexName: name, KeySchema: keys, Projection: projection}}
	}
	lsi := func(name *string, keys []dynamodbtypes.KeySchemaElement, projection *dynamodbtypes.Projection) []dynamodbtypes.LocalSecondaryIndex {
		return []dynamodbtypes.LocalSecondaryIndex{{IndexName: name, KeySchema: keys, Projection: projection}}
	}

	tests := []struct {
		name string
		gsis []dynamodbtypes.GlobalSecondaryIndex
		lsis []dynamodbtypes.LocalSecondaryIndex
	}{
		{name: "GSIWithoutName", gsis: gsi(nil, gsiKeySchema(), gsiAll())},
		{name: "GSIEmptyName", gsis: gsi(aws.String(""), gsiKeySchema(), gsiAll())},
		{name: "GSIEmpty", gsis: gsi(nil, nil, nil)},
		{name: "GSIWithoutKeySchema", gsis: gsi(name, nil, gsiAll())},
		{name: "GSIKeyWithoutAttributeName", gsis: gsi(name, noAttribute, gsiAll())},
		{name: "GSIWithoutProjection", gsis: gsi(name, gsiKeySchema(), nil)},
		{name: "GSIEmptyProjection", gsis: gsi(name, gsiKeySchema(), &dynamodbtypes.Projection{})},
		{name: "GSINameOnly", gsis: gsi(name, nil, nil)},
		{name: "LSIWithoutName", lsis: lsi(nil, localKeys, gsiAll())},
		{name: "LSIEmptyName", lsis: lsi(aws.String(""), localKeys, gsiAll())},
		{name: "LSIEmpty", lsis: lsi(nil, nil, nil)},
		{name: "LSIWithoutKeySchema", lsis: lsi(name, nil, gsiAll())},
		{name: "LSIKeyWithoutAttributeName", lsis: lsi(name, noAttribute, gsiAll())},
		{name: "LSIWithoutProjection", lsis: lsi(name, localKeys, nil)},
		{name: "LSINameOnly", lsis: lsi(name, nil, nil)},
		{name: "GSIWithoutNameBeforeLSIWithoutName", gsis: gsi(nil, gsiKeySchema(), gsiAll()), lsis: lsi(nil, localKeys, gsiAll())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				attrs := append(gsiAttributes(), dynamodbtypes.AttributeDefinition{
					AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS,
				})

				_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
					TableName:            aws.String(parityPokemonTable),
					BillingMode:          dynamodbtypes.BillingModePayPerRequest,
					AttributeDefinitions: attrs,
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					GlobalSecondaryIndexes: tt.gsis,
					LocalSecondaryIndexes:  tt.lsis,
				}, skipClientValidation)
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})
	}
}
