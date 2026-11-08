package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func newDescribedTable(t *testing.T, billingMode string, throughput *types.ProvisionedThroughput) *Table {
	t.Helper()

	table := NewTable("described")
	require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{
		{AttributeName: new("pk"), AttributeType: new("S")},
		{AttributeName: new("gk"), AttributeType: new("N")},
	}))
	table.BillingMode = new(billingMode)

	require.NoError(t, table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema:             []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		ProvisionedThroughput: throughput,
	}))

	return table
}

func gsiUpdateCreate(name, attr string) *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{
		Create: &types.CreateGlobalSecondaryIndexAction{
			IndexName:  new(name),
			KeySchema:  []*types.KeySchemaElement{{AttributeName: attr, KeyType: "HASH"}},
			Projection: &types.Projection{ProjectionType: new("ALL")},
		},
	}
}

func TestDescriptionProvisionedTable(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PROVISIONED", &types.ProvisionedThroughput{ReadCapacityUnits: 3, WriteCapacityUnits: 2})

	d := table.Description("described")

	c.Equal("ACTIVE", d.TableStatus)
	// gk is declared but no key uses it, so it is not listed
	c.Equal([]types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}, d.AttributeDefinitions)
	c.Nil(d.BillingModeSummary)
	c.NotNil(d.ProvisionedThroughput)
	c.Equal(int64(3), d.ProvisionedThroughput.ReadCapacityUnits)
	c.Equal(int64(2), d.ProvisionedThroughput.WriteCapacityUnits)
	c.NotNil(d.DeletionProtectionEnabled)
	c.False(*d.DeletionProtectionEnabled)
}

func TestDescriptionAttributeDefinitionsFollowIndexKeys(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

	_, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiUpdateCreate("by-gk", "gk")})
	c.NoError(err)

	c.Equal([]types.AttributeDefinition{
		{AttributeName: new("gk"), AttributeType: new("N")},
		{AttributeName: new("pk"), AttributeType: new("S")},
	}, table.Description("described").AttributeDefinitions)

	d, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{
		{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: new("by-gk")}},
	})
	c.NoError(err)
	c.Equal([]types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}, d.AttributeDefinitions)
}

func TestDescriptionPayPerRequestTable(t *testing.T) {
	c := require.New(t)
	// DynamoDB reports 0/0 for on-demand tables even when capacity was sent
	table := newDescribedTable(t, "PAY_PER_REQUEST", &types.ProvisionedThroughput{ReadCapacityUnits: 10, WriteCapacityUnits: 10})

	d := table.Description("described")

	c.Equal("ACTIVE", d.TableStatus)
	c.NotNil(d.BillingModeSummary)
	c.Equal("PAY_PER_REQUEST", d.BillingModeSummary.BillingMode)
	c.NotNil(d.ProvisionedThroughput)
	c.Equal(int64(0), d.ProvisionedThroughput.ReadCapacityUnits)
	c.Equal(int64(0), d.ProvisionedThroughput.WriteCapacityUnits)
}

func TestDescriptionGlobalIndexThroughput(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PROVISIONED", &types.ProvisionedThroughput{ReadCapacityUnits: 3, WriteCapacityUnits: 2})

	c.NoError(table.AddGlobalIndexes([]*types.GlobalSecondaryIndex{{
		IndexName:             new("by-gk"),
		KeySchema:             []*types.KeySchemaElement{{AttributeName: "gk", KeyType: "HASH"}},
		Projection:            &types.Projection{ProjectionType: new("ALL")},
		ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 4},
	}}))

	gsi, _ := table.IndexesDescription()
	c.Len(gsi, 1)
	c.NotNil(gsi[0].ProvisionedThroughput)
	c.Equal(int64(5), gsi[0].ProvisionedThroughput.ReadCapacityUnits)
	c.Equal(int64(4), gsi[0].ProvisionedThroughput.WriteCapacityUnits)
	c.Nil(gsi[0].OnDemandThroughput)
}

func TestApplyIndexChangesCreate(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

	d, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiUpdateCreate("by-gk", "gk")})
	c.NoError(err)
	c.Len(d.GlobalSecondaryIndexes, 1)

	created := d.GlobalSecondaryIndexes[0]
	c.Equal("ACTIVE", *created.IndexStatus)
	c.NotNil(created.Backfilling)
	c.False(*created.Backfilling)
	c.NotNil(created.OnDemandThroughput)
	c.Equal(int64(-1), *created.OnDemandThroughput.MaxReadRequestUnits)
	c.Equal(int64(-1), *created.OnDemandThroughput.MaxWriteRequestUnits)

	// the creation markers are specific to the UpdateTable response
	gsi, _ := table.IndexesDescription()
	c.Len(gsi, 1)
	c.Nil(gsi[0].Backfilling)
	c.Nil(gsi[0].OnDemandThroughput)
}

func TestApplyIndexChangesCreateKeepsActivationDelay(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)
	table.IndexActivationDelay = time.Hour

	d, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiUpdateCreate("by-gk", "gk")})
	c.NoError(err)
	c.Equal("CREATING", *d.GlobalSecondaryIndexes[0].IndexStatus)
}

func TestApplyIndexChangesCreateProvisionedOmitsOnDemand(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PROVISIONED", &types.ProvisionedThroughput{ReadCapacityUnits: 3, WriteCapacityUnits: 2})

	update := gsiUpdateCreate("by-gk", "gk")
	update.Create.ProvisionedThroughput = &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1}

	d, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{update})
	c.NoError(err)
	c.Nil(d.GlobalSecondaryIndexes[0].OnDemandThroughput)
	c.Equal(int64(1), d.GlobalSecondaryIndexes[0].ProvisionedThroughput.ReadCapacityUnits)
}

func TestApplyIndexChangesDeleteListsIndexAsDeleting(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

	_, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiUpdateCreate("by-gk", "gk")})
	c.NoError(err)

	d, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{
		{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: new("by-gk")}},
	})
	c.NoError(err)
	c.Len(d.GlobalSecondaryIndexes, 1)
	c.Equal("by-gk", *d.GlobalSecondaryIndexes[0].IndexName)
	c.Equal("DELETING", *d.GlobalSecondaryIndexes[0].IndexStatus)
	c.Equal([]types.KeySchemaElement{{AttributeName: "gk", KeyType: "HASH"}}, d.GlobalSecondaryIndexes[0].KeySchema)

	// later descriptions no longer list it
	gsi, _ := table.IndexesDescription()
	c.Empty(gsi)
	c.Empty(table.Description("described").GlobalSecondaryIndexes)
}

func TestApplyIndexChangesErrorStillDescribesTable(t *testing.T) {
	c := require.New(t)
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

	d, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{
		{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: new("missing")}},
	})
	c.Error(err)
	c.NotNil(d)
	c.Equal("described", d.TableName)
}
