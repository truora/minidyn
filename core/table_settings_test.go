package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// The messages and the order of the checks are the ones DynamoDB Local gives (#199).
const (
	testMsgNothingToUpdate = "Nothing to update"
	testMsgSameThroughput  = "The requested throughput value equals the current value"
	testMsgTableThroughput = "Table read and write throughput must be at least 1, and cannot be null"
	testMsgGSIThroughput   = "Global Secondary Index's read and write throughput must be at least 1, and cannot be null"
	testMsgNoTableThruput  = "No provisioned throughput specified for the table"
	testMsgNoGSIThroughput = "No provisioned throughput specified for the global secondary index"
	testMsgProtected       = "Resource cannot be deleted as it is currently protected against deletion. Disable deletion protection first."
)

func updateIndexes(table *Table, name string, attrs []*types.AttributeDefinition, changes []*types.GlobalSecondaryIndexUpdate) (*types.TableDescription, error) {
	return table.UpdateTable(name, &types.UpdateTableInput{AttributeDefinitions: attrs, GlobalSecondaryIndexUpdates: changes})
}

func throughput(read, write int64) *types.ProvisionedThroughput {
	return &types.ProvisionedThroughput{ReadCapacityUnits: read, WriteCapacityUnits: write}
}

func onDemand(read, write int64) *types.OnDemandThroughput {
	return &types.OnDemandThroughput{MaxReadRequestUnits: new(read), MaxWriteRequestUnits: new(write)}
}

func provisionedTable(t *testing.T) *Table {
	t.Helper()

	return newDescribedTable(t, "PROVISIONED", throughput(5, 5))
}

func provisionedTableWithIndex(t *testing.T) *Table {
	t.Helper()

	table := provisionedTable(t)
	require.NoError(t, table.AddGlobalIndexes([]*types.GlobalSecondaryIndex{{
		IndexName:             new("by-gk"),
		KeySchema:             gsiHashKey(),
		Projection:            gsiAllProjection(),
		ProvisionedThroughput: throughput(5, 5),
	}}))

	return table
}

func gsiThroughputUpdate(name string, pt *types.ProvisionedThroughput) *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{Update: &types.UpdateGlobalSecondaryIndexAction{IndexName: new(name), ProvisionedThroughput: pt}}
}

func requireDescribed(t *testing.T, table *Table, mode string, read, write int64) {
	t.Helper()

	d := table.Description("described")
	if mode == "" {
		require.Nil(t, d.BillingModeSummary)
	} else {
		require.NotNil(t, d.BillingModeSummary)
		require.Equal(t, mode, d.BillingModeSummary.BillingMode)
	}

	require.Equal(t, &types.ProvisionedThroughput{ReadCapacityUnits: read, WriteCapacityUnits: write}, d.ProvisionedThroughput)
}

func TestUpdateTableNothingToUpdate(t *testing.T) {
	table := provisionedTable(t)
	attrs := []*types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}

	for name, in := range map[string]*types.UpdateTableInput{
		"Empty":              {},
		"EmptyIndexUpdates":  {GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{}},
		"AttributesOnly":     {AttributeDefinitions: attrs},
		"AttributesNoChange": {AttributeDefinitions: attrs, GlobalSecondaryIndexUpdates: nil},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := table.UpdateTable("described", in)
			requireValidation(t, err, "ValidationException", testMsgNothingToUpdate)
		})
	}

	// settings the front end saw but minidyn ignores still count as a change
	_, err := table.UpdateTable("described", &types.UpdateTableInput{UnsupportedChanges: true})
	require.NoError(t, err)
}

func TestUpdateTableSwitchesBillingMode(t *testing.T) {
	table := provisionedTable(t)
	requireDescribed(t, table, "", 5, 5)

	d, err := table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST")})
	require.NoError(t, err)
	require.Equal(t, "PAY_PER_REQUEST", d.BillingModeSummary.BillingMode)
	require.Equal(t, &types.ProvisionedThroughput{}, d.ProvisionedThroughput)
	requireDescribed(t, table, "PAY_PER_REQUEST", 0, 0)

	// repeating the mode is accepted
	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST")})
	require.NoError(t, err)

	// the capacity of the provisioned period is gone, so it has to be sent again
	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED")})
	requireValidation(t, err, "ValidationException", testMsgNoTableThruput)
	requireDescribed(t, table, "PAY_PER_REQUEST", 0, 0)

	d, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED"), ProvisionedThroughput: throughput(7, 8)})
	require.NoError(t, err)
	require.Equal(t, &types.ProvisionedThroughput{ReadCapacityUnits: 7, WriteCapacityUnits: 8}, d.ProvisionedThroughput)

	// the summary stays once the table has been on demand
	requireDescribed(t, table, "PROVISIONED", 7, 8)
}

func TestUpdateTableThroughputWithPayPerRequestIsIgnored(t *testing.T) {
	table := provisionedTable(t)

	_, err := table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST"), ProvisionedThroughput: throughput(3, 3)})
	require.NoError(t, err)
	requireDescribed(t, table, "PAY_PER_REQUEST", 0, 0)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{ProvisionedThroughput: throughput(9, 9)})
	require.NoError(t, err)
	requireDescribed(t, table, "PAY_PER_REQUEST", 0, 0)

	// what was sent while on demand is not remembered
	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED")})
	requireValidation(t, err, "ValidationException", testMsgNoTableThruput)
}

func TestUpdateTableRejectsUnknownBillingMode(t *testing.T) {
	table := provisionedTable(t)

	_, err := table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("FOO")})
	requireValidation(t, err, "ValidationException",
		"1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]")
	requireDescribed(t, table, "", 5, 5)
}

func TestCreateTableRejectsUnknownBillingMode(t *testing.T) {
	table := NewTable("described")
	require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}))
	table.BillingMode = new("FOO")

	err := table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema:             []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		ProvisionedThroughput: throughput(5, 5),
	})
	requireValidation(t, err, "ValidationException",
		"1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]")
}

func TestUpdateTableChangesThroughput(t *testing.T) {
	table := provisionedTable(t)

	d, err := table.UpdateTable("described", &types.UpdateTableInput{ProvisionedThroughput: throughput(6, 5)})
	require.NoError(t, err)
	require.Equal(t, int64(6), d.ProvisionedThroughput.ReadCapacityUnits)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{ProvisionedThroughput: throughput(6, 4)})
	require.NoError(t, err)
	requireDescribed(t, table, "", 6, 4)

	// a mode that does not change is no excuse for the same capacity
	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED"), ProvisionedThroughput: throughput(6, 4)})
	requireValidation(t, err, "ValidationException", testMsgSameThroughput)

	// and so is going on demand with the capacity the table has now
	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST"), ProvisionedThroughput: throughput(6, 4)})
	requireValidation(t, err, "ValidationException", testMsgSameThroughput)
	requireDescribed(t, table, "", 6, 4)
}

func TestUpdateTableRejectsThroughputBelowOne(t *testing.T) {
	for name, pt := range map[string]*types.ProvisionedThroughput{
		"Zero": throughput(0, 0), "ZeroRead": throughput(0, 5), "ZeroWrite": throughput(5, 0),
		"NegativeRead": throughput(-1, 5), "NegativeWrite": throughput(5, -1), "Negative": throughput(-1, -1),
	} {
		t.Run(name, func(t *testing.T) {
			table := provisionedTable(t)

			_, err := table.UpdateTable("described", &types.UpdateTableInput{ProvisionedThroughput: pt})
			requireValidation(t, err, "ValidationException", testMsgTableThroughput)
			requireDescribed(t, table, "", 5, 5)

			// on demand tables are checked too
			_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST"), ProvisionedThroughput: pt})
			requireValidation(t, err, "ValidationException", testMsgTableThroughput)
			requireDescribed(t, table, "", 5, 5)
		})
	}
}

func TestUpdateTableDeletionProtection(t *testing.T) {
	table := provisionedTable(t)
	require.False(t, *table.Description("described").DeletionProtectionEnabled)
	require.NoError(t, table.CheckDeletable())

	d, err := table.UpdateTable("described", &types.UpdateTableInput{DeletionProtectionEnabled: new(true)})
	require.NoError(t, err)
	require.True(t, *d.DeletionProtectionEnabled)
	requireValidation(t, table.CheckDeletable(), "ValidationException", testMsgProtected)

	// the other settings leave the protection as it is
	_, err = table.UpdateTable("described", &types.UpdateTableInput{ProvisionedThroughput: throughput(6, 6)})
	require.NoError(t, err)
	require.True(t, *table.Description("described").DeletionProtectionEnabled)

	// and asking for it again is fine
	_, err = table.UpdateTable("described", &types.UpdateTableInput{DeletionProtectionEnabled: new(true)})
	require.NoError(t, err)

	d, err = table.UpdateTable("described", &types.UpdateTableInput{DeletionProtectionEnabled: new(false)})
	require.NoError(t, err)
	require.False(t, *d.DeletionProtectionEnabled)
	require.NoError(t, table.CheckDeletable())
}

func TestCreatePrimaryIndexStoresDeletionProtection(t *testing.T) {
	table := NewTable("described")
	require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}))
	table.BillingMode = new("PAY_PER_REQUEST")

	require.NoError(t, table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema:                 []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		DeletionProtectionEnabled: new(true),
	}))
	require.True(t, *table.Description("described").DeletionProtectionEnabled)
	requireValidation(t, table.CheckDeletable(), "ValidationException", testMsgProtected)
}

func TestCreatePrimaryIndexRejectsThroughputBelowOne(t *testing.T) {
	for _, pt := range []*types.ProvisionedThroughput{throughput(0, 0), throughput(-1, 1), throughput(1, 0)} {
		table := NewTable("described")
		require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}))
		table.BillingMode = new("PROVISIONED")

		err := table.CreatePrimaryIndex(&types.CreateTableInput{
			KeySchema:             []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			ProvisionedThroughput: pt,
		})
		requireValidation(t, err, "ValidationException", testMsgTableThroughput)
	}
}

func newOnDemandTable(t *testing.T, odt *types.OnDemandThroughput) (*Table, error) {
	t.Helper()

	table := NewTable("described")
	require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}))
	table.BillingMode = new("PAY_PER_REQUEST")

	err := table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema:          []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		OnDemandThroughput: odt,
	})

	return table, err
}

func TestOnDemandThroughputOnCreate(t *testing.T) {
	table, err := newOnDemandTable(t, onDemand(100, 50))
	require.NoError(t, err)
	require.Equal(t, onDemand(100, 50), table.Description("described").OnDemandThroughput)

	// -1 is "no limit", which is reported as nothing when both sides have it
	for _, odt := range []*types.OnDemandThroughput{nil, onDemand(-1, -1)} {
		table, err = newOnDemandTable(t, odt)
		require.NoError(t, err)
		require.Nil(t, table.Description("described").OnDemandThroughput)
	}

	// a limit with one side is accepted (the API reference: "MaxReadRequestUnits,
	// MaxWriteRequestUnits, or both"); the side that is absent or -1 has no limit
	for name, tc := range map[string]struct{ given, want *types.OnDemandThroughput }{
		"OnlyRead":     {&types.OnDemandThroughput{MaxReadRequestUnits: new(int64(5))}, onDemand(5, -1)},
		"OnlyWrite":    {&types.OnDemandThroughput{MaxWriteRequestUnits: new(int64(5))}, onDemand(-1, 5)},
		"ReadNoLimit":  {onDemand(-1, 5), onDemand(-1, 5)},
		"WriteNoLimit": {onDemand(5, -1), onDemand(5, -1)},
	} {
		table, err = newOnDemandTable(t, tc.given)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, table.Description("described").OnDemandThroughput, name)
	}

	for name, tc := range map[string]struct {
		odt *types.OnDemandThroughput
		msg string
	}{
		"ZeroRead":      {onDemand(0, 5), "One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range"},
		"Zero":          {onDemand(0, 0), "One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range"},
		"BelowMinusOne": {onDemand(-5, 5), "One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range"},
		"BadWrite":      {onDemand(5, -5), "One or more parameter values were invalid: Requested MaxWriteRequestUnits for OnDemandThroughput for table is outside of valid range"},
		"Empty":         {&types.OnDemandThroughput{}, types.WithDetail(testMsgTableThroughput, "reason", "OnDemandThroughput has neither MaxReadRequestUnits nor MaxWriteRequestUnits")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newOnDemandTable(t, tc.odt)
			requireValidation(t, err, "ValidationException", tc.msg)
		})
	}
}

func TestOnDemandThroughputIsIgnoredOnProvisionedTables(t *testing.T) {
	table := NewTable("described")
	require.NoError(t, table.SetAttributeDefinition([]*types.AttributeDefinition{{AttributeName: new("pk"), AttributeType: new("S")}}))
	table.BillingMode = new("PROVISIONED")

	// not even its values are checked
	require.NoError(t, table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema:             []*types.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		ProvisionedThroughput: throughput(5, 5),
		OnDemandThroughput:    onDemand(-3, 1),
	}))
	require.Nil(t, table.Description("described").OnDemandThroughput)

	_, err := table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: onDemand(10, 10)})
	require.NoError(t, err)
	require.Nil(t, table.Description("described").OnDemandThroughput)
}

func TestUpdateTableOnDemandThroughput(t *testing.T) {
	table := provisionedTable(t)

	// it applies with the mode, and goes with it
	d, err := table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST"), OnDemandThroughput: onDemand(10, 20)})
	require.NoError(t, err)
	require.Equal(t, onDemand(10, 20), d.OnDemandThroughput)

	// a request that does not mention it leaves it alone (DynamoDB Local resets it, see e2e/README.md)
	_, err = table.UpdateTable("described", &types.UpdateTableInput{DeletionProtectionEnabled: new(true)})
	require.NoError(t, err)
	require.Equal(t, onDemand(10, 20), table.Description("described").OnDemandThroughput)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: onDemand(200, 60)})
	require.NoError(t, err)
	require.Equal(t, onDemand(200, 60), table.Description("described").OnDemandThroughput)

	// -1 removes a limit, and only the side it is given for
	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: onDemand(7, -1)})
	require.NoError(t, err)
	require.Equal(t, onDemand(7, -1), table.Description("described").OnDemandThroughput)

	// a limit with one side replaces that side and keeps the other
	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: &types.OnDemandThroughput{MaxWriteRequestUnits: new(int64(60))}})
	require.NoError(t, err)
	require.Equal(t, onDemand(7, 60), table.Description("described").OnDemandThroughput)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: &types.OnDemandThroughput{MaxReadRequestUnits: new(int64(9))}})
	require.NoError(t, err)
	require.Equal(t, onDemand(9, 60), table.Description("described").OnDemandThroughput)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: &types.OnDemandThroughput{MaxWriteRequestUnits: new(int64(-1))}})
	require.NoError(t, err)
	require.Equal(t, onDemand(9, -1), table.Description("described").OnDemandThroughput)

	// no limit on either side is none
	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: onDemand(-1, -1)})
	require.NoError(t, err)
	require.Nil(t, table.Description("described").OnDemandThroughput)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: onDemand(1, 1)})
	require.NoError(t, err)

	// the API reference allows a limit of 1 or more, or -1: 0 and lower are rejected, as are no sides
	const outside = "One or more parameter values were invalid: Requested %s for OnDemandThroughput for table is outside of valid range"

	for name, tc := range map[string]struct {
		odt *types.OnDemandThroughput
		msg string
	}{
		"Zero":      {onDemand(0, 0), fmt.Sprintf(outside, "MaxReadRequestUnits")},
		"ZeroWrite": {onDemand(5, 0), fmt.Sprintf(outside, "MaxWriteRequestUnits")},
		"Negative":  {onDemand(-5, -1), fmt.Sprintf(outside, "MaxReadRequestUnits")},
		"OnlyZero":  {&types.OnDemandThroughput{MaxWriteRequestUnits: new(int64(0))}, fmt.Sprintf(outside, "MaxWriteRequestUnits")},
		"Empty":     {&types.OnDemandThroughput{}, types.WithDetail(testMsgTableThroughput, "reason", "OnDemandThroughput has neither MaxReadRequestUnits nor MaxWriteRequestUnits")},
	} {
		_, err = table.UpdateTable("described", &types.UpdateTableInput{OnDemandThroughput: tc.odt})
		requireValidation(t, err, "ValidationException", tc.msg)
		require.Equal(t, onDemand(1, 1), table.Description("described").OnDemandThroughput, name)
	}

	// going back to provisioned drops the limits
	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED"), ProvisionedThroughput: throughput(2, 2)})
	require.NoError(t, err)
	require.Nil(t, table.Description("described").OnDemandThroughput)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST")})
	require.NoError(t, err)
	require.Nil(t, table.Description("described").OnDemandThroughput)
}

func TestUpdateTableIndexThroughput(t *testing.T) {
	table := provisionedTableWithIndex(t)

	d, err := updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(8, 9))})
	require.NoError(t, err)
	require.Equal(t, throughput(8, 9), d.GlobalSecondaryIndexes[0].ProvisionedThroughput)

	gsi, _ := table.IndexesDescription()
	require.Equal(t, throughput(8, 9), gsi[0].ProvisionedThroughput)

	// a decrease of one of the two is a change
	_, err = updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(4, 9))})
	require.NoError(t, err)

	_, err = updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(4, 9))})
	requireValidation(t, err, "ValidationException", types.WithDetail(testMsgSameThroughput, "index", "by-gk", "table", "described"))

	for _, pt := range []*types.ProvisionedThroughput{throughput(0, 0), throughput(-1, 1), throughput(1, 0)} {
		_, err = updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", pt)})
		requireValidation(t, err, "ValidationException", types.WithDetail(testMsgGSIThroughput, "index", "by-gk", "table", "described"))
	}

	_, err = updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", nil)})
	requireValidation(t, err, "ValidationException", types.WithDetail(testMsgNoGSIThroughput,
		"reason", "the table is provisioned and the index update has no capacity", "index", "by-gk", "table", "described"))

	gsi, _ = table.IndexesDescription()
	require.Equal(t, throughput(4, 9), gsi[0].ProvisionedThroughput)
}

func onDemandIndexMessage(msg string) string {
	return types.WithDetail(msg, "reason", "the table is on demand after this request, so an index has no capacity to change",
		"index", "by-gk", "table", "described")
}

func TestUpdateTableIndexThroughputOnPayPerRequest(t *testing.T) {
	table := provisionedTableWithIndex(t)

	// Local answers 500 for this; the text is what DynamoDB says for an index created this way
	const msg = "One or more parameter values were invalid: ProvisionedThroughput should not be specified for index: by-gk when BillingMode is PAY_PER_REQUEST"

	_, err := table.UpdateTable("described", &types.UpdateTableInput{
		BillingMode:                 new("PAY_PER_REQUEST"),
		GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(9, 9))},
	})
	requireValidation(t, err, "ValidationException", onDemandIndexMessage(msg))
	requireDescribed(t, table, "", 5, 5)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PAY_PER_REQUEST")})
	require.NoError(t, err)

	_, err = updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(9, 9))})
	requireValidation(t, err, "ValidationException", onDemandIndexMessage(msg))

	// without capacity the entry has nothing to change
	_, err = updateIndexes(table, "described", nil, []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", nil)})
	require.NoError(t, err)

	// the index keeps its capacity while the table is on demand, and gets the table back to provisioned
	gsi, _ := table.IndexesDescription()
	require.Equal(t, throughput(5, 5), gsi[0].ProvisionedThroughput)

	_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED"), ProvisionedThroughput: throughput(5, 5)})
	require.NoError(t, err)
}

// The index named does not depend on the order a map gives.
func TestUpdateTableBackToProvisionedNamesTheFirstIndex(t *testing.T) {
	for range 20 {
		table := newTableWithExistingIndex(t)
		_, err := table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("another"), gsiHashKey(), gsiAllProjection())})
		require.NoError(t, err)

		_, err = table.ApplyIndexChanges("described", []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("zeta"), gsiHashKey(), gsiAllProjection())})
		require.NoError(t, err)

		_, err = table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED"), ProvisionedThroughput: throughput(3, 3)})
		requireValidation(t, err, "ValidationException", types.WithDetail(testMsgNoGSIThroughput,
			"reason", "the table goes back to provisioned and the index has no capacity", "index", "another", "table", "described"))
	}
}

func TestUpdateTableBackToProvisionedNeedsIndexThroughput(t *testing.T) {
	table := newTableWithExistingIndex(t)

	_, err := table.UpdateTable("described", &types.UpdateTableInput{BillingMode: new("PROVISIONED"), ProvisionedThroughput: throughput(3, 3)})
	requireValidation(t, err, "ValidationException", types.WithDetail(testMsgNoGSIThroughput,
		"reason", "the table goes back to provisioned and the index has no capacity", "index", "existing", "table", "described"))
	requireDescribed(t, table, "PAY_PER_REQUEST", 0, 0)

	// the same request can give the index its capacity
	d, err := table.UpdateTable("described", &types.UpdateTableInput{
		BillingMode:                 new("PROVISIONED"),
		ProvisionedThroughput:       throughput(3, 3),
		GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("existing", throughput(2, 2))},
	})
	require.NoError(t, err)
	require.Equal(t, throughput(2, 2), d.GlobalSecondaryIndexes[0].ProvisionedThroughput)
	requireDescribed(t, table, "PROVISIONED", 3, 3)
}

// DynamoDB Local reports a problem with an index change ahead of one with the table settings, the
// settings in the order below, and a rejected request changes nothing.
func TestUpdateTableValidationOrder(t *testing.T) {
	badIndexEntry := []*types.GlobalSecondaryIndexUpdate{{}}
	missingIndex := []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("missing", throughput(1, 1))}

	tests := []struct {
		name string
		in   *types.UpdateTableInput
		msg  string
	}{
		{"IndexEntryBeforeThroughput", &types.UpdateTableInput{ProvisionedThroughput: throughput(-1, 1), GlobalSecondaryIndexUpdates: badIndexEntry}, msgNullGSIAction},
		{"MissingIndexBeforeThroughput", &types.UpdateTableInput{ProvisionedThroughput: throughput(0, 0), GlobalSecondaryIndexUpdates: missingIndex}, types.WithDetail(msgInvalidGSIName, "index", "missing", "table", "described")},
		{"IndexThroughputBeforeTableThroughput", &types.UpdateTableInput{
			ProvisionedThroughput:       throughput(5, 5),
			GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(0, 0))},
		}, types.WithDetail(testMsgGSIThroughput, "index", "by-gk", "table", "described")},
		{"IndexSameBeforeTableMinimum", &types.UpdateTableInput{
			ProvisionedThroughput:       throughput(0, 0),
			GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{gsiThroughputUpdate("by-gk", throughput(5, 5))},
		}, types.WithDetail(testMsgSameThroughput, "index", "by-gk", "table", "described")},
		{"IndexEntryBeforeDeletionProtection", &types.UpdateTableInput{DeletionProtectionEnabled: new(true), GlobalSecondaryIndexUpdates: badIndexEntry}, msgNullGSIAction},
		{"ThroughputBeforeAttributes", &types.UpdateTableInput{
			ProvisionedThroughput: throughput(-1, 1),
			AttributeDefinitions:  []*types.AttributeDefinition{{}},
		}, testMsgTableThroughput},
		{"ThroughputBeforeOnDemand", &types.UpdateTableInput{ProvisionedThroughput: throughput(-1, 1), OnDemandThroughput: onDemand(-5, 1)}, testMsgTableThroughput},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := provisionedTableWithIndex(t)

			_, err := table.UpdateTable("described", tt.in)
			requireValidation(t, err, "ValidationException", tt.msg)

			requireDescribed(t, table, "", 5, 5)
			require.False(t, *table.Description("described").DeletionProtectionEnabled)
		})
	}
}

func TestUpdateTableIsAtomicAcrossSettingsAndIndexes(t *testing.T) {
	table := provisionedTableWithIndex(t)

	// the index to create can not be built, so none of the settings apply
	_, err := table.UpdateTable("described", &types.UpdateTableInput{
		BillingMode:               new("PAY_PER_REQUEST"),
		DeletionProtectionEnabled: new(true),
		OnDemandThroughput:        onDemand(3, 3),
		AttributeDefinitions:      []*types.AttributeDefinition{{AttributeName: new("other"), AttributeType: new("S")}},
		GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{
			gsiCreate(new("by-undefined"), []*types.KeySchemaElement{{AttributeName: "undefined", KeyType: "HASH"}}, gsiAllProjection()),
		},
	})
	require.Error(t, err)

	d := table.Description("described")
	require.Nil(t, d.BillingModeSummary)
	require.Equal(t, throughput(5, 5), d.ProvisionedThroughput)
	require.False(t, *d.DeletionProtectionEnabled)
	require.Nil(t, d.OnDemandThroughput)
	require.NotContains(t, table.AttributesDef, "other")
	require.NoError(t, table.CheckDeletable())

	// and with every part valid they all do
	d, err = table.UpdateTable("described", &types.UpdateTableInput{
		BillingMode:               new("PAY_PER_REQUEST"),
		DeletionProtectionEnabled: new(true),
		OnDemandThroughput:        onDemand(3, 3),
		GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{
			{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: new("by-gk")}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "PAY_PER_REQUEST", d.BillingModeSummary.BillingMode)
	require.True(t, *d.DeletionProtectionEnabled)
	require.Equal(t, onDemand(3, 3), d.OnDemandThroughput)
	require.NotContains(t, table.Indexes, "by-gk")
}

func TestUpdateTableCreatedIndexFollowsTheNewBillingMode(t *testing.T) {
	table := newDescribedTable(t, "PAY_PER_REQUEST", nil)

	// an index created in the request that goes back to provisioned needs capacity
	_, err := table.UpdateTable("described", &types.UpdateTableInput{
		BillingMode:                 new("PROVISIONED"),
		ProvisionedThroughput:       throughput(3, 3),
		GlobalSecondaryIndexUpdates: []*types.GlobalSecondaryIndexUpdate{gsiCreate(new("by-gk"), gsiHashKey(), gsiAllProjection())},
		AttributeDefinitions:        []*types.AttributeDefinition{{AttributeName: new("gk"), AttributeType: new("N")}},
	})
	requireValidation(t, err, "ValidationException", testMsgNoGSIThroughput)
	requireDescribed(t, table, "PAY_PER_REQUEST", 0, 0)
	require.Empty(t, table.Indexes)
}
