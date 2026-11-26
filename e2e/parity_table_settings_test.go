package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// The cases of this file send UpdateTable and CreateTable requests that change the billing mode, the
// capacity, the on-demand limits and the deletion protection of a table (#199), and compare what
// each engine answers and what DescribeTable reports afterwards. DynamoDB Local answers some
// requests with a 500 (a limit with only one side on UpdateTable, a bad billing mode, a capacity
// for an index of an on-demand table, going back to provisioned with an index that has no
// capacity), which is not a behavior to copy: each has a test that asserts both engines.

const settingsBadModeMessage = "1 validation error detected: Value 'FOO' at 'billingMode' failed to satisfy constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]"

func settingsThroughput(read, write int64) *dynamodbtypes.ProvisionedThroughput {
	return &dynamodbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(read), WriteCapacityUnits: aws.Int64(write)}
}

func settingsLimits(read, write int64) *dynamodbtypes.OnDemandThroughput {
	return &dynamodbtypes.OnDemandThroughput{MaxReadRequestUnits: aws.Int64(read), MaxWriteRequestUnits: aws.Int64(write)}
}

// settingsSummary lists what a description says about the settings: the billing mode summary, the
// capacity, the deletion protection, the on-demand limits and the capacity of each index. The
// timestamps and the decrease counters differ by engine and are left out.
func settingsSummary(d *dynamodbtypes.TableDescription) string {
	mode := "none"
	if d.BillingModeSummary != nil {
		mode = string(d.BillingModeSummary.BillingMode)
	}

	parts := []string{
		"mode=" + mode,
		fmt.Sprintf("throughput=%d/%d", aws.ToInt64(d.ProvisionedThroughput.ReadCapacityUnits), aws.ToInt64(d.ProvisionedThroughput.WriteCapacityUnits)),
		fmt.Sprintf("protected=%t", aws.ToBool(d.DeletionProtectionEnabled)),
	}

	if d.OnDemandThroughput != nil {
		parts = append(parts, fmt.Sprintf("limits=%d/%d", aws.ToInt64(d.OnDemandThroughput.MaxReadRequestUnits), aws.ToInt64(d.OnDemandThroughput.MaxWriteRequestUnits)))
	}

	indexes := []string{}

	for _, g := range d.GlobalSecondaryIndexes {
		capacity := "none"
		if g.ProvisionedThroughput != nil {
			capacity = fmt.Sprintf("%d/%d", aws.ToInt64(g.ProvisionedThroughput.ReadCapacityUnits), aws.ToInt64(g.ProvisionedThroughput.WriteCapacityUnits))
		}

		indexes = append(indexes, aws.ToString(g.IndexName)+"="+capacity)
	}

	sort.Strings(indexes)

	if len(indexes) > 0 {
		parts = append(parts, "indexes:"+strings.Join(indexes, ","))
	}

	return strings.Join(parts, " ")
}

func settingsDescribe(ctx context.Context, client *dynamodb.Client) string {
	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(parityPokemonTable)})
	if err != nil {
		return normalizeSDKErrorString(err.Error())
	}

	return settingsSummary(out.Table)
}

func settingsOutcome(out *dynamodbtypes.TableDescription, err error) string {
	if err != nil {
		return normalizeSDKErrorString(err.Error())
	}

	return settingsSummary(out)
}

type settingsStep func(ctx context.Context, client *dynamodb.Client) string

type settingsOption func(*dynamodb.UpdateTableInput)

func withMode(mode dynamodbtypes.BillingMode) settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.BillingMode = mode }
}

func withThroughput(read, write int64) settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.ProvisionedThroughput = settingsThroughput(read, write) }
}

func withEmptyThroughput() settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.ProvisionedThroughput = &dynamodbtypes.ProvisionedThroughput{} }
}

func withLimits(read, write int64) settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.OnDemandThroughput = settingsLimits(read, write) }
}

func withProtection(on bool) settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.DeletionProtectionEnabled = aws.Bool(on) }
}

func withIndexThroughput(name string, pt *dynamodbtypes.ProvisionedThroughput) settingsOption {
	return func(in *dynamodb.UpdateTableInput) {
		in.GlobalSecondaryIndexUpdates = append(in.GlobalSecondaryIndexUpdates, dynamodbtypes.GlobalSecondaryIndexUpdate{
			Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{IndexName: aws.String(name), ProvisionedThroughput: pt},
		})
	}
}

func withIndexEntries(entries ...dynamodbtypes.GlobalSecondaryIndexUpdate) settingsOption {
	return func(in *dynamodb.UpdateTableInput) {
		if in.GlobalSecondaryIndexUpdates == nil {
			in.GlobalSecondaryIndexUpdates = []dynamodbtypes.GlobalSecondaryIndexUpdate{}
		}

		in.GlobalSecondaryIndexUpdates = append(in.GlobalSecondaryIndexUpdates, entries...)
	}
}

func withReplicaUpdates(updates ...dynamodbtypes.ReplicationGroupUpdate) settingsOption {
	return func(in *dynamodb.UpdateTableInput) {
		in.ReplicaUpdates = append([]dynamodbtypes.ReplicationGroupUpdate{}, updates...)
	}
}

func withIndexCreate(name string, pt *dynamodbtypes.ProvisionedThroughput) settingsOption {
	return func(in *dynamodb.UpdateTableInput) {
		action := gsiCreateAction(name)
		action.ProvisionedThroughput = pt
		in.AttributeDefinitions = gsiAttributes()
		in.GlobalSecondaryIndexUpdates = append(in.GlobalSecondaryIndexUpdates, dynamodbtypes.GlobalSecondaryIndexUpdate{Create: action})
	}
}

func withIndexDelete(name string) settingsOption {
	return func(in *dynamodb.UpdateTableInput) {
		in.GlobalSecondaryIndexUpdates = append(in.GlobalSecondaryIndexUpdates, dynamodbtypes.GlobalSecondaryIndexUpdate{
			Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(name)},
		})
	}
}

func withAttributes(defs ...dynamodbtypes.AttributeDefinition) settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.AttributeDefinitions = defs }
}

// update sends an UpdateTable and reports its answer, then the description of the table.
func update(label string, options ...settingsOption) settingsStep {
	return func(ctx context.Context, client *dynamodb.Client) string {
		in := &dynamodb.UpdateTableInput{TableName: aws.String(parityPokemonTable)}
		for _, option := range options {
			option(in)
		}

		out, err := client.UpdateTable(ctx, in, skipClientValidation)

		var desc *dynamodbtypes.TableDescription
		if out != nil {
			desc = out.TableDescription
		}

		return label + ": " + settingsOutcome(desc, err) + " | describe: " + settingsDescribe(ctx, client)
	}
}

func deleteTable(label, table string) settingsStep {
	return func(ctx context.Context, client *dynamodb.Client) string {
		out, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})

		var desc *dynamodbtypes.TableDescription
		if out != nil {
			desc = out.TableDescription
		}

		return label + ": " + settingsOutcome(desc, err) + " | describe: " + settingsDescribe(ctx, client)
	}
}

type settingsCreateOption func(*dynamodb.CreateTableInput)

func provisioned(read, write int64) settingsCreateOption {
	return func(in *dynamodb.CreateTableInput) {
		in.BillingMode = dynamodbtypes.BillingModeProvisioned
		in.ProvisionedThroughput = settingsThroughput(read, write)
	}
}

func payPerRequest(in *dynamodb.CreateTableInput) {
	in.BillingMode = dynamodbtypes.BillingModePayPerRequest
}

func createLimits(odt *dynamodbtypes.OnDemandThroughput) settingsCreateOption {
	return func(in *dynamodb.CreateTableInput) { in.OnDemandThroughput = odt }
}

func createProtection(on bool) settingsCreateOption {
	return func(in *dynamodb.CreateTableInput) { in.DeletionProtectionEnabled = aws.Bool(on) }
}

func createThroughput(pt *dynamodbtypes.ProvisionedThroughput) settingsCreateOption {
	return func(in *dynamodb.CreateTableInput) { in.ProvisionedThroughput = pt }
}

// withIndex creates the index "existing" on the key "type"; pt is its capacity, nil for none.
func withIndex(pt *dynamodbtypes.ProvisionedThroughput) settingsCreateOption {
	return func(in *dynamodb.CreateTableInput) {
		in.AttributeDefinitions = gsiAttributes()
		in.GlobalSecondaryIndexes = []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("existing"), KeySchema: gsiKeySchema(), Projection: gsiAll(), ProvisionedThroughput: pt,
		}}
	}
}

// runSettings creates the table, then runs the steps, and compares the transcript of both engines.
func runSettings(t *testing.T, options []settingsCreateOption, steps ...settingsStep) {
	t.Helper()

	RunE2E(t, func(t *testing.T, client *dynamodb.Client) []string {
		t.Helper()

		return settingsTranscript(t, client, options, steps)
	})
}

// settingsTranscript creates the table, runs the steps and returns what the engine answered.
func settingsTranscript(t *testing.T, client *dynamodb.Client, options []settingsCreateOption, steps []settingsStep) []string {
	t.Helper()

	ctx := context.Background()

	in := &dynamodb.CreateTableInput{
		TableName:            aws.String(parityPokemonTable),
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
		KeySchema:            []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}},
	}
	for _, option := range options {
		option(in)
	}

	created, err := client.CreateTable(ctx, in, skipClientValidation)
	if err != nil {
		// a rejected request leaves no table
		return []string{"create: " + normalizeSDKErrorString(err.Error()), "describe: " + settingsDescribe(ctx, client)}
	}

	out := []string{"create: " + settingsSummary(created.TableDescription)}

	for _, g := range in.GlobalSecondaryIndexes {
		parityWaitUntilGSIActive(ctx, t, client, parityPokemonTable, aws.ToString(g.IndexName))
	}

	for _, step := range steps {
		out = append(out, step(ctx, client))
	}

	return out
}

// runDivergence runs the same transcript on each engine and asserts what each one answers, for the
// requests where minidyn follows the API reference and DynamoDB Local does something else.
func runDivergence(t *testing.T, options []settingsCreateOption, steps []settingsStep, minidyn, local []string) {
	t.Helper()

	run := func(t *testing.T, endpoint string) []string {
		t.Helper()

		return settingsTranscript(t, newTracedSDKClient(t, endpoint, &traceRecorder{}), options, steps)
	}

	t.Run("minidyn", func(t *testing.T) {
		require.Equal(t, minidyn, run(t, setupMinidynEndpoint(t)))
	})

	t.Run("dynamodb-local", func(t *testing.T) {
		require.Equal(t, local, run(t, setupDynamoDBLocalEndpoint(t)))
	})
}

func TestE2E_UpdateTableBillingMode(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5)},
			update("ToPayPerRequest", withMode(dynamodbtypes.BillingModePayPerRequest)),
			update("ToPayPerRequestAgain", withMode(dynamodbtypes.BillingModePayPerRequest)),
			update("ToProvisionedWithoutThroughput", withMode(dynamodbtypes.BillingModeProvisioned)),
			update("ToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(7, 8)),
			update("ToProvisionedAgain", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(2, 3)),
		)
	})

	t.Run("ThroughputWithPayPerRequest", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5)},
			update("ToPayPerRequestWithThroughput", withMode(dynamodbtypes.BillingModePayPerRequest), withThroughput(3, 3)),
			update("ThroughputOnly", withThroughput(9, 9)),
			update("ToProvisionedWithoutThroughput", withMode(dynamodbtypes.BillingModeProvisioned)),
			update("ToProvisionedLowerThroughput", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(1, 1)),
		)
	})

	t.Run("PayPerRequestInvalidThroughput", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5)},
			update("ToPayPerRequestWithZero", withMode(dynamodbtypes.BillingModePayPerRequest), withThroughput(0, 0)),
			update("ToPayPerRequestWithNegative", withMode(dynamodbtypes.BillingModePayPerRequest), withThroughput(-1, 1)),
			update("ToPayPerRequestWithSame", withMode(dynamodbtypes.BillingModePayPerRequest), withThroughput(5, 5)),
			update("ToProvisionedWithZero", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(0, 1)),
		)
	})

	t.Run("CreatedOnDemand", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{payPerRequest},
			update("ToProvisionedWithoutThroughput", withMode(dynamodbtypes.BillingModeProvisioned)),
			update("ToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(4, 6)),
			update("ToPayPerRequest", withMode(dynamodbtypes.BillingModePayPerRequest)),
		)
	})

	t.Run("SameModeProvisioned", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5)},
			update("SameMode", withMode(dynamodbtypes.BillingModeProvisioned)),
			update("SameModeSameThroughput", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(5, 5)),
		)
	})
}

func TestE2E_UpdateTableProvisionedThroughput(t *testing.T) {
	tests := []struct {
		name  string
		steps []settingsStep
	}{
		{"SameValues", []settingsStep{update("Same", withThroughput(5, 5)), update("SameAgain", withThroughput(5, 5))}},
		{"IncreaseAndDecrease", []settingsStep{
			update("Increase", withThroughput(6, 5)),
			update("Decrease", withThroughput(6, 4)),
			update("SameAsNow", withThroughput(6, 4)),
		}},
		{"BelowOne", []settingsStep{
			update("Zero", withThroughput(0, 0)),
			update("ZeroRead", withThroughput(0, 5)),
			update("ZeroWrite", withThroughput(5, 0)),
			update("NegativeRead", withThroughput(-1, 5)),
			update("NegativeWrite", withThroughput(5, -1)),
			update("Negative", withThroughput(-1, -1)),
			update("NoValues", withEmptyThroughput()),
		}},
		{"RejectedChangesNothing", []settingsStep{
			update("ThroughputZeroWithProtection", withThroughput(0, 0), withProtection(true)),
			update("ThroughputZeroWithMode", withThroughput(0, 0), withMode(dynamodbtypes.BillingModePayPerRequest)),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runSettings(t, []settingsCreateOption{provisioned(5, 5)}, tt.steps...)
		})
	}
}

func TestE2E_UpdateTableNothingToUpdate(t *testing.T) {
	runSettings(t, []settingsCreateOption{provisioned(5, 5)},
		update("Empty"),
		update("EmptyIndexUpdates", withIndexEntries()),
		update("AttributesOnly", withAttributes(dynamodbtypes.AttributeDefinition{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS})),
		update("BadAttributeOnly", withAttributes(dynamodbtypes.AttributeDefinition{AttributeName: aws.String("id"), AttributeType: "X"})),
		update("ProtectionOff", withProtection(false)),
		update("SameMode", withMode(dynamodbtypes.BillingModeProvisioned)),
		update("LimitsOnProvisioned", withLimits(5, 5)),
		update("EmptyReplicaUpdates", withReplicaUpdates()),
		update("ThroughputBeforeAttributes", withThroughput(-1, 1), withAttributes(dynamodbtypes.AttributeDefinition{AttributeName: aws.String("id"), AttributeType: "X"})),
	)
}

func TestE2E_DeletionProtection(t *testing.T) {
	t.Run("CreateProtected", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{payPerRequest, createProtection(true)},
			deleteTable("DeleteProtected", parityPokemonTable),
			deleteTable("DeleteProtectedAgain", parityPokemonTable),
			update("Unprotect", withProtection(false)),
			deleteTable("Delete", parityPokemonTable),
			deleteTable("DeleteMissing", parityPokemonTable),
		)
	})

	t.Run("CreateUnprotected", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{payPerRequest, createProtection(false)},
			update("Protect", withProtection(true)),
			update("ProtectAgain", withProtection(true)),
			deleteTable("DeleteProtected", parityPokemonTable),
			update("Unprotect", withProtection(false)),
			update("UnprotectAgain", withProtection(false)),
			deleteTable("Delete", parityPokemonTable),
		)
	})

	t.Run("OtherSettingsKeepIt", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5), createProtection(true)},
			update("Throughput", withThroughput(6, 6)),
			update("ToPayPerRequest", withMode(dynamodbtypes.BillingModePayPerRequest)),
			deleteTable("DeleteProtected", parityPokemonTable),
			update("ProtectedWithMode", withProtection(false), withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(2, 2)),
			deleteTable("Delete", parityPokemonTable),
		)
	})

	t.Run("RejectedRequestKeepsIt", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5), withIndex(settingsThroughput(5, 5))},
			update("ProtectWithBadThroughput", withProtection(true), withThroughput(0, 1)),
			update("ProtectWithMissingIndex", withProtection(true), withIndexThroughput("missing", settingsThroughput(1, 1))),
			update("ProtectWithBadEntry", withProtection(true), withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{})),
			update("ProtectWithExistingIndex", withProtection(true), withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{Create: gsiCreateAction("existing")})),
			deleteTable("Delete", parityPokemonTable),
		)
	})
}

func TestE2E_OnDemandThroughput(t *testing.T) {
	t.Run("UpdateLimits", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{payPerRequest, createLimits(settingsLimits(100, 50))},
			update("Raise", withLimits(200, 60)),
			update("Same", withLimits(200, 60)),
			update("Smallest", withLimits(1, 1)),
			update("NoLimit", withLimits(-1, -1)),
			update("RaiseAgain", withLimits(300, 70), withMode(dynamodbtypes.BillingModePayPerRequest)),
		)
	})

	t.Run("OnProvisionedTable", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5), createLimits(settingsLimits(-3, 1))},
			update("Limits", withLimits(10, 10)),
			update("ToPayPerRequestWithLimits", withMode(dynamodbtypes.BillingModePayPerRequest), withLimits(10, 20)),
			update("BackToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(2, 2)),
			update("LimitsAgain", withLimits(10, 10), withMode(dynamodbtypes.BillingModeProvisioned)),
		)
	})

	t.Run("SwitchingModeDropsLimits", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{provisioned(5, 5)},
			update("ToPayPerRequestWithLimits", withMode(dynamodbtypes.BillingModePayPerRequest), withLimits(20, 30), withProtection(true)),
			update("BackToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(3, 3), withProtection(false)),
			update("ToPayPerRequest", withMode(dynamodbtypes.BillingModePayPerRequest)),
		)
	})
}

func TestE2E_CreateTableSettings(t *testing.T) {
	tests := []struct {
		name    string
		options []settingsCreateOption
	}{
		{"Limits", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(100, 50))}},
		{"SmallestLimits", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(1, 1))}},
		{"NoLimits", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(-1, -1))}},
		{"ZeroLimits", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(0, 0))}},
		{"ZeroReadLimit", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(0, 5))}},
		{"ZeroWriteLimit", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(5, 0))}},
		{"ReadLimitBelowNoLimit", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(-5, 5))}},
		{"WriteLimitBelowNoLimit", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(5, -5))}},
		{"BothLimitsBelowNoLimit", []settingsCreateOption{payPerRequest, createLimits(settingsLimits(-2, -2))}},
		{"EmptyLimits", []settingsCreateOption{payPerRequest, createLimits(&dynamodbtypes.OnDemandThroughput{})}},
		{"LimitsOnProvisioned", []settingsCreateOption{provisioned(5, 5), createLimits(settingsLimits(10, 10))}},
		{"BadLimitsOnProvisioned", []settingsCreateOption{provisioned(5, 5), createLimits(settingsLimits(-3, 1))}},
		{"ProtectedWithLimits", []settingsCreateOption{payPerRequest, createProtection(true), createLimits(settingsLimits(9, 9))}},
		{"ProtectedProvisioned", []settingsCreateOption{provisioned(5, 5), createProtection(true)}},
		{"ZeroThroughput", []settingsCreateOption{provisioned(0, 0)}},
		{"ZeroReadThroughput", []settingsCreateOption{provisioned(0, 5)}},
		{"ZeroWriteThroughput", []settingsCreateOption{provisioned(5, 0)}},
		{"NegativeThroughput", []settingsCreateOption{provisioned(-1, 1)}},
		{"NoThroughput", []settingsCreateOption{func(in *dynamodb.CreateTableInput) { in.BillingMode = dynamodbtypes.BillingModeProvisioned }}},
		{"ThroughputWithPayPerRequest", []settingsCreateOption{payPerRequest, createThroughput(settingsThroughput(5, 5))}},
		{"ZeroThroughputWithPayPerRequest", []settingsCreateOption{payPerRequest, createThroughput(settingsThroughput(0, 0))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runSettings(t, tt.options)
		})
	}
}

func TestE2E_UpdateTableIndexThroughput(t *testing.T) {
	indexed := []settingsCreateOption{provisioned(5, 5), withIndex(settingsThroughput(5, 5))}

	t.Run("Change", func(t *testing.T) {
		runSettings(t, indexed,
			update("Change", withIndexThroughput("existing", settingsThroughput(8, 9))),
			update("Same", withIndexThroughput("existing", settingsThroughput(8, 9))),
			update("DecreaseRead", withIndexThroughput("existing", settingsThroughput(4, 9))),
			update("Zero", withIndexThroughput("existing", settingsThroughput(0, 0))),
			update("ZeroWrite", withIndexThroughput("existing", settingsThroughput(5, 0))),
			update("Negative", withIndexThroughput("existing", settingsThroughput(-1, 1))),
			update("NoThroughput", withIndexThroughput("existing", nil)),
			update("MissingIndex", withIndexThroughput("missing", settingsThroughput(1, 1))),
		)
	})

	t.Run("WithTableThroughput", func(t *testing.T) {
		runSettings(t, indexed,
			update("Both", withThroughput(3, 3), withIndexThroughput("existing", settingsThroughput(4, 4))),
			update("TableZeroIndexSame", withThroughput(0, 0), withIndexThroughput("existing", settingsThroughput(4, 4))),
			update("TableSameIndexZero", withThroughput(3, 3), withIndexThroughput("existing", settingsThroughput(0, 0))),
			update("TableOkIndexNegative", withThroughput(6, 6), withIndexThroughput("existing", settingsThroughput(-1, 4))),
			update("TableZeroIndexMissing", withThroughput(0, 0), withIndexThroughput("missing", settingsThroughput(1, 1))),
			update("TableSameIndexMissing", withThroughput(3, 3), withIndexThroughput("missing", settingsThroughput(1, 1))),
		)
	})

	t.Run("IndexEntryBeforeTableSettings", func(t *testing.T) {
		runSettings(t, indexed,
			update("BadEntryAndZero", withThroughput(-1, 1), withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{})),
			update("BadEntryAndMode", withMode(dynamodbtypes.BillingModeProvisioned), withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{})),
			update("BadEntryAndProtection", withProtection(true), withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{})),
			update("ExistingIndexAndZero", withThroughput(0, 0), withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{Create: gsiCreateAction("existing")})),
		)
	})

	t.Run("BillingModeKeepsIndexThroughput", func(t *testing.T) {
		runSettings(t, indexed,
			update("ToPayPerRequest", withMode(dynamodbtypes.BillingModePayPerRequest)),
			// an index update with limits and no capacity is accepted on an on-demand table, and
			// both engines say there is nothing to change
			update("LimitsForIndex", withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{
				Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{IndexName: aws.String("existing"), OnDemandThroughput: settingsLimits(10, 10)},
			})),
			update("BackToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(2, 2)),
		)
	})

	t.Run("IndexOnDemandBecomesProvisioned", func(t *testing.T) {
		runSettings(t, []settingsCreateOption{payPerRequest, withIndex(nil)},
			// an index update with limits and no capacity is accepted on an on-demand table, and
			// both engines say there is nothing to change
			update("LimitsForIndex", withIndexEntries(dynamodbtypes.GlobalSecondaryIndexUpdate{
				Update: &dynamodbtypes.UpdateGlobalSecondaryIndexAction{IndexName: aws.String("existing"), OnDemandThroughput: settingsLimits(10, 10)},
			})),
			update("BackToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(3, 3), withIndexThroughput("existing", settingsThroughput(2, 2))),
		)
	})
}

// TestE2E_UpdateTableKeepsOnDemandThroughput covers a divergence from DynamoDB Local. Local
// removes the on-demand limits of a table in any UpdateTable that does not carry them (a
// deletion protection or a capacity change alone is enough). The UpdateTable reference says
// "Modifies the provisioned throughput settings, global secondary indexes, or DynamoDB Streams
// settings for a given table" and the call changes what the request names, so minidyn keeps the
// limits. Each engine's behavior is asserted separately.
func TestE2E_UpdateTableKeepsOnDemandThroughput(t *testing.T) {
	run := func(t *testing.T, endpoint string) string {
		t.Helper()

		ctx := context.Background()
		client := newTracedSDKClient(t, endpoint, &traceRecorder{})

		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:            aws.String(parityPokemonTable),
			BillingMode:          dynamodbtypes.BillingModePayPerRequest,
			OnDemandThroughput:   settingsLimits(100, 50),
			AttributeDefinitions: []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
			KeySchema:            []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}},
		})
		require.NoError(t, err)

		_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String(parityPokemonTable), DeletionProtectionEnabled: aws.Bool(true)})
		require.NoError(t, err)

		return settingsDescribe(ctx, client)
	}

	t.Run("minidyn", func(t *testing.T) {
		require.Equal(t, "mode=PAY_PER_REQUEST throughput=0/0 protected=true limits=100/50", run(t, setupMinidynEndpoint(t)))
	})

	t.Run("dynamodb-local", func(t *testing.T) {
		require.Equal(t, "mode=PAY_PER_REQUEST throughput=0/0 protected=true", run(t, setupDynamoDBLocalEndpoint(t)))
	})
}

// TestE2E_UpdateTableInvalidBillingMode covers a divergence from DynamoDB Local, which answers a
// BillingMode outside the enum with a 500 (InternalFailure). minidyn rejects it with the
// ValidationException DynamoDB documents for an enum member, and changes nothing.
func TestE2E_UpdateTableInvalidBillingMode(t *testing.T) {
	run := func(t *testing.T, endpoint string) error {
		t.Helper()

		ctx := context.Background()
		client := newTracedSDKClient(t, endpoint, &traceRecorder{})

		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:             aws.String(parityPokemonTable),
			BillingMode:           dynamodbtypes.BillingModeProvisioned,
			ProvisionedThroughput: settingsThroughput(5, 5),
			AttributeDefinitions:  []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
			KeySchema:             []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}},
		})
		require.NoError(t, err)

		_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String(parityPokemonTable), BillingMode: "FOO"}, skipClientValidation)

		require.Equal(t, "mode=none throughput=5/5 protected=false", settingsDescribe(ctx, client))

		return err
	}

	t.Run("minidyn", func(t *testing.T) {
		err := run(t, setupMinidynEndpoint(t))

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "ValidationException", apiErr.ErrorCode())
		require.Equal(t, settingsBadModeMessage, apiErr.ErrorMessage())
	})

	t.Run("dynamodb-local", func(t *testing.T) {
		var apiErr smithy.APIError

		require.ErrorAs(t, run(t, setupDynamoDBLocalEndpoint(t)), &apiErr)
		require.Equal(t, "InternalFailure", apiErr.ErrorCode())
	})
}

func onlyRead(n int64) *dynamodbtypes.OnDemandThroughput {
	return &dynamodbtypes.OnDemandThroughput{MaxReadRequestUnits: aws.Int64(n)}
}

func onlyWrite(n int64) *dynamodbtypes.OnDemandThroughput {
	return &dynamodbtypes.OnDemandThroughput{MaxWriteRequestUnits: aws.Int64(n)}
}

func withLimitsOf(odt *dynamodbtypes.OnDemandThroughput) settingsOption {
	return func(in *dynamodb.UpdateTableInput) { in.OnDemandThroughput = odt }
}

// TestE2E_OnDemandThroughputDocs covers the divergences from DynamoDB Local in the on-demand limits
// (see e2e/README.md). The API reference says of OnDemandThroughput "If you use this parameter,
// you must specify MaxReadRequestUnits, MaxWriteRequestUnits, or both" and lists each as
// "Required: No", and that a limit is "greater than or equal to 1", or -1 to remove it. minidyn
// follows it: a limit with one side is accepted (the given side is stored or replaced, the other
// one stays on UpdateTable and is absent on CreateTable, and a table describes an absent side as
// -1), a -1 removes only its side, and 0 or lower than -1 is rejected on UpdateTable as on
// CreateTable. Local rejects one side on CreateTable, answers a 500 to one side or to none on
// UpdateTable, drops both limits when either side is -1 and takes 0 as "no limit" on UpdateTable.
func TestE2E_OnDemandThroughputDocs(t *testing.T) {
	const (
		onDemandNoLimits = "mode=PAY_PER_REQUEST throughput=0/0 protected=false"
		baseTable        = "create: " + onDemandNoLimits + " limits=5/5"
		localNoTable     = "describe: operation error DynamoDB: DescribeTable, https response error StatusCode: 400, RequestID: , ResourceNotFoundException: Cannot do operations on a non-existent table"
		localCreateError = "create: operation error DynamoDB: CreateTable, https response error StatusCode: 400, RequestID: , api error ValidationException: Table read and write throughput must be at least 1, and cannot be null"
		local500         = "operation error DynamoDB: UpdateTable, https response error StatusCode: 500, RequestID: , api error InternalFailure: The request processing has failed because of an unknown error, exception or failure."
		outsideRange     = "operation error DynamoDB: UpdateTable, https response error StatusCode: 400, RequestID: , api error ValidationException: One or more parameter values were invalid: Requested MaxReadRequestUnits for OnDemandThroughput for table is outside of valid range"
		emptyLimits      = "operation error DynamoDB: UpdateTable, https response error StatusCode: 400, RequestID: , api error ValidationException: Table read and write throughput must be at least 1, and cannot be null"
	)

	creates := []struct {
		name    string
		limits  *dynamodbtypes.OnDemandThroughput
		minidyn string
		local   []string
	}{
		{"CreateOnlyRead", onlyRead(5), "create: " + onDemandNoLimits + " limits=5/-1", []string{localCreateError, localNoTable}},
		{"CreateOnlyWrite", onlyWrite(5), "create: " + onDemandNoLimits + " limits=-1/5", []string{localCreateError, localNoTable}},
		{"CreateReadNoLimit", settingsLimits(-1, 5), "create: " + onDemandNoLimits + " limits=-1/5", []string{"create: " + onDemandNoLimits}},
		{"CreateWriteNoLimit", settingsLimits(5, -1), "create: " + onDemandNoLimits + " limits=5/-1", []string{"create: " + onDemandNoLimits}},
	}

	for _, tt := range creates {
		t.Run(tt.name, func(t *testing.T) {
			runDivergence(t, []settingsCreateOption{payPerRequest, createLimits(tt.limits)}, nil, []string{tt.minidyn}, tt.local)
		})
	}

	// each update is described as it is answered, so the transcript is the answer and the table
	// it leaves
	described := func(limits string) string {
		if limits != "" {
			limits = " limits=" + limits
		}

		return "mode=PAY_PER_REQUEST throughput=0/0 protected=false" + limits
	}

	updates := []struct {
		name    string
		limits  *dynamodbtypes.OnDemandThroughput
		minidyn string
		local   string
	}{
		{"UpdateOnlyRead", onlyRead(9), "Update: " + described("9/5") + " | describe: " + described("9/5"), "Update: " + local500 + " | describe: " + described("5/5")},
		{"UpdateOnlyWrite", onlyWrite(9), "Update: " + described("5/9") + " | describe: " + described("5/9"), "Update: " + local500 + " | describe: " + described("5/5")},
		{"UpdateEmpty", &dynamodbtypes.OnDemandThroughput{}, "Update: " + emptyLimits + " | describe: " + described("5/5"), "Update: " + local500 + " | describe: " + described("5/5")},
		{"UpdateZero", settingsLimits(0, 0), "Update: " + outsideRange + " | describe: " + described("5/5"), "Update: " + described("") + " | describe: " + described("")},
		{"UpdateZeroSide", settingsLimits(0, 3), "Update: " + outsideRange + " | describe: " + described("5/5"), "Update: " + described("") + " | describe: " + described("")},
		{"UpdateBelowNoLimit", settingsLimits(-5, -1), "Update: " + outsideRange + " | describe: " + described("5/5"), "Update: " + described("") + " | describe: " + described("")},
		{"UpdateWriteNoLimit", settingsLimits(7, -1), "Update: " + described("7/-1") + " | describe: " + described("7/-1"), "Update: " + described("") + " | describe: " + described("")},
	}

	for _, tt := range updates {
		t.Run(tt.name, func(t *testing.T) {
			runDivergence(t, []settingsCreateOption{payPerRequest, createLimits(settingsLimits(5, 5))},
				[]settingsStep{update("Update", withLimitsOf(tt.limits))},
				[]string{baseTable, tt.minidyn}, []string{baseTable, tt.local})
		})
	}
}

const (
	settingsLocal500Update = "operation error DynamoDB: UpdateTable, https response error StatusCode: 500, RequestID: , api error InternalFailure: The request processing has failed because of an unknown error, exception or failure."
	settingsLocal500Create = "operation error DynamoDB: CreateTable, https response error StatusCode: 500, RequestID: , api error InternalFailure: The request processing has failed because of an unknown error, exception or failure."
	settingsOnDemandIndex  = "create: mode=PAY_PER_REQUEST throughput=0/0 protected=false indexes:existing=none"
)

// TestE2E_UpdateTableIndexCapacityOnDemand covers a divergence from DynamoDB Local, which answers a
// capacity for a global secondary index of an on-demand table with a 500 (InternalFailure).
// minidyn rejects it with the text DynamoDB gives for an index created that way, and changes
// nothing.
func TestE2E_UpdateTableIndexCapacityOnDemand(t *testing.T) {
	const described = "mode=PAY_PER_REQUEST throughput=0/0 protected=false indexes:existing=none"

	runDivergence(t, []settingsCreateOption{payPerRequest, withIndex(nil)},
		[]settingsStep{update("IndexCapacity", withIndexThroughput("existing", settingsThroughput(9, 9)))},
		[]string{settingsOnDemandIndex, "IndexCapacity: operation error DynamoDB: UpdateTable, https response error StatusCode: 400, RequestID: , api error ValidationException: " +
			"One or more parameter values were invalid: ProvisionedThroughput should not be specified for index: existing when BillingMode is PAY_PER_REQUEST | describe: " + described},
		[]string{settingsOnDemandIndex, "IndexCapacity: " + settingsLocal500Update + " | describe: " + described})
}

// TestE2E_UpdateTableProvisionedNeedsIndexCapacity covers a divergence from DynamoDB Local, which
// answers going back to provisioned with a global secondary index that has no capacity with a 500
// (InternalFailure). minidyn rejects it with the text DynamoDB gives when a table is created
// that way, and changes nothing.
func TestE2E_UpdateTableProvisionedNeedsIndexCapacity(t *testing.T) {
	const described = "mode=PAY_PER_REQUEST throughput=0/0 protected=false indexes:existing=none"

	runDivergence(t, []settingsCreateOption{payPerRequest, withIndex(nil)},
		[]settingsStep{update("BackToProvisioned", withMode(dynamodbtypes.BillingModeProvisioned), withThroughput(3, 3))},
		[]string{settingsOnDemandIndex, "BackToProvisioned: operation error DynamoDB: UpdateTable, https response error StatusCode: 400, RequestID: , api error ValidationException: " +
			"No provisioned throughput specified for the global secondary index | describe: " + described},
		[]string{settingsOnDemandIndex, "BackToProvisioned: " + settingsLocal500Update + " | describe: " + described})
}

// TestE2E_CreateTableInvalidBillingMode covers a divergence from DynamoDB Local, which answers a
// BillingMode outside the enum with a 500 (InternalFailure) on CreateTable, as it does on
// UpdateTable. minidyn rejects it with the ValidationException DynamoDB documents for an enum
// member, and creates no table.
func TestE2E_CreateTableInvalidBillingMode(t *testing.T) {
	bad := func(in *dynamodb.CreateTableInput) {
		in.BillingMode = "FOO"
		in.ProvisionedThroughput = settingsThroughput(5, 5)
	}
	noTable := "describe: operation error DynamoDB: DescribeTable, https response error StatusCode: 400, RequestID: , ResourceNotFoundException: Cannot do operations on a non-existent table"

	runDivergence(t, []settingsCreateOption{bad}, nil,
		[]string{"create: operation error DynamoDB: CreateTable, https response error StatusCode: 400, RequestID: , api error ValidationException: " + settingsBadModeMessage, noTable},
		[]string{"create: " + settingsLocal500Create, noTable})
}

// withLocalIndex creates a table with a sort key and a local secondary index named "by-local".
func withLocalIndex(in *dynamodb.CreateTableInput) {
	in.AttributeDefinitions = gsiAttributes()
	in.KeySchema = []dynamodbtypes.KeySchemaElement{
		{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
		{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeRange},
	}
	in.LocalSecondaryIndexes = []dynamodbtypes.LocalSecondaryIndex{{
		IndexName: aws.String("by-local"),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		Projection: gsiAll(),
	}}
}

// localIndexes lists the local secondary indexes of the table, to see a request left them alone.
func localIndexes(label string) settingsStep {
	return func(ctx context.Context, client *dynamodb.Client) string {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(parityPokemonTable)})
		if err != nil {
			return label + ": " + normalizeSDKErrorString(err.Error())
		}

		names := []string{}
		for _, l := range out.Table.LocalSecondaryIndexes {
			names = append(names, aws.ToString(l.IndexName))
		}

		return label + ": " + strings.Join(names, ",")
	}
}

// A local secondary index has no capacity to change: UpdateTable answers an index update that names
// it as it does one that names an index the table does not have. Deleting it is refused with a
// message of its own, and an unknown name keeps ResourceNotFoundException.
func TestE2E_UpdateTableLocalIndexIsNotGlobal(t *testing.T) {
	runSettings(t, []settingsCreateOption{provisioned(5, 5), withLocalIndex},
		update("UpdateLocal", withIndexThroughput("by-local", settingsThroughput(8, 9))),
		update("UpdateMissing", withIndexThroughput("by-missing", settingsThroughput(8, 9))),
		update("DeleteMissing", withIndexDelete("by-missing")),
		update("DeleteLocal", withIndexDelete("by-local")),
		localIndexes("LocalIndexes"),
	)
}

func TestE2E_GlobalIndexCapacity(t *testing.T) {
	t.Run("CreateTableProvisioned", func(t *testing.T) {
		for name, pt := range map[string]*dynamodbtypes.ProvisionedThroughput{
			"Zero": settingsThroughput(0, 0), "ZeroRead": settingsThroughput(0, 1), "ZeroWrite": settingsThroughput(1, 0), "Negative": settingsThroughput(-1, 1),
		} {
			t.Run(name, func(t *testing.T) {
				runSettings(t, []settingsCreateOption{provisioned(5, 5), withIndex(pt)})
			})
		}
	})

	t.Run("UpdateTableProvisioned", func(t *testing.T) {
		for name, pt := range map[string]*dynamodbtypes.ProvisionedThroughput{
			"Zero": settingsThroughput(0, 0), "ZeroRead": settingsThroughput(0, 1), "ZeroWrite": settingsThroughput(1, 0), "Negative": settingsThroughput(-1, 1), "Valid": settingsThroughput(1, 1),
		} {
			t.Run(name, func(t *testing.T) {
				runSettings(t, []settingsCreateOption{provisioned(5, 5)}, update("Create", withIndexCreate("created", pt)))
			})
		}
	})
}

// TestE2E_GlobalIndexCapacityOnDemandCreate covers a divergence from DynamoDB Local, which accepts a
// capacity for a global secondary index created on an on-demand table: CreateTable stores it
// and UpdateTable drops it. The API reference says of a table's ProvisionedThroughput that it
// must not be given for a PAY_PER_REQUEST table, and DynamoDB answers it for an index with
// "ProvisionedThroughput should not be specified for index: ... when BillingMode is
// PAY_PER_REQUEST". minidyn rejects the request on both calls and changes nothing.
func TestE2E_GlobalIndexCapacityOnDemandCreate(t *testing.T) {
	const (
		described = "mode=PAY_PER_REQUEST throughput=0/0 protected=false"
		noTable   = "describe: operation error DynamoDB: DescribeTable, https response error StatusCode: 400, RequestID: , ResourceNotFoundException: Cannot do operations on a non-existent table"
		invalid   = "operation error DynamoDB: %s, https response error StatusCode: 400, RequestID: , api error ValidationException: " +
			"One or more parameter values were invalid: ProvisionedThroughput should not be specified for index: %s when BillingMode is PAY_PER_REQUEST"
	)

	for _, capacity := range []struct {
		name string
		pt   *dynamodbtypes.ProvisionedThroughput
		text string
	}{
		{"Capacity", settingsThroughput(5, 5), "5/5"},
		{"ZeroCapacity", settingsThroughput(0, 0), "0/0"},
	} {
		t.Run("CreateTable"+capacity.name, func(t *testing.T) {
			runDivergence(t, []settingsCreateOption{payPerRequest, withIndex(capacity.pt)}, nil,
				[]string{"create: " + fmt.Sprintf(invalid, "CreateTable", "existing"), noTable},
				[]string{"create: " + described + " indexes:existing=" + capacity.text})
		})

		t.Run("UpdateTable"+capacity.name, func(t *testing.T) {
			runDivergence(t, []settingsCreateOption{payPerRequest},
				[]settingsStep{update("Create", withIndexCreate("created", capacity.pt))},
				[]string{"create: " + described, "Create: " + fmt.Sprintf(invalid, "UpdateTable", "created") + " | describe: " + described},
				[]string{"create: " + described, "Create: " + described + " indexes:created=none | describe: " + described + " indexes:created=none"})
		})
	}
}

// TestE2E_UpdateTableDecreaseCounter compares the table capacity DescribeTable reports after a
// decrease, with the number of decreases made today.
func TestE2E_UpdateTableDecreaseCounter(t *testing.T) {
	RunE2E(t, func(t *testing.T, client *dynamodb.Client) []string {
		t.Helper()

		ctx := context.Background()
		out := []string{}

		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:             aws.String(parityPokemonTable),
			BillingMode:           dynamodbtypes.BillingModeProvisioned,
			ProvisionedThroughput: settingsThroughput(5, 5),
			AttributeDefinitions:  []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
			KeySchema:             []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}},
		})
		require.NoError(t, err)

		describe := func(label string) {
			desc, describeErr := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(parityPokemonTable)})
			require.NoError(t, describeErr)

			pt := desc.Table.ProvisionedThroughput
			out = append(out, fmt.Sprintf("%s: %d/%d decreases=%d", label,
				aws.ToInt64(pt.ReadCapacityUnits), aws.ToInt64(pt.WriteCapacityUnits), aws.ToInt64(pt.NumberOfDecreasesToday)))
		}

		describe("Created")

		_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String(parityPokemonTable), ProvisionedThroughput: settingsThroughput(3, 4)})
		require.NoError(t, err)
		describe("Lowered")

		_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String(parityPokemonTable), ProvisionedThroughput: settingsThroughput(6, 6)})
		require.NoError(t, err)
		describe("Raised")

		return out
	})
}
