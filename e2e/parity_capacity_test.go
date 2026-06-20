package e2e

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// capacitySnapshot is a stable, comparable projection of *types.ConsumedCapacity.
//
// minidyn computes consumed capacity from the documented docsize/AWS item-size algorithm
// (the contract). DynamoDB Local's exact byte-to-unit rounding can legitimately differ, so
// parity here is structural — presence, table name, and whether any capacity was charged —
// rather than exact unit equality. Exact values are pinned by the unit tests in the
// capacity and aws-v2/client packages. The spike test below logs the raw numbers for anyone
// reconciling minidyn against a specific DynamoDB Local build.
type capacitySnapshot struct {
	Present   bool
	TableName string
	Positive  bool // CapacityUnits > 0
}

func snapshotCapacity(cc *dynamodbtypes.ConsumedCapacity) capacitySnapshot {
	if cc == nil {
		return capacitySnapshot{}
	}

	return capacitySnapshot{
		Present:   true,
		TableName: aws.ToString(cc.TableName),
		Positive:  aws.ToFloat64(cc.CapacityUnits) > 0,
	}
}

func sortedCapacitySnapshots(ccs []dynamodbtypes.ConsumedCapacity) []capacitySnapshot {
	out := make([]capacitySnapshot, 0, len(ccs))
	for i := range ccs {
		out = append(out, snapshotCapacity(&ccs[i]))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].TableName < out[j].TableName })

	return out
}

// capacityBreakdown captures the structural shape of an INDEXES-mode ConsumedCapacity:
// which sub-objects DynamoDB reports (the per-table and per-index breakdown), not the exact
// unit magnitudes — those can legitimately differ between minidyn's docsize algorithm and a
// given DynamoDB Local build, and are pinned by the unit tests. Parity here asserts that
// minidyn reports the SAME breakdown structure DynamoDB does: whether a Table sub-object is
// present and which GSIs/LSIs are charged.
type capacityBreakdown struct {
	Present      bool
	TableName    string
	TablePresent bool     // cc.Table sub-object reported
	GSINames     []string // sorted GlobalSecondaryIndexes keys
	LSINames     []string // sorted LocalSecondaryIndexes keys
}

func snapshotBreakdown(cc *dynamodbtypes.ConsumedCapacity) capacityBreakdown {
	if cc == nil {
		return capacityBreakdown{}
	}

	return capacityBreakdown{
		Present:      true,
		TableName:    aws.ToString(cc.TableName),
		TablePresent: cc.Table != nil,
		GSINames:     sortedCapacityMapKeys(cc.GlobalSecondaryIndexes),
		LSINames:     sortedCapacityMapKeys(cc.LocalSecondaryIndexes),
	}
}

func sortedCapacityMapKeys(m map[string]dynamodbtypes.Capacity) []string {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// TestE2E_CapacitySpike runs against both engines and logs DynamoDB Local's actual
// ConsumedCapacity numbers (run with -v) so the implementer can reconcile minidyn's
// docsize-derived values against a specific DynamoDB Local build. It also asserts the
// structural parity invariant, so it doubles as a regression test.
func TestE2E_CapacitySpike(t *testing.T) {
	RunE2E(t, func(t *testing.T, client *dynamodb.Client) capacitySnapshot {
		t.Helper()
		ctx := context.Background()

		parityCreatePokemonTable(ctx, t, client)

		putOut, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:              aws.String(parityPokemonTable),
			Item:                   parityMarshalPokemon(t, parityPokemon{ID: "spike-1", Type: "fire", Name: "charmander", Level: 7}),
			ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		})
		require.NoError(t, err)
		t.Logf("PUT ConsumedCapacity = %#v", putOut.ConsumedCapacity)

		getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:              aws.String(parityPokemonTable),
			Key:                    map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "spike-1"}},
			ConsistentRead:         aws.Bool(true),
			ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
		})
		require.NoError(t, err)
		t.Logf("GET(consistent) ConsumedCapacity = %#v", getOut.ConsumedCapacity)

		return snapshotCapacity(getOut.ConsumedCapacity)
	})
}

func TestE2E_ConsumedCapacity(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "GetItemConsistentTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "water", Name: "squirtle"})

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName:              aws.String(parityPokemonTable),
					Key:                    map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
					ConsistentRead:         aws.Bool(true),
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "GetItemEventualTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "water", Name: "squirtle"})

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName:              aws.String(parityPokemonTable),
					Key:                    map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "GetItemNoneOmitsCapacity",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "water", Name: "squirtle"})

				out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "PutItemTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:              aws.String(parityPokemonTable),
					Item:                   parityMarshalPokemon(t, parityPokemon{ID: "cc-put", Type: "fire", Name: "ponyta", Level: 40}),
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "UpdateItemTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                aws.String(parityPokemonTable),
					Key:                      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
					UpdateExpression:         aws.String("SET #t = :t"),
					ExpressionAttributeNames: map[string]string{"#t": "type"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":t": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
					},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "DeleteItemTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName:              aws.String(parityPokemonTable),
					Key:                    map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "QueryTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                aws.String(parityPokemonTable),
					KeyConditionExpression:   aws.String("#id = :id"),
					ExpressionAttributeNames: map[string]string{"#id": "id"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "ScanTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:              aws.String(parityPokemonTable),
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
				})
				require.NoError(t, err)

				return snapshotCapacity(out.ConsumedCapacity)
			},
		},
		{
			name: "BatchGetItemTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
						parityPokemonTable: {Keys: []map[string]dynamodbtypes.AttributeValue{
							{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
						}},
					},
				})
				require.NoError(t, err)

				return sortedCapacitySnapshots(out.ConsumedCapacity)
			},
		},
		{
			name: "BatchWriteItemTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						parityPokemonTable: {
							{PutRequest: &dynamodbtypes.PutRequest{Item: parityMarshalPokemon(t, parityPokemon{ID: "bw1", Type: "fire", Name: "a"})}},
							{PutRequest: &dynamodbtypes.PutRequest{Item: parityMarshalPokemon(t, parityPokemon{ID: "bw2", Type: "fire", Name: "b"})}},
						},
					},
				})
				require.NoError(t, err)

				return sortedCapacitySnapshots(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactGetItemsTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					TransactItems: []dynamodbtypes.TransactGetItem{
						{Get: &dynamodbtypes.Get{
							TableName: aws.String(parityPokemonTable),
							Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
						}},
					},
				})
				require.NoError(t, err)

				return sortedCapacitySnapshots(out.ConsumedCapacity)
			},
		},
		{
			name: "TransactWriteItemsTotal",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityTotal,
					TransactItems: []dynamodbtypes.TransactWriteItem{
						{Put: &dynamodbtypes.Put{
							TableName: aws.String(parityPokemonTable),
							Item:      parityMarshalPokemon(t, parityPokemon{ID: "tw1", Type: "fire", Name: "x"}),
						}},
					},
				})
				require.NoError(t, err)

				return sortedCapacitySnapshots(out.ConsumedCapacity)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}

// TestE2E_ConsumedCapacityIndexes exercises ReturnConsumedCapacity=INDEXES, where DynamoDB
// reports a per-table/per-index breakdown in addition to the totals. These cover the paths the
// TOTAL tests above cannot: reads targeted at a secondary index (capacity attributed to the
// index) and writes against a table that has a GSI (capacity charged on the base table AND
// each affected GSI). Parity is on the breakdown SHAPE (snapshotBreakdown), not exact units.
func TestE2E_ConsumedCapacityIndexes(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			// Base-table query in INDEXES mode: DynamoDB reports a Table sub-object and no
			// index entries.
			name: "QueryBaseTableIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                aws.String(parityPokemonTable),
					KeyConditionExpression:   aws.String("#id = :id"),
					ExpressionAttributeNames: map[string]string{"#id": "id"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
					},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
				})
				require.NoError(t, err)
				t.Logf("Query(base) INDEXES ConsumedCapacity = %#v", out.ConsumedCapacity)

				return snapshotBreakdown(out.ConsumedCapacity)
			},
		},
		{
			// Query targeted at a GSI: capacity is attributed to the index, not the base table.
			name: "QueryOnGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                aws.String(parityPokemonTable),
					IndexName:                aws.String(parityByTypeIndexName),
					KeyConditionExpression:   aws.String("#type = :type"),
					ExpressionAttributeNames: map[string]string{"#type": "type"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":type": &dynamodbtypes.AttributeValueMemberS{Value: "grass"},
					},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
				})
				require.NoError(t, err)
				t.Logf("Query(GSI) INDEXES ConsumedCapacity = %#v", out.ConsumedCapacity)

				return snapshotBreakdown(out.ConsumedCapacity)
			},
		},
		{
			// Scan targeted at a GSI: capacity is attributed to the index.
			name: "ScanOnGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:              aws.String(parityPokemonTable),
					IndexName:              aws.String(parityByTypeIndexName),
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
				})
				require.NoError(t, err)
				t.Logf("Scan(GSI) INDEXES ConsumedCapacity = %#v", out.ConsumedCapacity)

				return snapshotBreakdown(out.ConsumedCapacity)
			},
		},
		{
			// Write against a table that has a GSI: DynamoDB charges write capacity on the base
			// table AND each affected GSI, so the breakdown includes the GSI.
			name: "PutItemWithGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)

				out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:              aws.String(parityPokemonTable),
					Item:                   parityMarshalPokemon(t, parityPokemon{ID: "cc-put-idx", Type: "fire", Name: "ponyta", Level: 40}),
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
				})
				require.NoError(t, err)
				t.Logf("Put(GSI) INDEXES ConsumedCapacity = %#v", out.ConsumedCapacity)

				return snapshotBreakdown(out.ConsumedCapacity)
			},
		},
		{
			name: "UpdateItemWithGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName:                aws.String(parityPokemonTable),
					Key:                      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
					UpdateExpression:         aws.String("SET #t = :t"),
					ExpressionAttributeNames: map[string]string{"#t": "type"},
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":t": &dynamodbtypes.AttributeValueMemberS{Value: "poison"},
					},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
				})
				require.NoError(t, err)
				t.Logf("Update(GSI) INDEXES ConsumedCapacity = %#v", out.ConsumedCapacity)

				return snapshotBreakdown(out.ConsumedCapacity)
			},
		},
		{
			name: "DeleteItemWithGSIIndexes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityAddByTypeGlobalSecondaryIndex(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "bulbasaur"})

				out, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
					TableName:              aws.String(parityPokemonTable),
					Key:                    map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
					ReturnConsumedCapacity: dynamodbtypes.ReturnConsumedCapacityIndexes,
				})
				require.NoError(t, err)
				t.Logf("Delete(GSI) INDEXES ConsumedCapacity = %#v", out.ConsumedCapacity)

				return snapshotBreakdown(out.ConsumedCapacity)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
