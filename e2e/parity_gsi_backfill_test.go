package e2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An index created by UpdateTable holds the items the table already had (#271).

func backfillStr(v string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberS{Value: v}
}

func backfillRecord(id string, attrs map[string]dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
	item := map[string]dynamodbtypes.AttributeValue{"id": backfillStr(id), "extra": backfillStr("e-" + id)}
	for name, v := range attrs {
		item[name] = v
	}

	return item
}

// backfillSignatures describes items as "id(attr,attr,...)", sorted: which items an index returns
// and which attributes it shows of them.
func backfillSignatures(items []map[string]dynamodbtypes.AttributeValue) []string {
	out := make([]string, 0, len(items))

	for _, item := range items {
		names := make([]string, 0, len(item))
		for name := range item {
			names = append(names, name)
		}

		sort.Strings(names)

		out = append(out, strings.TrimPrefix(parityAttrValueString(item["id"]), "S:")+"("+strings.Join(names, ",")+")")
	}

	sort.Strings(out)

	return out
}

func backfillScan(ctx context.Context, t *testing.T, client *dynamodb.Client) []string {
	t.Helper()

	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(parityPokemonTable), IndexName: aws.String(parityByTypeIndexName)})
	require.NoError(t, err)

	return backfillSignatures(out.Items)
}

// backfillOutcome puts the items, creates the index by-type on type (S) with the projection, then
// reads the index with a Scan and a Query, runs UpdateItem SET extra on the item of each id in
// updateIDs (an update that does not touch the index key) and deletes the item of each id in
// deleteIDs, reporting the outcome of each and the Scan after them.
func backfillOutcome(t *testing.T, client *dynamodb.Client, projection *dynamodbtypes.Projection, items []map[string]dynamodbtypes.AttributeValue, updateIDs, deleteIDs []string) []string {
	t.Helper()

	ctx := context.Background()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(parityPokemonTable),
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}},
		KeySchema:            []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash}},
	})
	require.NoError(t, err)

	for _, item := range items {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(parityPokemonTable), Item: item})
		require.NoError(t, err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:            aws.String(parityPokemonTable),
		AttributeDefinitions: gsiAttributes(),
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
			IndexName: aws.String(parityByTypeIndexName), KeySchema: gsiKeySchema(), Projection: projection,
		}}},
	})
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, parityPokemonTable, parityByTypeIndexName)

	out := append([]string{"scan"}, backfillScan(ctx, t, client)...)

	queried, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(parityPokemonTable),
		IndexName:                 aws.String(parityByTypeIndexName),
		KeyConditionExpression:    aws.String("#t = :t"),
		ExpressionAttributeNames:  map[string]string{"#t": "type"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":t": backfillStr("a")},
	})
	require.NoError(t, err)

	out = append(out, "query a")
	out = append(out, backfillSignatures(queried.Items)...)

	for _, id := range updateIDs {
		key := map[string]dynamodbtypes.AttributeValue{"id": backfillStr(id)}

		_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(parityPokemonTable),
			Key:                       key,
			UpdateExpression:          aws.String("SET extra = :x"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":x": backfillStr("changed")},
		})
		if err != nil {
			out = append(out, "update "+id+": "+normalizeSDKErrorString(err.Error()))
		} else {
			out = append(out, "update "+id+": OK")
		}

		got, getErr := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(parityPokemonTable), Key: key})
		require.NoError(t, getErr)

		out = append(out, "extra of "+id+": "+parityAttrValueString(got.Item["extra"]))
	}

	for _, id := range deleteIDs {
		_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(parityPokemonTable),
			Key:       map[string]dynamodbtypes.AttributeValue{"id": backfillStr(id)},
		})
		if err != nil {
			out = append(out, "delete "+id+": "+normalizeSDKErrorString(err.Error()))
		} else {
			out = append(out, "delete "+id+": OK")
		}
	}

	return append(append(out, "scan after deletes"), backfillScan(ctx, t, client)...)
}

func backfillBaseItems() []map[string]dynamodbtypes.AttributeValue {
	return []map[string]dynamodbtypes.AttributeValue{
		backfillRecord("a1", map[string]dynamodbtypes.AttributeValue{"type": backfillStr("a")}),
		backfillRecord("a2", map[string]dynamodbtypes.AttributeValue{"type": backfillStr("a")}),
		backfillRecord("b1", map[string]dynamodbtypes.AttributeValue{"type": backfillStr("b")}),
		backfillRecord("sparse", nil),
	}
}

func TestE2E_UpdateTableCreateIndexBackfill(t *testing.T) {
	include := &dynamodbtypes.Projection{
		ProjectionType: dynamodbtypes.ProjectionTypeInclude, NonKeyAttributes: []string{"extra"},
	}
	keysOnly := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeKeysOnly}
	emptyType := backfillRecord("empty", map[string]dynamodbtypes.AttributeValue{"type": backfillStr("")})

	tests := []struct {
		name       string
		projection *dynamodbtypes.Projection
		extra      []map[string]dynamodbtypes.AttributeValue
		updateIDs  []string
		deleteIDs  []string
	}{
		{name: "AllProjection", projection: gsiAll()},
		{name: "KeysOnlyProjection", projection: keysOnly},
		{name: "IncludeProjection", projection: include},
		{name: "EmptyValueIsSkipped", projection: gsiAll(), extra: []map[string]dynamodbtypes.AttributeValue{emptyType}},
		{name: "UpdateOfSkippedItem", projection: gsiAll(), extra: []map[string]dynamodbtypes.AttributeValue{emptyType}, updateIDs: []string{"empty"}},
		{name: "DeleteOfSkippedItem", projection: gsiAll(), extra: []map[string]dynamodbtypes.AttributeValue{emptyType}, deleteIDs: []string{"empty", "sparse", "a1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) []string {
				t.Helper()

				return backfillOutcome(t, client, tt.projection, append(backfillBaseItems(), tt.extra...), tt.updateIDs, tt.deleteIDs)
			})
		})
	}

	t.Run("WrongTypeIsSkippedByMinidynOnly", func(t *testing.T) {
		// Intentional divergence (e2e/README.md, "Intentional divergences"): the developer guide,
		// under "Phases of index creation", says an item whose index key attribute has another
		// data type than the index key schema is not in the index once it is ACTIVE. minidyn
		// leaves it out. DynamoDB Local indexes it.
		const (
			minidyn = "minidyn"
			local   = "dynamodb-local"
		)

		const wrongTypeUpdateErr = "operation error DynamoDB: UpdateItem, https response error StatusCode: 400, RequestID: , " +
			"api error ValidationException: One or more parameter values were invalid: Type mismatch for Index Key"

		wrongType := backfillRecord("wrong", map[string]dynamodbtypes.AttributeValue{"type": &dynamodbtypes.AttributeValueMemberN{Value: "5"}})
		common := []string{
			"scan", "a1(extra,id,type)", "a2(extra,id,type)", "b1(extra,id,type)",
			"query a", "a1(extra,id,type)", "a2(extra,id,type)",
			"update wrong: " + wrongTypeUpdateErr, "extra of wrong: S:e-wrong",
			"delete wrong: OK",
			"scan after deletes", "a1(extra,id,type)", "a2(extra,id,type)", "b1(extra,id,type)",
		}
		withWrong := []string{
			"scan", "a1(extra,id,type)", "a2(extra,id,type)", "b1(extra,id,type)", "wrong(extra,id,type)",
			"query a", "a1(extra,id,type)", "a2(extra,id,type)",
			"update wrong: " + wrongTypeUpdateErr, "extra of wrong: S:e-wrong",
			"delete wrong: OK",
			"scan after deletes", "a1(extra,id,type)", "a2(extra,id,type)", "b1(extra,id,type)",
		}

		want := map[string][]string{minidyn: common, local: withWrong}

		for engine, endpoint := range map[string]func(*testing.T) string{
			minidyn: setupMinidynEndpoint,
			local:   setupDynamoDBLocalEndpoint,
		} {
			t.Run(engine, func(t *testing.T) {
				client := newTracedSDKClient(t, endpoint(t), &traceRecorder{})

				got := backfillOutcome(t, client, gsiAll(), append(backfillBaseItems(), wrongType), []string{"wrong"}, []string{"wrong"})

				// Both engines reject the update, with the same text, and leave the item as it was.
				require.Equal(t, want[engine], got)
			})
		}
	})
}
