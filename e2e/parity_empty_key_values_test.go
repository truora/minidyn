package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Empty S and B values bound to key attributes: DynamoDB rejects them in key conditions,
// item keys, start keys and the key attributes of an index. They are fine elsewhere (a
// FilterExpression, a non-key attribute).
//
// Tables: "ekeys" has pk S / sk S, GSI by-g on g S / gs S and LSI by-l on pk / l S; "ekbin" has
// pk B / sk B and GSI by-b on gb B / gbs B.

const (
	emptyKeyTable    = "ekeys"
	emptyKeyBinTable = "ekbin"
)

type ekItem = map[string]dynamodbtypes.AttributeValue

func ekS(v string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberS{Value: v}
}

func ekB(v ...byte) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberB{Value: append([]byte{}, v...)}
}

func ekAttr(name string, typ dynamodbtypes.ScalarAttributeType) dynamodbtypes.AttributeDefinition {
	return dynamodbtypes.AttributeDefinition{AttributeName: aws.String(name), AttributeType: typ}
}

func ekKey(name string, kt dynamodbtypes.KeyType) dynamodbtypes.KeySchemaElement {
	return dynamodbtypes.KeySchemaElement{AttributeName: aws.String(name), KeyType: kt}
}

func ekCreateTables(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	hash, rng := dynamodbtypes.KeyTypeHash, dynamodbtypes.KeyTypeRange
	str, bin := dynamodbtypes.ScalarAttributeTypeS, dynamodbtypes.ScalarAttributeTypeB
	all := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(emptyKeyTable),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema:   []dynamodbtypes.KeySchemaElement{ekKey("pk", hash), ekKey("sk", rng)},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			ekAttr("pk", str), ekAttr("sk", str), ekAttr("g", str), ekAttr("gs", str), ekAttr("l", str),
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-g"),
			KeySchema:  []dynamodbtypes.KeySchemaElement{ekKey("g", hash), ekKey("gs", rng)},
			Projection: all,
		}},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{{
			IndexName:  aws.String("by-l"),
			KeySchema:  []dynamodbtypes.KeySchemaElement{ekKey("pk", hash), ekKey("l", rng)},
			Projection: all,
		}},
	})
	require.NoError(t, err)
	parityWaitUntilGSIActive(ctx, t, client, emptyKeyTable, "by-g")

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(emptyKeyBinTable),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema:   []dynamodbtypes.KeySchemaElement{ekKey("pk", hash), ekKey("sk", rng)},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			ekAttr("pk", bin), ekAttr("sk", bin), ekAttr("gb", bin), ekAttr("gbs", bin),
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-b"),
			KeySchema:  []dynamodbtypes.KeySchemaElement{ekKey("gb", hash), ekKey("gbs", rng)},
			Projection: all,
		}},
	})
	require.NoError(t, err)
	parityWaitUntilGSIActive(ctx, t, client, emptyKeyBinTable, "by-b")

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(emptyKeyTable), Item: ekItem{
		"pk": ekS("a"), "sk": ekS("1"), "g": ekS("g1"), "gs": ekS("x"), "l": ekS("y"), "n": ekS(""),
	}})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(emptyKeyBinTable), Item: ekItem{
		"pk": ekB(1), "sk": ekB(1), "gb": ekB(2), "gbs": ekB(3),
	}})
	require.NoError(t, err)
}

func ekQuery(index, table, cond string, vals ekItem, filter string) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()
		ctx := context.Background()

		ekCreateTables(ctx, t, client)

		in := &dynamodb.QueryInput{
			TableName:                 aws.String(table),
			KeyConditionExpression:    aws.String(cond),
			ExpressionAttributeValues: vals,
		}
		if index != "" {
			in.IndexName = aws.String(index)
		}

		if filter != "" {
			in.FilterExpression = aws.String(filter)
		}

		if strings.Contains(cond, "#k") {
			in.ExpressionAttributeNames = map[string]string{"#k": "pk", "#r": "sk"}
		}

		out, err := client.Query(ctx, in)
		if err != nil {
			return fuzzErrString(err)
		}

		return len(out.Items)
	}
}

func ekRun(fn func(ctx context.Context, client *dynamodb.Client) error) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()
		ctx := context.Background()

		ekCreateTables(ctx, t, client)

		return fuzzErrString(fn(ctx, client))
	}
}

func TestE2E_EmptyKeyValues_Query(t *testing.T) {
	e, eb := ekS(""), ekB()

	tests := []struct {
		name       string
		knownIssue string
		fn         func(t *testing.T, client *dynamodb.Client) any
	}{
		{name: "TableHashEmptyS", fn: ekQuery("", emptyKeyTable, "pk = :p", ekItem{":p": e}, "")},
		{name: "TableHashEmptyB", fn: ekQuery("", emptyKeyBinTable, "pk = :p", ekItem{":p": eb}, "")},
		{name: "TableRangeEqualsEmptyS", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk = :s", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "TableRangeLessEmptyS", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk < :s", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "TableRangeGreaterEqualEmptyS", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk >= :s", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "TableRangeGreaterEmptyB", fn: ekQuery("", emptyKeyBinTable, "pk = :p AND sk > :s", ekItem{":p": ekB(1), ":s": eb}, "")},
		{name: "TableBetweenEmptyLow", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk BETWEEN :a AND :b", ekItem{":p": ekS("a"), ":a": e, ":b": ekS("z")}, "")},
		{name: "TableBetweenEmptyHigh", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk BETWEEN :a AND :b", ekItem{":p": ekS("a"), ":a": ekS("0"), ":b": e}, "")},
		{name: "TableBetweenEmptyBothB", fn: ekQuery("", emptyKeyBinTable, "pk = :p AND sk BETWEEN :a AND :b", ekItem{":p": ekB(1), ":a": eb, ":b": eb}, "")},
		{name: "TableBeginsWithEmptyS", fn: ekQuery("", emptyKeyTable, "pk = :p AND begins_with(sk, :s)", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "TableBeginsWithEmptyB", fn: ekQuery("", emptyKeyBinTable, "pk = :p AND begins_with(sk, :s)", ekItem{":p": ekB(1), ":s": eb}, "")},
		{name: "TableEmptyHashAndRange", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk = :s", ekItem{":p": e, ":s": e}, "")},
		{name: "TableAliasedEmpty", fn: ekQuery("", emptyKeyTable, "#k = :p AND begins_with(#r, :s)", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "TableReversedOperandsEmpty", fn: ekQuery("", emptyKeyTable, ":p = pk AND :s < sk", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "GSIHashEmptyS", fn: ekQuery("by-g", emptyKeyTable, "g = :g", ekItem{":g": e}, "")},
		{name: "GSIHashEmptyB", fn: ekQuery("by-b", emptyKeyBinTable, "gb = :g", ekItem{":g": eb}, "")},
		{name: "GSIRangeEqualsEmptyS", fn: ekQuery("by-g", emptyKeyTable, "g = :g AND gs = :s", ekItem{":g": ekS("g1"), ":s": e}, "")},
		{name: "GSIRangeLessEmptyS", fn: ekQuery("by-g", emptyKeyTable, "g = :g AND gs < :s", ekItem{":g": ekS("g1"), ":s": e}, "")},
		{name: "GSIBetweenEmptyHigh", fn: ekQuery("by-g", emptyKeyTable, "g = :g AND gs BETWEEN :a AND :b", ekItem{":g": ekS("g1"), ":a": ekS("0"), ":b": e}, "")},
		{name: "GSIBeginsWithEmptyS", fn: ekQuery("by-g", emptyKeyTable, "g = :g AND begins_with(gs, :s)", ekItem{":g": ekS("g1"), ":s": e}, "")},
		{name: "GSIBeginsWithEmptyB", fn: ekQuery("by-b", emptyKeyBinTable, "gb = :g AND begins_with(gbs, :s)", ekItem{":g": ekB(2), ":s": eb}, "")},
		{name: "LSIHashEmptyS", fn: ekQuery("by-l", emptyKeyTable, "pk = :p", ekItem{":p": e}, "")},
		{name: "LSIRangeGreaterEmptyS", fn: ekQuery("by-l", emptyKeyTable, "pk = :p AND l > :s", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "LSIBetweenEmptyLow", fn: ekQuery("by-l", emptyKeyTable, "pk = :p AND l BETWEEN :a AND :b", ekItem{":p": ekS("a"), ":a": e, ":b": ekS("z")}, "")},
		{name: "LSIBeginsWithEmptyS", fn: ekQuery("by-l", emptyKeyTable, "pk = :p AND begins_with(l, :s)", ekItem{":p": ekS("a"), ":s": e}, "")},
		{name: "FilterEmptyString", fn: ekQuery("", emptyKeyTable, "pk = :p", ekItem{":p": ekS("a"), ":f": e}, "n = :f")},
		{name: "FilterBeginsWithEmptyString", fn: ekQuery("", emptyKeyTable, "pk = :p", ekItem{":p": ekS("a"), ":f": e}, "begins_with(n, :f)")},
		{name: "TableBetweenReversedS", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk BETWEEN :a AND :b", ekItem{":p": ekS("a"), ":a": ekS("z"), ":b": ekS("a")}, "")},
		{name: "TableBetweenEmptyBoth", fn: ekQuery("", emptyKeyTable, "pk = :p AND sk BETWEEN :a AND :b", ekItem{":p": ekS("a"), ":a": e, ":b": e}, "")},
		{name: "TableBetweenEmptyLowB", fn: ekQuery("", emptyKeyBinTable, "pk = :p AND sk BETWEEN :a AND :b", ekItem{":p": ekB(1), ":a": eb, ":b": ekB(1)}, "")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { RunE2EKnown(t, tt.knownIssue, tt.fn) })
	}
}

func TestE2E_EmptyKeyValues_Operations(t *testing.T) {
	tbl := aws.String(emptyKeyTable)
	e := ekS("")
	okKey := ekItem{"pk": ekS("a"), "sk": ekS("1")}
	emptySK := ekItem{"pk": ekS("a"), "sk": e}
	emptyPK := ekItem{"pk": e, "sk": ekS("1")}
	withGSI := func(extra ekItem) ekItem {
		item := ekItem{"pk": ekS("a"), "sk": ekS("2")}
		for k, v := range extra {
			item[k] = v
		}

		return item
	}

	update := func(key ekItem, expr string, v dynamodbtypes.AttributeValue) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: tbl, Key: key, UpdateExpression: aws.String(expr), ExpressionAttributeValues: ekItem{":v": v},
			})

			return err
		}
	}

	put := func(table string, item ekItem) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: item})

			return err
		}
	}

	get := func(table string, key ekItem) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(table), Key: key})

			return err
		}
	}

	batchPut := func(item ekItem) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				emptyKeyTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: item}}},
			}})

			return err
		}
	}

	transact := func(item dynamodbtypes.TransactWriteItem) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{item}})

			return err
		}
	}

	query := func(index string, vals ekItem, start ekItem) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			in := &dynamodb.QueryInput{
				TableName: tbl, KeyConditionExpression: aws.String("pk = :p"), ExpressionAttributeValues: vals, ExclusiveStartKey: start,
			}
			if index != "" {
				in.IndexName = aws.String(index)
				in.KeyConditionExpression = aws.String("g = :p")
			}

			_, err := c.Query(ctx, in)

			return err
		}
	}

	tests := []struct {
		name       string
		knownIssue string
		fn         func(ctx context.Context, c *dynamodb.Client) error
	}{
		{name: "GetItemEmptyRangeKey", fn: get(emptyKeyTable, emptySK)},
		{name: "GetItemEmptyHashKey", fn: get(emptyKeyTable, emptyPK)},
		{name: "GetItemEmptyBinaryKey", fn: get(emptyKeyBinTable, ekItem{"pk": ekB(1), "sk": ekB()})},
		{name: "PutItemEmptyRangeKey", fn: put(emptyKeyTable, emptySK)},
		{name: "PutItemEmptyHashKey", fn: put(emptyKeyTable, emptyPK)},
		{name: "PutItemEmptyBinaryKey", fn: put(emptyKeyBinTable, ekItem{"pk": ekB(1), "sk": ekB()})},
		{name: "PutItemEmptyGSIHashKey", fn: put(emptyKeyTable, withGSI(ekItem{"g": e}))},
		{name: "PutItemEmptyGSIRangeKey", fn: put(emptyKeyTable, withGSI(ekItem{"g": ekS("g1"), "gs": e}))},
		{name: "PutItemEmptyGSIBinaryKey", fn: put(emptyKeyBinTable, ekItem{"pk": ekB(1), "sk": ekB(2), "gb": ekB()})},
		{name: "PutItemEmptyLSIKey", fn: put(emptyKeyTable, withGSI(ekItem{"l": e}))},
		{name: "PutItemEmptyNonKeyAttribute", fn: put(emptyKeyTable, withGSI(ekItem{"n": e}))},
		{name: "UpdateItemEmptyKey", fn: update(emptySK, "SET n = :v", ekS("x"))},
		{name: "UpdateItemSetEmptyGSIKey", fn: update(okKey, "SET g = :v", e)},
		{name: "UpdateItemSetEmptyGSIRangeKey", fn: update(okKey, "SET gs = :v", e)},
		{name: "UpdateItemSetEmptyLSIKey", fn: update(okKey, "SET l = :v", e)},
		{name: "UpdateItemSetEmptyNonKeyAttribute", fn: update(okKey, "SET n = :v", e)},
		{name: "DeleteItemEmptyKey", fn: func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: tbl, Key: emptySK})

			return err
		}},
		{name: "BatchGetItemEmptyKey", fn: func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				emptyKeyTable: {Keys: []ekItem{okKey, emptySK}},
			}})

			return err
		}},
		{name: "BatchWritePutEmptyKey", fn: batchPut(emptySK)},
		{name: "BatchWritePutEmptyGSIKey", fn: batchPut(withGSI(ekItem{"g": e}))},
		{name: "BatchWriteDeleteEmptyKey", fn: func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				emptyKeyTable: {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: emptySK}}},
			}})

			return err
		}},
		{name: "TransactWritePutEmptyKey", fn: transact(dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{TableName: tbl, Item: emptySK}})},
		{name: "TransactWritePutEmptyGSIKey", fn: transact(dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{TableName: tbl, Item: withGSI(ekItem{"g": e})}})},
		{name: "TransactWriteUpdateEmptyKey", fn: transact(dynamodbtypes.TransactWriteItem{Update: &dynamodbtypes.Update{
			TableName: tbl, Key: emptySK, UpdateExpression: aws.String("SET n = :v"), ExpressionAttributeValues: ekItem{":v": ekS("x")},
		}})},
		{name: "TransactWriteDeleteEmptyKey", fn: transact(dynamodbtypes.TransactWriteItem{Delete: &dynamodbtypes.Delete{TableName: tbl, Key: emptySK}})},
		{name: "TransactWriteConditionCheckEmptyKey", fn: transact(dynamodbtypes.TransactWriteItem{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: tbl, Key: emptySK, ConditionExpression: aws.String("attribute_exists(pk)"),
		}})},
		{name: "TransactGetEmptyKey", fn: func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{{
				Get: &dynamodbtypes.Get{TableName: tbl, Key: emptySK},
			}}})

			return err
		}},
		{name: "QueryExclusiveStartKeyEmptyRangeKey", knownIssue: "#171", fn: query("", ekItem{":p": ekS("a")}, emptySK)},
		{name: "QueryGSIExclusiveStartKeyEmptyIndexKey", knownIssue: "#171", fn: query("by-g", ekItem{":p": ekS("g1")},
			ekItem{"pk": ekS("a"), "sk": ekS("1"), "g": ekS("g1"), "gs": e})},
		{name: "ScanExclusiveStartKeyEmptyRangeKey", knownIssue: "#171", fn: func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.Scan(ctx, &dynamodb.ScanInput{TableName: tbl, ExclusiveStartKey: emptySK})

			return err
		}},
		{name: "ScanFilterEmptyString", fn: func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.Scan(ctx, &dynamodb.ScanInput{
				TableName: tbl, FilterExpression: aws.String("n = :f"), ExpressionAttributeValues: ekItem{":f": e},
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { RunE2EKnown(t, tt.knownIssue, ekRun(tt.fn)) })
	}
}
