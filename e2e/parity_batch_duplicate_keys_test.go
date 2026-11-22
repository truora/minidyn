package e2e

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Batches name a key twice (#235, #246). DynamoDB Local rejects it with "Provided list of item
// keys contains duplicates", at a point that differs between the two operations:
//
//   - BatchWriteItem validates each request and then compares its key with the earlier ones, so
//     an invalid request, in request order, comes before a duplicate that follows it and after
//     one that precedes it. Keys are compared as the table stores them: only the key attributes
//     count, and 1 and 1.0 are the same key.
//   - BatchGetItem compares the raw key maps of all its tables before it validates anything: a
//     duplicate wins over an empty, mistyped or incomplete key, a bad projection, an unknown
//     table, and the 100 key limit when it comes first. 1 and 1.0 are different keys, and so
//     are keys with different extra attributes.
//
// Tables: "ekeys" (pk S / sk S, with an index each of GSI and LSI), "ekeys2" (pk S / sk S),
// "ekn" (pk S / sk N) and "ekbin".

func bdCreateSecondTable(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("ekeys2"),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema: []dynamodbtypes.KeySchemaElement{
			ekKey("pk", dynamodbtypes.KeyTypeHash), ekKey("sk", dynamodbtypes.KeyTypeRange),
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			ekAttr("pk", dynamodbtypes.ScalarAttributeTypeS), ekAttr("sk", dynamodbtypes.ScalarAttributeTypeS),
		},
	})
	require.NoError(t, err)
}

func bdKey(pk, sk string, extra ...any) ekItem {
	key := ekItem{"pk": ekS(pk), "sk": ekS(sk)}
	for i := 0; i+1 < len(extra); i += 2 {
		key[extra[i].(string)] = extra[i+1].(dynamodbtypes.AttributeValue)
	}

	return key
}

func bdNKey(sk string) ekItem { return ekItem{"pk": ekS("a"), "sk": voN(sk)} }

func TestE2E_BatchWriteDuplicateKeys(t *testing.T) {
	const second = "ekeys2"

	put := func(item ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{Item: item}}
	}
	del := func(key ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key}}
	}
	one := func(reqs ...dynamodbtypes.WriteRequest) map[string][]dynamodbtypes.WriteRequest {
		return map[string][]dynamodbtypes.WriteRequest{emptyKeyTable: reqs}
	}
	nth := func(count int) []dynamodbtypes.WriteRequest {
		reqs := make([]dynamodbtypes.WriteRequest, count)
		for i := range reqs {
			reqs[i] = put(bdKey("n", strconv.Itoa(i)))
		}

		return reqs
	}
	e := ekS("")
	type batch = map[string][]dynamodbtypes.WriteRequest

	// Local goes through the tables in the order of the request, which a Go map cannot carry:
	// when the first problem is in a different table for either order, the cases compare only
	// that the batch was rejected.
	anyError := map[string]bool{
		"DuplicateAndInvalidInTwoTables": true, "InvalidAndDuplicateInTwoTables": true,
		"DuplicateAndMissingRangeKeyInTwoTables": true,
	}

	tests := []struct {
		name  string
		items batch
	}{
		{"PutPut", one(put(bdKey("b", "1")), put(bdKey("b", "1")))},
		{"PutDelete", one(put(bdKey("b", "1")), del(bdKey("b", "1")))},
		{"DeletePut", one(del(bdKey("b", "1")), put(bdKey("b", "1")))},
		{"DeleteDelete", one(del(bdKey("b", "1")), del(bdKey("b", "1")))},
		{"DuplicateOfStoredItem", one(put(bdKey("a", "1")), del(bdKey("a", "1")))},
		{"NotAdjacent", one(put(bdKey("b", "1")), put(bdKey("b", "2")), put(bdKey("b", "3")), put(bdKey("b", "1")))},
		{"DifferentKeys", one(put(bdKey("b", "1")), put(bdKey("b", "2")), del(bdKey("c", "1")))},
		{"NonKeyAttributesDiffer", one(put(bdKey("b", "1", "x", ekS("1"))), put(bdKey("b", "1", "x", ekS("2"))))},
		{"PutWithAttributesAndDelete", one(put(bdKey("b", "1", "x", ekS("1"))), del(bdKey("b", "1")))},
		{"NonKeyEmptyStringOnBoth", one(put(bdKey("b", "1", "n", e)), put(bdKey("b", "1", "n", e)))},
		{"SameKeyInTwoTables", batch{emptyKeyTable: {put(bdKey("b", "1"))}, second: {put(bdKey("b", "1"))}}},
		{"NumbersSpelledDifferently", batch{"ekn": {put(bdNKey("1")), put(bdNKey("1.0"))}}},
		{"NumbersWithLeadingZero", batch{"ekn": {put(bdNKey("1")), put(bdNKey("01"))}}},
		{"NumbersSpelledTheSame", batch{"ekn": {put(bdNKey("1")), put(bdNKey("1"))}}},
		{"DifferentNumbers", batch{"ekn": {put(bdNKey("1")), put(bdNKey("2"))}}},

		// the order against the other checks of the batch
		{"NoTables", batch{}},
		{"EmptyRequestList", one()},
		{"EmptyRequestListAndDuplicateInOtherTable", batch{emptyKeyTable: {}, second: {put(bdKey("b", "1")), put(bdKey("b", "1"))}}},
		{"DuplicateAndUnknownTable", batch{emptyKeyTable: {put(bdKey("b", "1")), put(bdKey("b", "1"))}, "zzz": {put(bdKey("b", "1"))}}},
		{"DuplicateInUnknownTable", batch{"zzz": {put(bdKey("b", "1")), put(bdKey("b", "1"))}}},
		{"DuplicateThenNoRequestSet", one(put(bdKey("b", "1")), put(bdKey("b", "1")), dynamodbtypes.WriteRequest{})},
		{"NoRequestThenDuplicate", one(dynamodbtypes.WriteRequest{}, put(bdKey("b", "1")), put(bdKey("b", "1")))},
		{"DuplicateAt25Requests", one(append(nth(23), put(bdKey("b", "1")), put(bdKey("b", "1")))...)},
		{"DuplicateAt26Requests", one(append(nth(24), put(bdKey("b", "1")), put(bdKey("b", "1")))...)},
		{"DuplicateAt26RequestsOfTwoTables", batch{emptyKeyTable: nth(13), second: append(nth(11), put(bdKey("b", "1")), put(bdKey("b", "1")))}},
		{"EmptyKeyValueOnBothPuts", one(put(bdKey("", "1")), put(bdKey("", "1")))},
		{"EmptyKeyValueOnBothDeletes", one(del(bdKey("b", "")), del(bdKey("b", "")))},
		{"EmptyKeyValueThenDuplicate", one(put(bdKey("", "9")), put(bdKey("b", "1")), put(bdKey("b", "1")))},
		{"DuplicateThenEmptyKeyValue", one(put(bdKey("b", "1")), put(bdKey("b", "1")), put(bdKey("", "9")))},
		{"EmptyKeyValuesAreNotDuplicates", one(put(bdKey("", "1")), put(bdKey("", "2")))},
		{"WrongKeyTypeOnBothPuts", one(
			put(ekItem{"pk": voN("1"), "sk": ekS("1")}), put(ekItem{"pk": voN("1"), "sk": ekS("1")}))},
		{"MissingRangeKeyOnBothPuts", one(put(ekItem{"pk": ekS("b")}), put(ekItem{"pk": ekS("b")}))},
		{"ExtraAttributeOnBothDeletes", one(del(bdKey("b", "1", "x", ekS("1"))), del(bdKey("b", "1", "x", ekS("1"))))},
		{"PutThenDeleteWithExtraAttribute", one(put(bdKey("b", "1")), del(bdKey("b", "1", "x", ekS("1"))))},
		{"EmptyIndexKeyOnBothPuts", one(put(bdKey("b", "1", "g", e)), put(bdKey("b", "1", "g", e)))},
		{"PutThenDuplicateWithEmptyIndexKey", one(put(bdKey("b", "1")), put(bdKey("b", "1", "g", e)))},
		{"DuplicateThenEmptyIndexKey", one(put(bdKey("b", "1")), put(bdKey("b", "1")), put(bdKey("b", "2", "g", e)))},
		{"EmptyIndexKeyThenDuplicate", one(put(bdKey("b", "2", "g", e)), put(bdKey("b", "1")), put(bdKey("b", "1")))},
		{"DuplicateAndInvalidInTwoTables", batch{emptyKeyTable: {put(bdKey("b", "1")), put(bdKey("b", "1"))}, second: {put(bdKey("", "1"))}}},
		{"InvalidAndDuplicateInTwoTables", batch{emptyKeyTable: {put(bdKey("", "1"))}, second: {put(bdKey("b", "1")), put(bdKey("b", "1"))}}},
		{"DuplicateAndMissingRangeKeyInTwoTables", batch{
			emptyKeyTable: {put(bdKey("b", "1")), put(bdKey("b", "1"))}, second: {put(ekItem{"pk": ekS("b")})},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, voRun(func(ctx context.Context, client *dynamodb.Client) string {
				bdCreateSecondTable(ctx, t, client)

				_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: tt.items})

				errText := fuzzErrString(err)
				if anyError[tt.name] {
					errText = "rejected: " + strconv.FormatBool(err != nil)
				}

				return fmt.Sprintf("%s ekeys=%v ekeys2=%v ekn=%v", errText,
					bwScan(ctx, t, client, emptyKeyTable, ""), bwScan(ctx, t, client, second, ""), bwScan(ctx, t, client, "ekn", ""))
			}))
		})
	}
}

func TestE2E_BatchGetDuplicateKeys(t *testing.T) {
	const second = "ekeys2"

	manyKeys := func(count int) []ekItem {
		keys := make([]ekItem, count)
		for i := range keys {
			keys[i] = bdKey("n", strconv.Itoa(i))
		}

		return keys
	}
	keys := func(proj string, keys ...ekItem) dynamodbtypes.KeysAndAttributes {
		in := dynamodbtypes.KeysAndAttributes{Keys: append([]ekItem{}, keys...)}
		if proj != "" {
			in.ProjectionExpression = aws.String(proj)
		}

		return in
	}
	type batch = map[string]dynamodbtypes.KeysAndAttributes

	// As for BatchWriteItem, Local goes through the tables in the order of the request, so these
	// cases compare only that the batch was rejected.
	anyError := map[string]bool{
		"EmptyKeyListAndDuplicateInOtherTable": true, "EmptyKeyListAndMoreThan100KeysInOtherTable": true,
		"DuplicateInSecondTableAfter100Keys": true,
		// two tables fail with different errors: the SDK marshals RequestItems from a map, so
		// Local sees a random table order, while minidyn goes by table name (D1 level 3)
		"InvalidKeyAndUndeclaredNameInTwoTables": true, "UnknownTableAndUndeclaredName": true,
	}

	tests := []struct {
		name  string
		items batch
	}{
		{"Duplicate", batch{emptyKeyTable: keys("", bdKey("a", "1"), bdKey("a", "1"))}},
		{"NotAdjacent", batch{emptyKeyTable: keys("", bdKey("a", "1"), bdKey("b", "1"), bdKey("a", "1"))}},
		{"DifferentKeys", batch{emptyKeyTable: keys("", bdKey("a", "1"), bdKey("a", "2"))}},
		{"SameKeyInTwoTables", batch{emptyKeyTable: keys("", bdKey("a", "1")), second: keys("", bdKey("a", "1"))}},
		{"DuplicateWithExtraAttributes", batch{emptyKeyTable: keys("", bdKey("a", "1", "x", ekS("1")), bdKey("a", "1", "x", ekS("1")))}},
		// the raw key maps are compared: other attributes and other spellings make other keys
		{"ExtraAttributesDiffer", batch{emptyKeyTable: keys("", bdKey("a", "1", "x", ekS("1")), bdKey("a", "1", "x", ekS("2")))}},
		{"KeyAndKeyWithExtraAttribute", batch{emptyKeyTable: keys("", bdKey("a", "1"), bdKey("a", "1", "x", ekS("2")))}},
		{"NumbersSpelledDifferently", batch{"ekn": keys("", bdNKey("1"), bdNKey("1.0"))}},
		{"NumbersWithLeadingZero", batch{"ekn": keys("", bdNKey("1"), bdNKey("01"))}},
		{"NumbersSpelledTheSame", batch{"ekn": keys("", bdNKey("1"), bdNKey("1"))}},

		// the order against the other checks of the batch
		{"NoTables", batch{}},
		{"EmptyKeyList", batch{emptyKeyTable: keys("")}},
		{"EmptyKeyListOfUnknownTable", batch{"zzz": keys("")}},
		{"EmptyKeyListAndProjection", batch{emptyKeyTable: keys("#x")}},
		{"EmptyKeyListAndUnknownTable", batch{emptyKeyTable: keys(""), "zzz": keys("", bdKey("a", "1"))}},
		{"EmptyKeyListAndInvalidKeyInOtherTable", batch{emptyKeyTable: keys(""), second: keys("", bdKey("", "1"))}},
		{"EmptyKeyListAndValidOtherTable", batch{emptyKeyTable: keys(""), second: keys("", bdKey("a", "1"))}},
		{"EmptyKeyListAndMoreThan100KeysInOtherTable", batch{emptyKeyTable: keys(""), second: keys("", manyKeys(101)...)}},
		{"EmptyKeyListAndDuplicateInOtherTable", batch{emptyKeyTable: keys(""), second: keys("", bdKey("a", "1"), bdKey("a", "1"))}},
		{"DuplicateEmptyKeyValue", batch{emptyKeyTable: keys("", bdKey("a", ""), bdKey("a", ""))}},
		{"DuplicateEmptyHashKeyValue", batch{emptyKeyTable: keys("", bdKey("", "1"), bdKey("", "1"))}},
		{"EmptyKeyValuesAreNotDuplicates", batch{emptyKeyTable: keys("", bdKey("a", ""), bdKey("a", "x"))}},
		{"DuplicateWrongKeyType", batch{emptyKeyTable: keys("", ekItem{"pk": voN("1"), "sk": ekS("1")}, ekItem{"pk": voN("1"), "sk": ekS("1")})}},
		{"DuplicateMissingRangeKey", batch{emptyKeyTable: keys("", ekItem{"pk": ekS("a")}, ekItem{"pk": ekS("a")})}},
		{"DuplicateAndUndeclaredName", batch{emptyKeyTable: keys("#x", bdKey("a", "1"), bdKey("a", "1"))}},
		{"DuplicateAndSyntaxError", batch{emptyKeyTable: keys("g,,", bdKey("a", "1"), bdKey("a", "1"))}},
		{"DuplicateAndUnknownTable", batch{emptyKeyTable: keys("", bdKey("a", "1"), bdKey("a", "1")), "zzz": keys("", bdKey("a", "1"))}},
		{"DuplicateInUnknownTable", batch{"zzz": keys("", bdKey("a", "1"), bdKey("a", "1"))}},
		{"InvalidKeyInFirstTableAndDuplicateInSecond", batch{emptyKeyTable: keys("", bdKey("", "1")), second: keys("", bdKey("a", "1"), bdKey("a", "1"))}},
		{"UndeclaredNameInFirstTableAndDuplicateInSecond", batch{emptyKeyTable: keys("#x", bdKey("a", "1")), second: keys("", bdKey("a", "1"), bdKey("a", "1"))}},
		{"DuplicateInFirstTableAndUndeclaredNameInSecond", batch{emptyKeyTable: keys("", bdKey("a", "1"), bdKey("a", "1")), second: keys("#x", bdKey("a", "1"))}},
		{"InvalidKeyAndUndeclaredNameInTwoTables", batch{emptyKeyTable: keys("", bdKey("", "1")), second: keys("#x", bdKey("a", "1"))}},
		{"UnknownTableAndUndeclaredName", batch{"zzz": keys("", bdKey("a", "1")), emptyKeyTable: keys("#x", bdKey("a", "1"))}},
		{"100Keys", batch{emptyKeyTable: keys("", manyKeys(100)...)}},
		{"101Keys", batch{emptyKeyTable: keys("", manyKeys(101)...)}},
		{"101KeysOfTwoTables", batch{emptyKeyTable: keys("", manyKeys(60)...), second: keys("", manyKeys(41)...)}},
		{"101KeysAndUndeclaredName", batch{emptyKeyTable: keys("#x", manyKeys(101)...)}},
		{"101KeysAndEmptyKeyValue", batch{emptyKeyTable: keys("", append(manyKeys(100), bdKey("a", ""))...)}},
		{"DuplicateAs101stKey", batch{emptyKeyTable: keys("", append(manyKeys(100), bdKey("n", "1"))...)}},
		{"DuplicateInSecondTableAfter100Keys", batch{emptyKeyTable: keys("", manyKeys(100)...), second: keys("", bdKey("a", "1"), bdKey("a", "1"))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, voRun(func(ctx context.Context, client *dynamodb.Client) string {
				bdCreateSecondTable(ctx, t, client)

				out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: tt.items})
				if err != nil {
					if anyError[tt.name] {
						return "rejected"
					}

					return fuzzErrString(err)
				}

				return fmt.Sprintf("ok items=%d,%d", len(out.Responses[emptyKeyTable]), len(out.Responses[second]))
			}))
		})
	}
}
