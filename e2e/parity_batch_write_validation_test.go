package e2e

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// bwScan returns every item of table (or of its index), as sorted text.
func bwScan(ctx context.Context, t *testing.T, client *dynamodb.Client, table, index string) []string {
	t.Helper()

	in := &dynamodb.ScanInput{TableName: aws.String(table)}
	if index != "" {
		in.IndexName = aws.String(index)
	}

	out, err := client.Scan(ctx, in)
	require.NoError(t, err)

	items := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		items = append(items, fuzzAttrJSON(&dynamodbtypes.AttributeValueMemberM{Value: item}))
	}

	sort.Strings(items)

	return items
}

// TestE2E_BatchWriteValidatesBeforeWriting pins that BatchWriteItem validates every request
// before it applies any (#216): a batch with an invalid request is rejected as a whole, so the
// valid requests around it are not written, in the table or in its indexes.
func TestE2E_BatchWriteValidatesBeforeWriting(t *testing.T) {
	e := ekS("")
	valid := ekItem{"pk": ekS("v"), "sk": ekS("1"), "g": ekS("gv"), "gs": ekS("x")}
	validKey := ekItem{"pk": ekS("v"), "sk": ekS("2")}
	put := func(item ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{Item: item}}
	}
	del := func(key ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key}}
	}
	num := &dynamodbtypes.AttributeValueMemberN{Value: "1"}
	one := func(reqs ...dynamodbtypes.WriteRequest) map[string][]dynamodbtypes.WriteRequest {
		return map[string][]dynamodbtypes.WriteRequest{emptyKeyTable: reqs}
	}

	// These cases compare only that the batch was rejected: with a missing table and an invalid
	// request, DynamoDB Local reports either one first depending on the wire order and the table
	// schema, so which one wins is not a stable parity target (see core.ValidateBatchWrite).
	anyError := map[string]bool{"InvalidItemAndUnknownTable": true}

	tests := []struct {
		name  string
		items map[string][]dynamodbtypes.WriteRequest
	}{
		{"PutEmptyHashKey", one(put(valid), put(ekItem{"pk": e, "sk": ekS("1")}))},
		{"PutEmptyRangeKey", one(put(valid), put(ekItem{"pk": ekS("v"), "sk": e}))},
		{"PutWrongKeyType", one(put(valid), put(ekItem{"pk": num, "sk": ekS("1")}))},
		{"PutMissingRangeKey", one(put(valid), put(ekItem{"pk": ekS("v")}))},
		{"PutEmptyGSIKey", one(put(valid), put(ekItem{"pk": ekS("v"), "sk": ekS("2"), "g": e}))},
		{"PutEmptyLSIKey", one(put(valid), put(ekItem{"pk": ekS("v"), "sk": ekS("2"), "l": e}))},
		{"DeleteEmptyKey", one(put(valid), del(ekItem{"pk": ekS("a"), "sk": e}))},
		{"DeleteWrongKeyType", one(put(valid), del(ekItem{"pk": num, "sk": ekS("1")}))},
		{"DeleteMissingRangeKey", one(put(valid), del(ekItem{"pk": ekS("a")}))},
		{"DeleteExtraAttribute", one(put(valid), del(ekItem{"pk": ekS("a"), "sk": ekS("1"), "g": ekS("g1")}))},
		{"DeleteOfExistingItemThenInvalid", one(del(ekItem{"pk": ekS("a"), "sk": ekS("1")}), put(ekItem{"pk": e, "sk": ekS("1")}))},
		{"InvalidThenValid", one(put(ekItem{"pk": e, "sk": ekS("1")}), put(valid))},
		{"TwoInvalid", one(put(valid), put(ekItem{"pk": ekS("v"), "sk": ekS("2"), "g": e}), put(ekItem{"pk": num, "sk": ekS("1")}))},
		{"TwoInvalidKeyFirst", one(put(valid), put(ekItem{"pk": num, "sk": ekS("1")}), put(ekItem{"pk": ekS("v"), "sk": ekS("2"), "g": e}))},
		{"ValidDeleteThenInvalidPut", one(del(validKey), put(valid), put(ekItem{"pk": e, "sk": ekS("1")}))},
		{"InvalidInSecondTable", map[string][]dynamodbtypes.WriteRequest{
			emptyKeyTable:    {put(valid)},
			emptyKeyBinTable: {put(ekItem{"pk": ekB(2), "sk": ekB()})},
		}},
		// DynamoDB validates the existing tables first and reports a missing one last
		{"InvalidItemAndUnknownTable", map[string][]dynamodbtypes.WriteRequest{
			"aaa":         {put(valid)},
			emptyKeyTable: {put(ekItem{"pk": e, "sk": ekS("1")})},
		}},
		{"UnknownTable", map[string][]dynamodbtypes.WriteRequest{
			emptyKeyTable: {put(valid)},
			"no-such":     {put(valid)},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				ekCreateTables(ctx, t, client)

				_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: tt.items})

				errText := fuzzErrString(err)
				if anyError[tt.name] {
					errText = "rejected: " + strconv.FormatBool(err != nil)
				}

				return map[string]any{
					"error": errText,
					"table": bwScan(ctx, t, client, emptyKeyTable, ""),
					"gsi":   bwScan(ctx, t, client, emptyKeyTable, "by-g"),
					"bin":   bwScan(ctx, t, client, emptyKeyBinTable, ""),
				}
			})
		})
	}
}
