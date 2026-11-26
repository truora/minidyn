package e2e

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// TestE2E_BatchWriteEmptyItemOrKey pins that BatchWriteItem rejects a PutRequest with an empty
// Item and a DeleteRequest with an empty Key before it applies any request (#309), and the order
// of that check against the others of the batch. DynamoDB Local reports an empty Item as "Item
// to put in PutItem cannot be null" and an empty Key as "The number of conditions on the keys is
// invalid", at the position of the request among the checks of its table: after an invalid
// request or a duplicate that comes before it, before one that comes after. An invalid number
// anywhere in the batch is reported first.
//
// Which table is reported when two tables hold an invalid request depends on the wire order in
// Local (see core.ValidateBatchWrite), so no case here has an invalid request in two tables.
func TestE2E_BatchWriteEmptyItemOrKey(t *testing.T) {
	valid := ekItem{"pk": ekS("v"), "sk": ekS("1")}
	put := func(item ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{Item: item}}
	}
	del := func(key ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key}}
	}
	one := func(reqs ...dynamodbtypes.WriteRequest) map[string][]dynamodbtypes.WriteRequest {
		return map[string][]dynamodbtypes.WriteRequest{emptyKeyTable: reqs}
	}
	bad := &dynamodbtypes.AttributeValueMemberN{Value: "1e1000"}
	empty := ekItem{}

	tests := []struct {
		name  string
		items map[string][]dynamodbtypes.WriteRequest
	}{
		{"PutEmptyItem", one(put(valid), put(empty))},
		{"DeleteEmptyKey", one(put(valid), del(empty))},
		{"EmptyPutOnly", one(put(empty))},
		{"EmptyPutBeforeValid", one(put(empty), put(valid))},
		{"EmptyPutThenInvalidKey", one(put(valid), put(empty), put(ekItem{"pk": ekS(""), "sk": ekS("1")}))},
		{"InvalidKeyThenEmptyPut", one(put(valid), put(ekItem{"pk": ekS(""), "sk": ekS("1")}), put(empty))},
		{"EmptyPutThenDuplicate", one(put(valid), put(empty), put(valid))},
		{"DuplicateThenEmptyPut", one(put(valid), put(valid), put(empty))},
		{"EmptyDeleteThenDuplicate", one(put(valid), del(empty), del(valid))},
		{"DuplicateThenEmptyDelete", one(put(valid), del(valid), del(empty))},
		{"EmptyPutThenBadNumber", one(put(valid), put(empty), put(ekItem{"pk": ekS("n"), "sk": ekS("1"), "n": bad}))},
		{"BadNumberThenEmptyPut", one(put(valid), put(ekItem{"pk": ekS("n"), "sk": ekS("1"), "n": bad}), put(empty))},
		{"EmptyPutInSecondTable", map[string][]dynamodbtypes.WriteRequest{
			emptyKeyTable:    {put(valid)},
			emptyKeyBinTable: {put(empty)},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				ekCreateTables(ctx, t, client)

				_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: tt.items})

				return map[string]any{
					"error": fuzzErrString(err),
					"table": bwScan(ctx, t, client, emptyKeyTable, ""),
					"bin":   bwScan(ctx, t, client, emptyKeyBinTable, ""),
				}
			})
		})
	}
}

// rewriteBody replaces text in the body of every request. The SDK refuses to send a request
// without its required Item or Key, so a request that leaves them out is built by cutting them
// from the body.
type rewriteBody struct {
	next http.RoundTripper
	from []string
	to   []string
}

func (r rewriteBody) RoundTrip(req *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	body := string(raw)
	for i := range r.from {
		body = strings.ReplaceAll(body, r.from[i], r.to[i])
	}

	req.Body = io.NopCloser(strings.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Del("Content-Length")

	return r.next.RoundTrip(req)
}

// TestE2E_BatchWriteMissingItemOrKey pins what happens when a request of a BatchWriteItem leaves
// out its Item or Key (#309). The SDK would not send it, so the body is edited. DynamoDB Local
// reports "The supplied Item contains a null AttributeValue" in the pass that checks the numbers,
// in request order, before any other problem of the batch: a duplicate or an invalid key before
// it, and an unknown table, do not come first, and an invalid number before it does.
func TestE2E_BatchWriteMissingItemOrKey(t *testing.T) {
	valid := ekItem{"pk": ekS("v"), "sk": ekS("1")}
	badNumber := ekItem{"pk": ekS("n"), "sk": ekS("1"), "n": &dynamodbtypes.AttributeValueMemberN{Value: "1e1000"}}
	emptyKey := ekItem{"pk": ekS(""), "sk": ekS("1")}
	put := func(item ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{Item: item}}
	}
	del := func(key ekItem) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key}}
	}
	// the empty Item and Key are the ones cut from the body
	gone := ekItem{}
	one := func(reqs ...dynamodbtypes.WriteRequest) map[string][]dynamodbtypes.WriteRequest {
		return map[string][]dynamodbtypes.WriteRequest{emptyKeyTable: reqs}
	}

	tests := []struct {
		name  string
		items map[string][]dynamodbtypes.WriteRequest
	}{
		{"MissingItem", one(put(valid), put(gone))},
		{"MissingKey", one(put(valid), del(gone))},
		{"MissingItemOnly", one(put(gone))},
		{"DuplicateThenMissingItem", one(put(valid), put(valid), put(gone))},
		{"MissingKeyThenDuplicate", one(put(valid), del(gone), put(valid))},
		{"InvalidKeyThenMissingItem", one(put(emptyKey), put(gone))},
		{"MissingItemThenInvalidKey", one(put(gone), put(emptyKey))},
		{"BadNumberThenMissingItem", one(put(badNumber), put(gone))},
		{"MissingItemThenBadNumber", one(put(gone), put(badNumber))},
		{"MissingItemThenMissingKey", one(put(gone), del(gone))},
		{"MissingItemAndUnknownTable", map[string][]dynamodbtypes.WriteRequest{
			"no-such":     {put(valid)},
			emptyKeyTable: {put(gone)},
		}},
		{"MissingItemInUnknownTable", map[string][]dynamodbtypes.WriteRequest{"no-such": {put(gone)}}},
	}

	cut := func(next http.RoundTripper) http.RoundTripper {
		return rewriteBody{
			next: next,
			from: []string{`"PutRequest":{"Item":{}}`, `"DeleteRequest":{"Key":{}}`},
			to:   []string{`"PutRequest":{}`, `"DeleteRequest":{}`},
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := func(t *testing.T, endpoint string) any {
				t.Helper()
				ctx := context.Background()
				client := newWrappedSDKClient(t, endpoint, cut)

				ekCreateTables(ctx, t, client)

				_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: tt.items})

				return map[string]any{
					"error": fuzzErrString(err),
					"table": bwScan(ctx, t, client, emptyKeyTable, ""),
					"bin":   bwScan(ctx, t, client, emptyKeyBinTable, ""),
				}
			}

			var gotMinidyn, gotLocal any

			t.Run("minidyn", func(t *testing.T) { gotMinidyn = run(t, setupMinidynEndpoint(t)) })
			t.Run("dynamodb-local", func(t *testing.T) { gotLocal = run(t, setupDynamoDBLocalEndpoint(t)) })

			if !t.Failed() {
				assertParityEqual(t, gotMinidyn, gotLocal)
			}
		})
	}
}
