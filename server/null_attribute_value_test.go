package server

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func postRawJSON(t *testing.T, url, target, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	require.NoError(t, err)

	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(raw)
}

// A JSON null inside an L or an M can only come from raw JSON. DynamoDB Local rejects it
// with a ValidationException; it must not reach the interpreter (#268).
func TestServerRejectsNullAttributeValueInListOrMap(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	status, body := postRawJSON(t, ts.URL, "CreateTable", `{"TableName":"tbl","BillingMode":"PAY_PER_REQUEST",`+
		`"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],`+
		`"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`)
	require.Equal(t, http.StatusOK, status, body)

	tests := []struct {
		name   string
		target string
		body   string
		want   string
	}{
		{
			name:   "put attribute",
			target: "PutItem",
			body:   `{"TableName":"tbl","Item":{"id":{"S":"a"},"x":null}}`,
			want:   "The supplied Item contains a null AttributeValue: x is null",
		},
		{
			name:   "put list element",
			target: "PutItem",
			body:   `{"TableName":"tbl","Item":{"id":{"S":"a"},"l":{"L":[null]}}}`,
			want:   "The supplied Item contains a null AttributeValue",
		},
		{
			name:   "put map value",
			target: "PutItem",
			body:   `{"TableName":"tbl","Item":{"id":{"S":"a"},"m":{"M":{"k":null}}}}`,
			want:   "The supplied Item contains a null AttributeValue: k is null",
		},
		{
			name:   "update list element",
			target: "UpdateItem",
			body: `{"TableName":"tbl","Key":{"id":{"S":"a"}},"UpdateExpression":"SET l = :v",` +
				`"ExpressionAttributeValues":{":v":{"L":[null]}}}`,
			want: "The supplied Item contains a null AttributeValue",
		},
		{
			name:   "condition map value",
			target: "PutItem",
			body: `{"TableName":"tbl","Item":{"id":{"S":"a"}},"ConditionExpression":"l = :v",` +
				`"ExpressionAttributeValues":{":v":{"M":{"k":null}}}}`,
			want: "The supplied Item contains a null AttributeValue: k is null",
		},
		{
			name:   "transact condition check nested nil",
			target: "TransactWriteItems",
			body: `{"TransactItems":[{"ConditionCheck":{"TableName":"tbl","Key":{"id":{"S":"a"}},` +
				`"ConditionExpression":"x = :v","ExpressionAttributeValues":{":v":{"L":[null]}}}}]}`,
			want: "The supplied Item contains a null AttributeValue",
		},
		{
			name:   "transact condition check top-level null",
			target: "TransactWriteItems",
			body: `{"TransactItems":[{"ConditionCheck":{"TableName":"tbl","Key":{"id":{"S":"a"}},` +
				`"ConditionExpression":"x = :v","ExpressionAttributeValues":{":v":null}}}]}`,
			want: "ExpressionAttributeValues contains invalid value: The supplied Item contains a null AttributeValue for key :v",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := postRawJSON(t, ts.URL, tt.target, tt.body)

			require.Equal(t, http.StatusBadRequest, status, body)
			require.Contains(t, body, "ValidationException")
			require.Contains(t, body, tt.want)
		})
	}
}
