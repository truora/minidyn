package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// rawCall sends a DynamoDB JSON request straight to the server and decodes the raw response,
// so fields the SDK would default (Count: 0) or normalize (Items: nil vs []) are visible.
func rawCall(t *testing.T, srv http.Handler, op, body string) map[string]json.RawMessage {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func TestServerQueryNoMatchesRawJSON(t *testing.T) {
	srv := NewServer()

	rawCall(t, srv, "CreateTable", `{"TableName":"orders",
		"KeySchema":[{"AttributeName":"customer","KeyType":"HASH"}],
		"AttributeDefinitions":[{"AttributeName":"customer","AttributeType":"S"}],
		"BillingMode":"PAY_PER_REQUEST"}`)

	query := `{"TableName":"orders","KeyConditionExpression":"customer = :c",
		"ExpressionAttributeValues":{":c":{"S":"nobody"}}%s}`

	out := rawCall(t, srv, "Query", strings.Replace(query, "%s", "", 1))
	require.JSONEq(t, `0`, string(out["Count"]))
	require.JSONEq(t, `0`, string(out["ScannedCount"]))
	require.JSONEq(t, `[]`, string(out["Items"]))

	out = rawCall(t, srv, "Query", strings.Replace(query, "%s", `,"Select":"COUNT"`, 1))
	require.JSONEq(t, `0`, string(out["Count"]))
	require.JSONEq(t, `0`, string(out["ScannedCount"]))
	require.NotContains(t, out, "Items")

	out = rawCall(t, srv, "Scan", `{"TableName":"orders"}`)
	require.JSONEq(t, `0`, string(out["Count"]))
	require.JSONEq(t, `[]`, string(out["Items"]))
}
