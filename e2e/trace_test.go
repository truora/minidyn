package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
)

// Full-response parity ("trace diff").
//
// Parity tests only compare what each test returns, so a divergence in a response field the
// test ignores (ScannedCount, ReturnValues, a projected attribute, a stray error message...)
// goes unnoticed. With tracing enabled, RunE2E also records every raw HTTP response each
// engine sends back, plus a snapshot of every table (DescribeTable and a Scan of the table
// and each index) at the end of the test, normalizes both and diffs them.
//
//	MINIDYN_E2E_TRACE=report  log differences (t.Log, visible with -v) and keep tests green
//	MINIDYN_E2E_TRACE=strict  fail tests whose traces differ
//	MINIDYN_E2E_TRACE_OUT=f   also append one JSON line per diverging test to file f
//
// Tracing is off by default: it surfaces gaps to triage, it is not a gate yet.
const (
	traceModeEnv = "MINIDYN_E2E_TRACE"
	traceOutEnv  = "MINIDYN_E2E_TRACE_OUT"
)

type traceMode int

const (
	traceOff traceMode = iota
	traceReport
	traceStrict
)

func currentTraceMode() traceMode {
	switch strings.ToLower(os.Getenv(traceModeEnv)) {
	case "report":
		return traceReport
	case "strict":
		return traceStrict
	default:
		return traceOff
	}
}

// traceEntry is one API call as seen on the wire.
type traceEntry struct {
	Op       string `json:"op"`
	Request  any    `json:"request,omitempty"`
	Status   int    `json:"status"`
	Response any    `json:"response,omitempty"`
}

// traceRecorder is an http.RoundTripper that records each DynamoDB call.
type traceRecorder struct {
	next http.RoundTripper

	mu      sync.Mutex
	entries []traceEntry
}

// newTracedSDKClient returns an SDK client for endpoint whose calls are recorded by rec.
func newTracedSDKClient(t *testing.T, endpoint string, rec *traceRecorder) *dynamodb.Client {
	t.Helper()

	return newWrappedSDKClient(t, endpoint, func(next http.RoundTripper) http.RoundTripper {
		rec.next = next

		return rec
	})
}

func (r *traceRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	var reqBody []byte

	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}

		_ = req.Body.Close()
		reqBody = b
		req.Body = io.NopCloser(bytes.NewReader(b))
	}

	resp, err := r.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if err != nil {
		return nil, err
	}

	resp.Body = io.NopCloser(bytes.NewReader(respBody))

	op := req.Header.Get("X-Amz-Target")
	if i := strings.LastIndex(op, "."); i >= 0 {
		op = op[i+1:]
	}

	r.mu.Lock()
	r.entries = append(r.entries, traceEntry{
		Op:       op,
		Request:  decodeJSONOrString(reqBody),
		Status:   resp.StatusCode,
		Response: decodeJSONOrString(respBody),
	})
	r.mu.Unlock()

	return resp, nil
}

func (r *traceRecorder) snapshot() []traceEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]traceEntry(nil), r.entries...)
}

func decodeJSONOrString(b []byte) any {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}

	var v any

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()

	if err := dec.Decode(&v); err != nil {
		return string(b)
	}

	return v
}

// engineTrace is what one engine produced for one test.
type engineTrace struct {
	Calls []traceEntry
	State []traceEntry
}

// createdTables returns the sorted, de-duplicated table names of successful CreateTable
// calls in a trace. The final-state snapshot uses them instead of ListTables, which minidyn
// does not implement.
func createdTables(calls []traceEntry) []string {
	seen := map[string]bool{}

	var names []string

	for _, c := range calls {
		if c.Op != "CreateTable" || c.Status != http.StatusOK {
			continue
		}

		req, _ := c.Request.(map[string]any)
		if name, ok := req["TableName"].(string); ok && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}

	sort.Strings(names)

	return names
}

// captureFinalState records, through a separate client, the state a test left behind in
// tables: DescribeTable plus a consistent Scan of each table and an (eventually consistent)
// Scan of each of its indexes. Errors are recorded too (for example a deleted table): they
// are part of the state to compare.
func captureFinalState(t *testing.T, endpoint string, tables []string) []traceEntry {
	t.Helper()

	rec := &traceRecorder{}
	client := newTracedSDKClient(t, endpoint, rec)
	ctx := context.Background()

	for _, name := range tables {
		desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if err != nil {
			continue
		}

		_, _ = client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(name), ConsistentRead: aws.Bool(true)})

		var indexes []string

		for _, gsi := range desc.Table.GlobalSecondaryIndexes {
			indexes = append(indexes, aws.ToString(gsi.IndexName))
		}

		for _, lsi := range desc.Table.LocalSecondaryIndexes {
			indexes = append(indexes, aws.ToString(lsi.IndexName))
		}

		sort.Strings(indexes)

		for _, idx := range indexes {
			_, _ = client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(name), IndexName: aws.String(idx)})
		}
	}

	return rec.snapshot()
}

// compareTraces reports differences between the two engines' traces according to mode.
func compareTraces(t *testing.T, mode traceMode, minidyn, local engineTrace) {
	t.Helper()

	var diffs []string

	diffs = append(diffs, diffEntries("call", normalizeTrace(minidyn.Calls), normalizeTrace(local.Calls))...)
	diffs = append(diffs, diffEntries("final state", normalizeTrace(minidyn.State), normalizeTrace(local.State))...)

	if len(diffs) == 0 {
		return
	}

	msg := fmt.Sprintf("PARITY-TRACE: %d difference(s) between minidyn (-) and dynamodb-local (+):\n%s",
		len(diffs), strings.Join(diffs, "\n"))

	writeTraceReport(t, diffs)

	if mode == traceStrict {
		t.Error(msg)

		return
	}

	t.Log(msg)
}

func diffEntries(kind string, m, l []traceEntry) []string {
	var out []string

	n := min(len(m), len(l))

	for i := 0; i < n; i++ {
		if m[i].Op != l[i].Op {
			out = append(out, fmt.Sprintf("%s #%d: operation sequence diverges (minidyn %s, dynamodb-local %s); "+
				"later entries not compared", kind, i, m[i].Op, l[i].Op))

			return out
		}

		mm := map[string]any{"status": m[i].Status, "response": m[i].Response}
		ll := map[string]any{"status": l[i].Status, "response": l[i].Response}

		if d := cmp.Diff(mm, ll); d != "" {
			req, _ := json.Marshal(m[i].Request)
			out = append(out, fmt.Sprintf("%s #%d %s %s\n%s", kind, i, m[i].Op, truncate(string(req), 300), d))
		}
	}

	if len(m) != len(l) {
		out = append(out, fmt.Sprintf("%s: minidyn made %d call(s), dynamodb-local %d", kind, len(m), len(l)))
	}

	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n] + "..."
}

var traceReportMu sync.Mutex

// writeTraceReport appends {"test": ..., "diffs": [...]} to MINIDYN_E2E_TRACE_OUT, if set.
func writeTraceReport(t *testing.T, diffs []string) {
	t.Helper()

	path := os.Getenv(traceOutEnv)
	if path == "" {
		return
	}

	line, err := json.Marshal(map[string]any{"test": t.Name(), "diffs": diffs})
	if err != nil {
		return
	}

	traceReportMu.Lock()
	defer traceReportMu.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // path comes from the developer's env
	if err != nil {
		t.Logf("trace report: %v", err)

		return
	}

	defer func() { _ = f.Close() }()

	_, _ = f.Write(append(line, '\n'))
}

// volatileFields differ between any two DynamoDB deployments and carry no behavior.
var volatileFields = map[string]bool{
	"TableArn":                          true,
	"TableId":                           true,
	"IndexArn":                          true,
	"CreationDateTime":                  true,
	"LastIncreaseDateTime":              true,
	"LastDecreaseDateTime":              true,
	"LastUpdateToPayPerRequestDateTime": true,
	"ItemCount":                         true,
	"TableSizeBytes":                    true,
	"IndexSizeBytes":                    true,
	"LatestStreamArn":                   true,
	"LatestStreamLabel":                 true,
	// DynamoDB Local reports capacity on some writes even when ReturnConsumedCapacity is
	// not set; DynamoDB only returns it on request and minidyn never does.
	"ConsumedCapacity": true,
}

// listOrderKeys are lists whose order DynamoDB does not specify, with the field that
// identifies each element.
var listOrderKeys = map[string]string{
	"AttributeDefinitions":   "AttributeName",
	"GlobalSecondaryIndexes": "IndexName",
	"LocalSecondaryIndexes":  "IndexName",
}

// normalizeTrace makes traces comparable:
//   - volatile fields (ARNs, ids, timestamps, size counters) are dropped;
//   - set members (SS/NS/BS) and Scan / BatchGetItem result items are sorted, since their
//     order is unspecified;
//   - error bodies keep the exception name without its namespace and the message under one key;
//   - consecutive identical DescribeTable polls are collapsed to the last one, since how many
//     polls a test needs depends on timing (minidyn activates indexes after a delay).
func normalizeTrace(entries []traceEntry) []traceEntry {
	out := make([]traceEntry, 0, len(entries))

	for _, e := range entries {
		e.Response = normalizeValue(e.Response)
		e.Request = normalizeValue(e.Request)

		if e.Status >= 300 {
			e.Response = normalizeErrorBody(e.Response)
		}

		if e.Op == "Scan" || e.Op == "BatchGetItem" {
			e.Response = sortItemLists(e.Response)
		}

		if e.Op == "DescribeTable" && len(out) > 0 {
			prev := out[len(out)-1]
			if prev.Op == e.Op && cmp.Equal(prev.Request, e.Request) {
				out[len(out)-1] = e

				continue
			}
		}

		out = append(out, e)
	}

	return out
}

// normalizeField applies the per-field rules to an already normalized value.
func normalizeField(key string, val any) any {
	if key == "N" {
		return canonicalNumber(val)
	}

	if key == "NS" {
		if xs, ok := val.([]any); ok {
			for i := range xs {
				xs[i] = canonicalNumber(xs[i])
			}
		}
	}

	if key == "SS" || key == "NS" || key == "BS" {
		return sortedByJSON(val)
	}

	if field, ok := listOrderKeys[key]; ok {
		return sortedByField(val, field)
	}

	// DynamoDB Local activates new indexes instantly; DynamoDB (and minidyn, after
	// SetIndexActivationDelay) reports CREATING first. Both are valid.
	if key == "IndexStatus" && val == "CREATING" {
		return "ACTIVE"
	}

	return val
}

// canonicalNumber rewrites a DynamoDB number string by value: DynamoDB Local returns
// arithmetic results as "3.0" where DynamoDB and minidyn return "3". This also hides
// differences in how stored numbers are written ("1.50" vs "1.5"); cover those with a
// focused parity test instead.
func canonicalNumber(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return v
	}

	if r.IsInt() {
		return r.Num().String()
	}

	out := strings.TrimRight(r.FloatString(40), "0")

	return strings.TrimSuffix(out, ".")
}

func normalizeValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))

		for k, val := range x {
			if volatileFields[k] || isAbsent(k, val) {
				continue
			}

			val = normalizeField(k, normalizeValue(val))

			m[k] = val
		}

		return m
	case []any:
		s := make([]any, len(x))
		for i, val := range x {
			s[i] = normalizeValue(val)
		}

		return s
	default:
		return v
	}
}

// isAbsent reports values the SDK decodes the same as a missing field: JSON null, and empty
// strings outside attribute values (minidyn serializes zero-valued enums such as
// MultiRegionConsistency as ""). Empty "S"/"B"/"N" values are real data and are kept.
// Empty UnprocessedItems / UnprocessedKeys maps also mean "nothing left to retry".
func isAbsent(key string, v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return x == "" && key != "S" && key != "B" && key != "N"
	case map[string]any:
		return len(x) == 0 && (key == "UnprocessedItems" || key == "UnprocessedKeys")
	default:
		return false
	}
}

func normalizeErrorBody(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}

	out := map[string]any{}

	if typ, ok := m["__type"].(string); ok {
		if i := strings.LastIndex(typ, "#"); i >= 0 {
			typ = typ[i+1:]
		}

		out["__type"] = typ
	}

	for _, k := range []string{"message", "Message"} {
		if msg, ok := m[k].(string); ok {
			out["message"] = normalizeSDKErrorString(msg)
		}
	}

	for k, val := range m {
		if k != "__type" && k != "message" && k != "Message" {
			out[k] = val
		}
	}

	return out
}

// sortItemLists sorts every "Items" list and every list under "Responses" (BatchGetItem).
func sortItemLists(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}

	if items, ok := m["Items"]; ok {
		m["Items"] = sortedByJSON(items)
	}

	if resp, ok := m["Responses"].(map[string]any); ok {
		for k, items := range resp {
			resp[k] = sortedByJSON(items)
		}
	}

	return m
}

func sortedByField(v any, field string) any {
	s, ok := v.([]any)
	if !ok {
		return v
	}

	out := append([]any(nil), s...)

	sort.SliceStable(out, func(i, j int) bool {
		a, _ := out[i].(map[string]any)
		b, _ := out[j].(map[string]any)
		as, _ := a[field].(string)
		bs, _ := b[field].(string)

		return as < bs
	})

	return out
}

func sortedByJSON(v any) any {
	s, ok := v.([]any)
	if !ok {
		return v
	}

	type keyed struct {
		key string
		val any
	}

	ks := make([]keyed, len(s))

	for i, val := range s {
		b, _ := json.Marshal(val) // map keys are marshaled sorted, so this is canonical
		ks[i] = keyed{string(b), val}
	}

	sort.SliceStable(ks, func(i, j int) bool { return ks[i].key < ks[j].key })

	out := make([]any, len(ks))
	for i, k := range ks {
		out[i] = k.val
	}

	return out
}
