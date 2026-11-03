package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// Differential fuzzing: random operation sequences (fuzz_gen_test.go) run in lockstep against
// minidyn and DynamoDB Local; the first response that differs after normalization fails the
// seed with a reproducible report. The final table and index contents are compared too.
//
//	MINIDYN_E2E_FUZZ_SEEDS=n   number of seeds to run (default 8), starting at
//	MINIDYN_E2E_FUZZ_START=s   first seed (default 1)
//	MINIDYN_E2E_FUZZ_SEED=s    run only seed s (to reproduce a failure)
//	MINIDYN_E2E_FUZZ_OPS=n     operations per seed (default 40)
//	MINIDYN_E2E_FUZZ_KNOWN=1   also generate inputs that hit open parity issues
//	                           (explore mode: expect failures from known bugs)
//
// By default the generator avoids areas with an open parity issue, so a failure is a new gap:
// file a GitHub issue and add a focused parity test for it.
const (
	fuzzSeedsEnv = "MINIDYN_E2E_FUZZ_SEEDS"
	fuzzStartEnv = "MINIDYN_E2E_FUZZ_START"
	fuzzSeedEnv  = "MINIDYN_E2E_FUZZ_SEED"
	fuzzOpsEnv   = "MINIDYN_E2E_FUZZ_OPS"
	fuzzKnownEnv = "MINIDYN_E2E_FUZZ_KNOWN"
)

func envUint(t *testing.T, name string, def uint64) uint64 {
	t.Helper()

	v := os.Getenv(name)
	if v == "" {
		return def
	}

	n, err := strconv.ParseUint(v, 10, 64)
	require.NoError(t, err, "%s", name)

	return n
}

func fuzzSeeds(t *testing.T) []uint64 {
	t.Helper()

	if os.Getenv(fuzzSeedEnv) != "" {
		return []uint64{envUint(t, fuzzSeedEnv, 0)}
	}

	start := envUint(t, fuzzStartEnv, 1)
	count := envUint(t, fuzzSeedsEnv, 8)

	seeds := make([]uint64, 0, count)
	for s := start; s < start+count; s++ {
		seeds = append(seeds, s)
	}

	return seeds
}

func TestE2E_Fuzz(t *testing.T) {
	ops := int(envUint(t, fuzzOpsEnv, 40)) //nolint:gosec // small test parameter
	known := os.Getenv(fuzzKnownEnv) != ""

	for _, seed := range fuzzSeeds(t) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runFuzzSeed(t, seed, ops, known)
		})
	}
}

func runFuzzSeed(t *testing.T, seed uint64, ops int, known bool) {
	t.Helper()

	ctx := context.Background()
	minidynEP := setupMinidynEndpoint(t)
	localEP := setupDynamoDBLocalEndpoint(t)

	for _, ep := range []string{minidynEP, localEP} {
		client := newSDKClient(t, ep)
		require.NoError(t, fuzzCreateTable(ctx, client))
		parityWaitUntilGSIActive(ctx, t, client, fuzzTable, fuzzGSI)
	}

	recM, recL := &traceRecorder{}, &traceRecorder{}
	clientM := newTracedSDKClient(t, minidynEP, recM)
	clientL := newTracedSDKClient(t, localEP, recL)

	gen := newFuzzGen(seed, known)

	for i := range ops {
		op := gen.next()
		op.run(ctx, clientM)
		op.run(ctx, clientL)

		// Every op is exactly one HTTP call (no retries). Anything else, e.g. a transport error
		// that records nothing, must not be compared as the previous op's response.
		callsM, callsL := recM.snapshot(), recL.snapshot()
		if len(callsM) != i+1 || len(callsL) != i+1 {
			t.Fatalf("seed %d op #%d %s: expected %d recorded HTTP exchanges, got minidyn %d, dynamodb-local %d",
				seed, i, op.name, i+1, len(callsM), len(callsL))
		}

		m, l := fuzzNormalize(callsM[len(callsM)-1]), fuzzNormalize(callsL[len(callsL)-1])

		if d := cmp.Diff(fuzzComparable(m, known), fuzzComparable(l, known)); d != "" {
			d += fmt.Sprintf("\nminidyn:        %d %s\ndynamodb-local: %d %s",
				m.Status, fuzzJSON(m.Response), l.Status, fuzzJSON(l.Response))
			t.Fatalf("%s", fuzzReport(seed, known, fmt.Sprintf("op #%d %s", i, op.name), callsL, d))
		}
	}

	stateM := fuzzScansOnly(normalizeTrace(captureFinalState(t, minidynEP, []string{fuzzTable})))
	stateL := fuzzScansOnly(normalizeTrace(captureFinalState(t, localEP, []string{fuzzTable})))

	if diffs := diffEntries("final state", fuzzNormalizeAll(stateM), fuzzNormalizeAll(stateL)); len(diffs) > 0 {
		t.Fatalf("%s", fuzzReport(seed, known, "final state", recL.snapshot(), strings.Join(diffs, "\n")))
	}
}

// fuzzComparable is what must match for one call. Outside known mode, error messages are
// not compared (only the exception type): many validation messages differ (#164).
// fuzzScansOnly drops DescribeTable from a final-state snapshot: table descriptions are a
// known gap (#151) and the fuzzer never changes the schema.
func fuzzScansOnly(entries []traceEntry) []traceEntry {
	out := entries[:0:0]

	for _, e := range entries {
		if e.Op != "DescribeTable" {
			out = append(out, e)
		}
	}

	return out
}

func fuzzComparable(e traceEntry, known bool) map[string]any {
	resp := e.Response

	if r, ok := resp.(map[string]any); ok && e.Status >= 300 && !known {
		r = maps.Clone(r)
		delete(r, "message")
		resp = r
	}

	return map[string]any{"status": e.Status, "response": resp}
}

func fuzzNormalizeAll(entries []traceEntry) []traceEntry {
	out := make([]traceEntry, len(entries))
	for i, e := range entries {
		out[i] = fuzzNormalize(e)
	}

	return out
}

// fuzzNormalize applies the trace normalization plus rules for open issues that would
// otherwise make every seed fail on its first write or read. Remove a rule when its issue
// is fixed.
func fuzzNormalize(e traceEntry) traceEntry {
	e = normalizeTrace([]traceEntry{e})[0]

	resp, ok := e.Response.(map[string]any)
	if !ok {
		return e
	}

	req, _ := e.Request.(map[string]any)

	switch e.Op {
	case "PutItem":
		// #141: PutItem returns the written item even without ReturnValues.
		if _, asked := req["ReturnValues"]; !asked {
			delete(resp, "Attributes")
		}
	case "Query", "Scan":
		// #144: ScannedCount is never set; Count 0 and empty Items are omitted.
		delete(resp, "ScannedCount")

		if c, ok := resp["Count"].(json.Number); ok && c == "0" {
			delete(resp, "Count")
		}

		if items, ok := resp["Items"].([]any); ok && len(items) == 0 {
			delete(resp, "Items")
		}

		// A GSI query reads one g partition in sk order, but items with the same sk (and
		// different pk) have no defined order: sort only within those runs, so the sk order
		// and ScanIndexForward are still checked. (Scan items are already fully sorted.)
		if _, onIndex := req["IndexName"]; onIndex && e.Op == "Query" {
			if items, ok := resp["Items"].([]any); ok {
				resp["Items"] = sortTies(items, "sk")
			}
		}
	}

	return e
}

// sortTies sorts each run of consecutive items with an equal attr value by JSON, keeping the
// runs in their original order.
func sortTies(items []any, attr string) []any {
	out := make([]any, 0, len(items))

	for start := 0; start < len(items); {
		end := start + 1
		for end < len(items) && cmp.Equal(attrOf(items[end], attr), attrOf(items[start], attr)) {
			end++
		}

		run, _ := sortedByJSON(items[start:end]).([]any)
		out = append(out, run...)
		start = end
	}

	return out
}

func attrOf(item any, attr string) any {
	m, _ := item.(map[string]any)

	return m[attr]
}

func fuzzJSON(v any) string {
	b, _ := json.Marshal(v)

	return truncate(string(b), 600)
}

// fuzzReport describes a divergence with everything needed to reproduce it.
func fuzzReport(seed uint64, known bool, where string, calls []traceEntry, diff string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "minidyn diverges from DynamoDB Local at %s (seed %d)\n", where, seed)
	fmt.Fprintf(&b, "diff (-minidyn +dynamodb-local):\n%s\n", diff)
	fmt.Fprintf(&b, "requests so far (table %q: pk S, sk S, GSI %q on g/sk):\n", fuzzTable, fuzzGSI)

	for i, c := range calls {
		req, _ := json.Marshal(c.Request)
		fmt.Fprintf(&b, "  #%d %s %s\n", i, c.Op, req)
	}

	if len(calls) > 0 {
		req, _ := json.Marshal(calls[len(calls)-1].Request)
		fmt.Fprintf(&b, "failing request: %s\n", req)
	}

	env := ""
	if known {
		env = fuzzKnownEnv + "=1 "
	}

	fmt.Fprintf(&b, "reproduce: %s%s=%d go run ./tools/run-e2e TestE2E_Fuzz -count=1", env, fuzzSeedEnv, seed)

	return b.String()
}
