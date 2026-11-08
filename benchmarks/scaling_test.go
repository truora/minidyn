package benchmarks

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

// scalingEnv enables TestScaling. It measures wall time, so it is opt-in: run it on a quiet
// machine or a dedicated CI job, not in the default unit-test run.
const scalingEnv = "MINIDYN_BENCH_SCALING"

const (
	smallSize = 1_000
	largeSize = 10_000

	// maxFlatGrowth is the largest ns/op ratio (large/small table) accepted for an operation
	// whose work does not depend on the table size. Ideal is 1x (or log(10x) ≈ 1.3x); the
	// slack absorbs cache effects and timer noise. A linear operation shows ~10x.
	maxFlatGrowth = 3.0
	// maxLinearGrowth is the largest per-item ratio accepted for full-table operations.
	maxLinearGrowth = 3.0
	// fixedGrowth is the ratio below which an operation with a known issue is reported as
	// fixed. It is well under the limits above so a noisy host doesn't flip a known issue.
	fixedGrowth = 1.5
)

// expectation says how an operation must scale and, if it currently doesn't, which perf issue
// in benchmarks/README.md documents it.
type expectation struct {
	linear     bool   // cost proportional to the table size (compared per item)
	knownIssue string // README section, e.g. "P1"; the check then expects the issue to persist
}

var scalingExpectations = map[string]expectation{
	"GetItem":                  {},
	"PutItemOverwrite":         {},
	"PutItemNewThenDelete":     {knownIssue: "P3"},
	"UpdateItemCounter":        {},
	"UpdateItemNamedCondition": {},
	"UpdateItemIndexKey":       {knownIssue: "P3"},
	"QueryPartition":           {},
	"QueryRangeLimit":          {},
	"QueryWithFilter":          {},
	"QueryGSI":                 {},
	"QueryGSIAfterWrite":       {knownIssue: "P3"},
	"BatchGetItem25":           {},
	"TransactWritePutUpdate":   {knownIssue: "P2"},
	"ScanFilterFullTable":      {linear: true},
}

// TestScaling fails when an operation's cost grows with the table size beyond its
// expectation, and when an operation with a known issue starts scaling well (so the issue can
// be closed and the expectation tightened).
func TestScaling(t *testing.T) {
	if os.Getenv(scalingEnv) == "" {
		t.Skipf("set %s=1 to run (timing-sensitive)", scalingEnv)
	}

	// testing.Benchmark uses -test.benchtime (1s by default); a shorter run per measurement
	// keeps the whole check around a minute and is still stable for 3x thresholds.
	if bt := flag.Lookup("test.benchtime"); bt != nil && bt.Value.String() == "1s" {
		if err := bt.Value.Set("200ms"); err != nil {
			t.Fatal(err)
		}
	}

	for _, op := range benchOps {
		exp, ok := scalingExpectations[op.name]
		if !ok {
			t.Errorf("benchmark %q has no scaling expectation", op.name)

			continue
		}

		t.Run(op.name, func(t *testing.T) {
			small := measure(op, smallSize)
			large := measure(op, largeSize)

			growth := large / small
			limit := maxFlatGrowth
			kind := "ns/op"

			if exp.linear {
				growth = (large / largeSize) / (small / smallSize)
				limit = maxLinearGrowth
				kind = "ns/item"
			}

			t.Logf("%s: %d items %.0f ns/op, %d items %.0f ns/op: %s grows %.1fx (limit %.1fx)",
				op.name, smallSize, small, largeSize, large, kind, growth, limit)

			checkGrowth(t, op.name, exp, growth, limit)
		})
	}
}

func checkGrowth(t *testing.T, name string, exp expectation, growth, limit float64) {
	t.Helper()

	switch {
	case exp.knownIssue == "" && growth > limit:
		t.Errorf("%s got %.1fx slower on a 10x larger table (limit %.1fx): a performance regression",
			name, growth, limit)
	case exp.knownIssue != "" && growth <= fixedGrowth:
		t.Errorf("%s now scales within %.1fx (%.1fx): known issue %s appears FIXED; remove its "+
			"knownIssue in scaling_test.go and its section in benchmarks/README.md", name, fixedGrowth, growth, exp.knownIssue)
	case exp.knownIssue != "":
		t.Skipf("known performance issue %s (benchmarks/README.md): %.1fx slower on a 10x larger table",
			exp.knownIssue, growth)
	}
}

// measure returns the best ns/op of three benchmark runs, which filters out noisy runs.
func measure(op benchOp, size int) float64 {
	best := 0.0

	for range 3 {
		r := testing.Benchmark(func(b *testing.B) { runBench(b, op, size) })
		if r.N == 0 {
			panic(fmt.Sprintf("benchmark %s/%d failed", op.name, size))
		}

		if ns := float64(r.T.Nanoseconds()) / float64(r.N); best == 0 || ns < best {
			best = ns
		}
	}

	return best
}

// knownIssueNames lists operations with a known issue, for the README consistency test.
func knownIssueNames() []string {
	var out []string

	for name, exp := range scalingExpectations {
		if exp.knownIssue != "" {
			out = append(out, name+"="+exp.knownIssue)
		}
	}

	return out
}

// TestScalingExpectationsDocumented keeps scaling_test.go and README.md in sync; it runs in
// the normal unit-test suite.
func TestScalingExpectationsDocumented(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range knownIssueNames() {
		issue := entry[strings.Index(entry, "=")+1:]
		if !strings.Contains(string(readme), "### "+issue+" ") {
			t.Errorf("%s: benchmarks/README.md has no \"### %s \" section", entry, issue)
		}
	}

	for _, op := range benchOps {
		if _, ok := scalingExpectations[op.name]; !ok {
			t.Errorf("benchmark %q has no scaling expectation", op.name)
		}
	}
}
