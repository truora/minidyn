package benchmarks

import (
	"context"
	"testing"
)

// allocSmall and allocLarge are the table sizes compared by TestAllocationsDoNotGrow. They're
// small enough to build quickly in the unit-test suite.
const (
	allocSmall = 100
	allocLarge = 2_000
)

// allocKnownIssues are operations whose allocations currently grow with the table size,
// with the README section that documents why. Operations not listed (and not linear) must
// allocate the same on both sizes.
var allocKnownIssues = map[string]string{
	"TransactWritePutUpdate": "P2",
}

// TestAllocationsDoNotGrow is the deterministic counterpart of TestScaling, run in the
// normal unit-test suite: allocation counts don't depend on machine load, so a point
// operation or one-partition query that starts allocating per item in the *table* fails here.
func TestAllocationsDoNotGrow(t *testing.T) {
	for _, op := range benchOps {
		if scalingExpectations[op.name].linear {
			continue
		}

		t.Run(op.name, func(t *testing.T) {
			small := allocsPerOp(t, op, allocSmall)
			large := allocsPerOp(t, op, allocLarge)
			grows := large > small*1.2+5

			issue := allocKnownIssues[op.name]

			switch {
			case issue == "" && grows:
				t.Errorf("%s allocates %.0f per op on %d items but %.0f on %d items: cost grows with the table",
					op.name, small, allocSmall, large, allocLarge)
			case issue != "" && !grows:
				t.Errorf("%s no longer allocates more on a larger table (%.0f vs %.0f): known issue %s appears "+
					"FIXED; remove it from allocKnownIssues (and scalingExpectations if it scales now)",
					op.name, small, large, issue)
			case issue != "":
				t.Skipf("known performance issue %s: %.0f allocs/op on %d items, %.0f on %d",
					issue, small, allocSmall, large, allocLarge)
			}
		})
	}
}

func allocsPerOp(t *testing.T, op benchOp, size int) float64 {
	t.Helper()

	c := newTable(t, size)
	ctx := context.Background()
	i := 0

	return testing.AllocsPerRun(50, func() {
		if err := op.run(ctx, c, size, i); err != nil {
			t.Fatal(err)
		}

		i++
	})
}
