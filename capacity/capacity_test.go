package capacity

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/truora/minidyn/types"
)

func TestParseMode(t *testing.T) {
	cases := map[string]Mode{
		"":        ModeNone,
		"NONE":    ModeNone,
		"TOTAL":   ModeTotal,
		"INDEXES": ModeIndexes,
	}

	for in, want := range cases {
		got, err := ParseMode(in)
		if err != nil {
			t.Errorf("ParseMode(%q) returned error %v", in, err)
		}

		if got != want {
			t.Errorf("ParseMode(%q) = %q, want %q", in, got, want)
		}
	}
}

// DynamoDB rejects values outside the enum, and matches them case sensitively.
func TestParseModeRejectsValuesOutsideTheEnum(t *testing.T) {
	for _, in := range []string{"FOO", "total", "Indexes", " TOTAL"} {
		got, err := ParseMode(in)
		if err == nil {
			t.Fatalf("ParseMode(%q) = %q, want a validation error", in, got)
		}

		var apiErr types.Error
		if !errors.As(err, &apiErr) || apiErr.Code() != "ValidationException" {
			t.Fatalf("ParseMode(%q) error = %#v, want a ValidationException", in, err)
		}

		want := "1 validation error detected: Value '" + in + "' at 'returnConsumedCapacity' failed to " +
			"satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
		if apiErr.Message() != want {
			t.Fatalf("ParseMode(%q) message = %q, want %q", in, apiErr.Message(), want)
		}
	}
}

func TestReadUnits(t *testing.T) {
	cases := []struct {
		size       int
		consistent bool
		want       float64
	}{
		{0, true, 1},      // min 1
		{100, true, 1},    // < 4KB strong
		{100, false, 0.5}, // < 4KB eventual
		{5000, true, 2},   // ceil(5000/4096) = 2 blocks
		{5000, false, 1},  // 2 blocks halved
	}

	for _, c := range cases {
		if got := ReadUnits(c.size, c.consistent); got != c.want {
			t.Errorf("ReadUnits(%d, %v) = %v, want %v", c.size, c.consistent, got, c.want)
		}
	}
}

func TestWriteUnits(t *testing.T) {
	cases := []struct {
		size int
		want float64
	}{
		{0, 1},    // min 1
		{100, 1},  // < 1KB
		{1024, 1}, // exactly 1KB
		{1500, 2}, // ceil(1500/1024) = 2
	}

	for _, c := range cases {
		if got := WriteUnits(c.size); got != c.want {
			t.Errorf("WriteUnits(%d) = %v, want %v", c.size, got, c.want)
		}
	}
}

func TestForRead_None(t *testing.T) {
	if c := ForRead(ModeNone, "t", "", "", 100, true); c != nil {
		t.Fatalf("ModeNone must return nil, got %#v", c)
	}
}

func TestForRead_Total(t *testing.T) {
	c := ForRead(ModeTotal, "tbl", "", "", 100, true)
	if c == nil {
		t.Fatal("expected non-nil Consumed")
	}

	if c.TableName != "tbl" || c.CapacityUnits != 1 || c.ReadCapacityUnits != 1 {
		t.Fatalf("bad TOTAL read: %#v", c)
	}

	if c.WriteCapacityUnits != 0 || c.Breakdown || len(c.Indexes) != 0 {
		t.Fatalf("TOTAL must not set write/breakdown/index: %#v", c)
	}
}

func TestForRead_IndexesBaseTable(t *testing.T) {
	c := ForRead(ModeIndexes, "tbl", "", "", 100, true)
	if c == nil || !c.Breakdown {
		t.Fatalf("INDEXES must set Breakdown: %#v", c)
	}

	// A base-table read attributes its units to the Table sub-object and reports no index.
	if c.TableUnits != 1 || len(c.Indexes) != 0 {
		t.Fatalf("base-table INDEXES read must charge the table only: %#v", c)
	}
}

func TestForRead_IndexesGSI(t *testing.T) {
	c := ForRead(ModeIndexes, "tbl", "by-type", "GSI", 100, true)
	if c == nil || !c.Breakdown {
		t.Fatalf("INDEXES must set Breakdown: %#v", c)
	}

	// An index read charges the index; the Table sub-object is still reported, with zero units.
	if c.TableUnits != 0 {
		t.Fatalf("index read must leave the table sub-object at zero units: %#v", c)
	}

	if len(c.Indexes) != 1 || c.Indexes[0].Name != "by-type" || c.Indexes[0].Kind != "GSI" || c.Indexes[0].Units != 1 {
		t.Fatalf("INDEXES must carry the index entry: %#v", c)
	}
}

func TestForWrite(t *testing.T) {
	c := ForWrite(ModeTotal, "tbl", 100, nil)
	if c == nil || c.CapacityUnits != 1 || c.WriteCapacityUnits != 1 || c.ReadCapacityUnits != 0 {
		t.Fatalf("bad write: %#v", c)
	}
}

func TestForWrite_None(t *testing.T) {
	if c := ForWrite(ModeNone, "t", 100, nil); c != nil {
		t.Fatalf("ModeNone write must return nil, got %#v", c)
	}
}

func TestForWrite_WithIndexesTotal(t *testing.T) {
	// Two 1KB writes (base table + one GSI) cost 1 WCU each; the TOTAL always includes the GSI.
	c := ForWrite(ModeTotal, "tbl", 1024, []IndexWrite{{Name: "by-type", Kind: "GSI", Size: 1024}})
	if c == nil || c.CapacityUnits != 2 || c.WriteCapacityUnits != 2 {
		t.Fatalf("TOTAL write must include the GSI cost: %#v", c)
	}

	if c.Breakdown || len(c.Indexes) != 0 {
		t.Fatalf("TOTAL must not carry a breakdown: %#v", c)
	}
}

func TestForWrite_WithIndexesBreakdown(t *testing.T) {
	c := ForWrite(ModeIndexes, "tbl", 1024, []IndexWrite{{Name: "by-type", Kind: "GSI", Size: 1024}})
	if c == nil || !c.Breakdown || c.CapacityUnits != 2 {
		t.Fatalf("INDEXES write total must include the GSI cost: %#v", c)
	}

	if c.TableUnits != 1 {
		t.Fatalf("base-table sub-object must carry the table write units: %#v", c)
	}

	if len(c.Indexes) != 1 || c.Indexes[0].Name != "by-type" || c.Indexes[0].Kind != "GSI" || c.Indexes[0].Units != 1 {
		t.Fatalf("INDEXES write must carry the per-GSI entry: %#v", c)
	}
}

func TestConsumedByTable(t *testing.T) {
	r := ConsumedByTable(ModeTotal, map[string]float64{"tbl": 3.5}, true)
	if len(r) != 1 || r[0].TableName != "tbl" || r[0].CapacityUnits != 3.5 || r[0].ReadCapacityUnits != 3.5 || r[0].WriteCapacityUnits != 0 {
		t.Fatalf("bad read capacity: %#v", r)
	}

	w := ConsumedByTable(ModeTotal, map[string]float64{"tbl": 4}, false)
	if len(w) != 1 || w[0].CapacityUnits != 4 || w[0].WriteCapacityUnits != 4 || w[0].ReadCapacityUnits != 0 {
		t.Fatalf("bad write capacity: %#v", w)
	}

	if ConsumedByTable(ModeNone, map[string]float64{"tbl": 3}, true) != nil ||
		ConsumedByTable(ModeTotal, nil, true) != nil ||
		ConsumedByTable(ModeTotal, map[string]float64{"tbl": 0}, true) != nil {
		t.Fatalf("NONE, no tables and zero-unit tables must report nothing")
	}
}

func TestConsumedByTable_IndexesAttributesToTable(t *testing.T) {
	// At the INDEXES level aggregated units are attributed to the base-table sub-object; batch
	// and transactional ops report table totals only, with no per-index breakdown.
	r := ConsumedByTable(ModeIndexes, map[string]float64{"tbl": 3.5}, true)
	if len(r) != 1 || !r[0].Breakdown || r[0].TableUnits != 3.5 || len(r[0].Indexes) != 0 {
		t.Fatalf("bad INDEXES read capacity: %#v", r)
	}
}

func TestConsumedByTable_IsSortedByTableName(t *testing.T) {
	units := map[string]float64{}
	want := make([]string, 0, 12)

	for i := range 12 {
		name := fmt.Sprintf("table-%02d", i)
		units[name] = 1
		want = append(want, name)
	}

	for range 20 {
		var got []string
		for _, c := range ConsumedByTable(ModeTotal, units, true) {
			got = append(got, c.TableName)
		}

		if !slices.Equal(got, want) {
			t.Fatalf("tables = %v, want %v", got, want)
		}
	}
}

func TestWriteAccumulator_None(t *testing.T) {
	a := NewWriteAccumulator()
	a.Add("t", 1024, nil, 1)

	if a.Consumed(ModeNone) != nil {
		t.Fatalf("ModeNone must produce no consumed entries")
	}

	if NewWriteAccumulator().Consumed(ModeTotal) != nil {
		t.Fatalf("empty accumulator must produce no consumed entries")
	}
}

func TestWriteAccumulator_TotalAggregates(t *testing.T) {
	a := NewWriteAccumulator()
	// Two 1KB writes to the same table, plus one GSI touched by both.
	a.Add("t", 1024, []IndexWrite{{Name: "g", Kind: "GSI", Size: 1024}}, 1)
	a.Add("t", 1024, []IndexWrite{{Name: "g", Kind: "GSI", Size: 1024}}, 1)

	out := a.Consumed(ModeTotal)
	if len(out) != 1 || out[0].CapacityUnits != 4 || out[0].Breakdown {
		t.Fatalf("TOTAL must sum table+GSI without a breakdown: %#v", out)
	}
}

func TestWriteAccumulator_IndexesBreakdownSorted(t *testing.T) {
	a := NewWriteAccumulator()
	// Transactional factor doubles every unit; two GSIs reported in name order.
	a.Add("t", 1024, []IndexWrite{
		{Name: "z-idx", Kind: "GSI", Size: 1024},
		{Name: "a-idx", Kind: "LSI", Size: 1024},
	}, 2)

	out := a.Consumed(ModeIndexes)
	if len(out) != 1 {
		t.Fatalf("expected one table entry: %#v", out)
	}

	c := out[0]
	if !c.Breakdown || c.TableUnits != 2 || c.CapacityUnits != 6 {
		t.Fatalf("INDEXES total/table units wrong: %#v", c)
	}

	if len(c.Indexes) != 2 || c.Indexes[0].Name != "a-idx" || c.Indexes[1].Name != "z-idx" {
		t.Fatalf("per-index entries must be name-sorted: %#v", c.Indexes)
	}

	if c.Indexes[0].Units != 2 || c.Indexes[1].Units != 2 {
		t.Fatalf("per-index units must include the transactional factor: %#v", c.Indexes)
	}
}

func TestWriteAccumulator_ZeroUnitTableOmitted(t *testing.T) {
	a := NewWriteAccumulator()
	a.Add("t", 0, nil, 0) // factor 0 -> no units

	if out := a.Consumed(ModeTotal); out != nil {
		t.Fatalf("zero-unit table must be omitted: %#v", out)
	}
}

func TestWriteAccumulator_AddTransactDoublesWCU(t *testing.T) {
	single := NewWriteAccumulator()
	single.Add("t", 1000, []IndexWrite{{Name: "i", Kind: "GSI", Size: 100}}, 1)

	txn := NewWriteAccumulator()
	txn.AddTransact("t", 1000, []IndexWrite{{Name: "i", Kind: "GSI", Size: 100}})

	got, want := txn.Consumed(ModeIndexes), single.Consumed(ModeIndexes)
	if len(got) != 1 || len(want) != 1 {
		t.Fatalf("want one table, got %#v and %#v", got, want)
	}

	if got[0].CapacityUnits != 2*want[0].CapacityUnits || got[0].TableUnits != 2*want[0].TableUnits {
		t.Fatalf("transact write should double: single=%#v txn=%#v", want[0], got[0])
	}

	if got[0].Indexes[0].Units != 2*want[0].Indexes[0].Units {
		t.Fatalf("transact write should double index units: single=%#v txn=%#v", want[0].Indexes, got[0].Indexes)
	}
}

func TestTransactReadUnits_DoublesStronglyConsistentRCU(t *testing.T) {
	for _, size := range []int{0, 100, 5000} {
		if got, want := TransactReadUnits(size), 2*ReadUnits(size, true); got != want {
			t.Fatalf("TransactReadUnits(%d) = %v, want %v", size, got, want)
		}
	}
}
