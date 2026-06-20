package capacity

import "testing"

func TestParseMode(t *testing.T) {
	cases := map[string]Mode{
		"":         ModeNone,
		"NONE":     ModeNone,
		"TOTAL":    ModeTotal,
		"INDEXES":  ModeIndexes,
		"whatever": ModeNone,
	}

	for in, want := range cases {
		if got := ParseMode(in); got != want {
			t.Errorf("ParseMode(%q) = %q, want %q", in, got, want)
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

	if c.WriteCapacityUnits != 0 || c.Breakdown || c.IndexName != "" {
		t.Fatalf("TOTAL must not set write/breakdown/index: %#v", c)
	}
}

func TestForRead_IndexesGSI(t *testing.T) {
	c := ForRead(ModeIndexes, "tbl", "by-type", "GSI", 100, true)
	if c == nil || !c.Breakdown {
		t.Fatalf("INDEXES must set Breakdown: %#v", c)
	}

	if c.IndexName != "by-type" || c.IndexKind != "GSI" {
		t.Fatalf("INDEXES must carry index info: %#v", c)
	}
}

func TestForWrite(t *testing.T) {
	c := ForWrite(ModeTotal, "tbl", 100)
	if c == nil || c.CapacityUnits != 1 || c.WriteCapacityUnits != 1 || c.ReadCapacityUnits != 0 {
		t.Fatalf("bad write: %#v", c)
	}
}

func TestForWrite_None(t *testing.T) {
	if c := ForWrite(ModeNone, "t", 100); c != nil {
		t.Fatalf("ModeNone write must return nil, got %#v", c)
	}
}

func TestForUnits(t *testing.T) {
	r := ForReadUnits(ModeTotal, "tbl", 3.5)
	if r == nil || r.CapacityUnits != 3.5 || r.ReadCapacityUnits != 3.5 || r.WriteCapacityUnits != 0 {
		t.Fatalf("bad ForReadUnits: %#v", r)
	}

	w := ForWriteUnits(ModeTotal, "tbl", 4)
	if w == nil || w.CapacityUnits != 4 || w.WriteCapacityUnits != 4 || w.ReadCapacityUnits != 0 {
		t.Fatalf("bad ForWriteUnits: %#v", w)
	}

	if ForReadUnits(ModeNone, "tbl", 3) != nil || ForWriteUnits(ModeNone, "tbl", 3) != nil {
		t.Fatalf("ModeNone unit constructors must return nil")
	}
}

func TestForTransactWrite_DoublesWCU(t *testing.T) {
	single := ForWrite(ModeTotal, "t", 1000)
	txn := ForTransactWrite(ModeTotal, "t", 1000)

	if txn.CapacityUnits != 2*single.CapacityUnits {
		t.Fatalf("transact write should double: single=%v txn=%v", single.CapacityUnits, txn.CapacityUnits)
	}
}

func TestForTransactRead_DoublesRCU(t *testing.T) {
	single := ForRead(ModeTotal, "t", "", "", 100, true)
	txn := ForTransactRead(ModeTotal, "t", 100)

	if txn.CapacityUnits != 2*single.CapacityUnits {
		t.Fatalf("transact read should double: single=%v txn=%v", single.CapacityUnits, txn.CapacityUnits)
	}
}
