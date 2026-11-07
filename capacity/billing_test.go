package capacity

import (
	"strings"
	"testing"

	"github.com/truora/minidyn/types"
)

func billingItem(id string, pad int) map[string]*types.Item {
	return map[string]*types.Item{
		"id":   {S: &id},
		"name": {S: new(strings.Repeat("x", pad))},
	}
}

func TestWriteBilling(t *testing.T) {
	key := billingItem("k", 0)
	big := billingItem("k", 5000)
	small := billingItem("k", 10)

	tests := []struct {
		name     string
		before   map[string]*types.Item
		after    map[string]*types.Item
		wantSize int
	}{
		{"delete bills the deleted item", big, nil, Size(big)},
		{"update that shrinks bills the item before", big, small, Size(big)},
		{"update that grows bills the item after", small, big, Size(big)},
		{"put of a new item bills the item written", nil, big, Size(big)},
		{"delete of a missing item bills the key", nil, nil, Size(key)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if size := WriteBilling(key, tt.before, tt.after); size != tt.wantSize {
				t.Errorf("size = %d, want %d", size, tt.wantSize)
			}
		})
	}
}

func TestIndexWriteFor(t *testing.T) {
	small := billingItem("k", 10)
	big := billingItem("k", 5000)

	tests := []struct {
		name       string
		before     map[string]*types.Item
		after      map[string]*types.Item
		keyChanged bool
		want       IndexWrite
	}{
		{"entering the index bills the new entry", nil, big, false, IndexWrite{Name: "i", Kind: "GSI", Size: Size(big)}},
		{"leaving the index bills the old entry", big, nil, false, IndexWrite{Name: "i", Kind: "GSI", Size: Size(big)}},
		{"rewriting an entry bills the larger of the two", big, small, false, IndexWrite{Name: "i", Kind: "GSI", Size: Size(big)}},
		{
			"changing the index key deletes the old entry and writes the new", small, big, true,
			IndexWrite{Name: "i", Kind: "GSI", Size: Size(big), ReplacedSize: Size(small)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IndexWriteFor("i", "GSI", tt.before, tt.after, tt.keyChanged); got != tt.want {
				t.Errorf("IndexWriteFor() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestForWriteChargesTheReplacedIndexEntry(t *testing.T) {
	c := ForWrite(ModeIndexes, "t", 100, []IndexWrite{{Name: "i", Kind: "GSI", Size: 100, ReplacedSize: 2000}})

	// base 1 + index (1 for the new entry + 2 for the deleted one)
	if c.CapacityUnits != 4 || c.TableUnits != 1 || len(c.Indexes) != 1 || c.Indexes[0].Units != 3 {
		t.Fatalf("unexpected consumed capacity: %#v", c)
	}

	a := NewWriteAccumulator()
	a.AddTransact("t", 100, []IndexWrite{{Name: "i", Kind: "GSI", Size: 100, ReplacedSize: 2000}})

	if got := a.Consumed(ModeIndexes)[0]; got.CapacityUnits != 8 || got.Indexes[0].Units != 6 {
		t.Fatalf("unexpected accumulated capacity: %#v", got)
	}
}

func TestSizeCallsCountsSizedItems(t *testing.T) {
	before := SizeCalls()

	Size(billingItem("k", 1))
	SumSize([]map[string]*types.Item{billingItem("a", 1), billingItem("b", 1)})

	if got := SizeCalls() - before; got != 3 {
		t.Fatalf("SizeCalls advanced by %d, want 3", got)
	}
}
