package capacity

import (
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/types"
)

// TableLookup finds a table by name and returns nil when there is none. The caller of the
// functions below holds whatever lock protects the tables.
type TableLookup func(name string) *core.Table

// IndexWrites returns the per-index write footprint of a write that takes the item from before to
// after: for each secondary index the write changes, how it is billed to that index. Feeds the
// per-index ConsumedCapacity breakdown at the INDEXES level, and the total at every level.
func IndexWrites(table *core.Table, before, after map[string]*types.Item) []IndexWrite {
	deltas := table.WriteIndexDeltas(before, after)
	if len(deltas) == 0 {
		return nil
	}

	out := make([]IndexWrite, 0, len(deltas))
	for _, d := range deltas {
		out = append(out, IndexWriteFor(d.Name, d.Kind, d.Before, d.After, d.KeyChanged))
	}

	return out
}

// BillWrite returns the base size and the per-index footprint a write to table is billed for (see
// WriteBilling and IndexWrites). A nil table is billed by size alone.
func BillWrite(table *core.Table, key, before, after map[string]*types.Item) (int, []IndexWrite) {
	size := WriteBilling(key, before, after)
	if table == nil {
		return size, nil
	}

	return size, IndexWrites(table, before, after)
}

// ForItemWrite builds the consumed capacity of a single-item write to table: before is the item as
// measured ahead of the write and after the item it left (nil for a delete).
func ForItemWrite(mode Mode, table *core.Table, name string, key, before, after map[string]*types.Item) *Consumed {
	if mode == ModeNone {
		return nil
	}

	size, indexes := BillWrite(table, key, before, after)

	return ForWrite(mode, name, size, indexes)
}

// WriteKind is the kind of a write whose capacity is billed after the fact.
type WriteKind int

const (
	// WritePut writes Key, a whole item, over any item stored under the same key.
	WritePut WriteKind = iota
	// WriteUpdate changes the item stored under Key.
	WriteUpdate
	// WriteDelete removes the item stored under Key.
	WriteDelete
	// WriteCheck only checks a condition on Key and is billed by the key.
	WriteCheck
)

// WriteTarget is one write of a batch or transactional request: the table, what it writes or
// removes and, once measured, the item stored before. Measure it with Snapshot before the write
// is applied and bill it with Bill afterwards, because DynamoDB bills a write by the larger of the
// item before and after it, and a delete by the item it removed.
type WriteTarget struct {
	Table string
	Kind  WriteKind
	// Key is the item's key, or the whole item for a WritePut.
	Key    map[string]*types.Item
	before map[string]*types.Item
}

// Snapshot measures the item stored under the target's key. It must run before the write.
func (t *WriteTarget) Snapshot(tables TableLookup) {
	if t.Kind == WriteCheck {
		return
	}

	if table := tables(t.Table); table != nil {
		t.before = table.StoredItem(t.Key)
	}
}

// Bill returns the base size and per-index footprint of the write once it has been applied.
func (t *WriteTarget) Bill(tables TableLookup) (int, []IndexWrite) {
	table := tables(t.Table)

	var after map[string]*types.Item

	switch t.Kind {
	case WritePut:
		after = t.Key
	case WriteUpdate:
		if table != nil {
			after = table.StoredItem(t.Key)
		}
	case WriteDelete, WriteCheck:
	}

	return BillWrite(table, t.Key, t.before, after)
}
