package capacity

import (
	"fmt"
	"math"
	"sort"

	"github.com/truora/minidyn/types"
)

const (
	readBlockBytes  = 4096
	writeBlockBytes = 1024
)

// Mode is the resolved ReturnConsumedCapacity level.
type Mode string

const (
	// ModeNone means no ConsumedCapacity should be reported.
	ModeNone Mode = "NONE"
	// ModeTotal reports table-level totals only.
	ModeTotal Mode = "TOTAL"
	// ModeIndexes reports totals plus a per-table/index breakdown.
	ModeIndexes Mode = "INDEXES"
)

// ParseMode maps a ReturnConsumedCapacity string ("", "NONE", "TOTAL", "INDEXES") to a Mode. An
// unset value is NONE. Like DynamoDB it matches case sensitively and rejects anything else with
// a ValidationException, so callers validate before doing any work.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "", string(ModeNone):
		return ModeNone, nil
	case string(ModeTotal):
		return ModeTotal, nil
	case string(ModeIndexes):
		return ModeIndexes, nil
	default:
		return ModeNone, types.NewError("ValidationException", fmt.Sprintf(
			"1 validation error detected: Value '%s' at 'returnConsumedCapacity' failed to satisfy constraint: "+
				"Member must satisfy enum value set: [INDEXES, TOTAL, NONE]", s), nil)
	}
}

// ReadUnits returns the read capacity units for an item/result of the given byte size.
// One unit covers a 4KB strongly consistent read; eventually consistent reads cost half.
func ReadUnits(size int, consistent bool) float64 {
	blocks := math.Ceil(float64(size) / readBlockBytes)
	if blocks < 1 {
		blocks = 1
	}

	if consistent {
		return blocks
	}

	return blocks / 2
}

// WriteUnits returns the write capacity units for an item of the given byte size.
// One unit covers a 1KB write.
func WriteUnits(size int) float64 {
	blocks := math.Ceil(float64(size) / writeBlockBytes)
	if blocks < 1 {
		blocks = 1
	}

	return blocks
}

// IndexUnits is the capacity charged to one secondary index in an INDEXES-level breakdown.
type IndexUnits struct {
	Name  string
	Kind  string // "GSI" | "LSI"
	Units float64
}

// IndexWrite is the byte size of a write as it lands in one secondary index (the item projected
// into the index). Used to derive per-index write units for the INDEXES breakdown.
type IndexWrite struct {
	Name string
	Kind string // "GSI" | "LSI"
	Size int
	// ReplacedSize is the size of the entry deleted when the write changes the item's index key
	// (a second write to the index); zero otherwise.
	ReplacedSize int
}

// units returns the write capacity units the write costs the index.
func (w IndexWrite) units() float64 {
	u := WriteUnits(w.Size)
	if w.ReplacedSize > 0 {
		u += WriteUnits(w.ReplacedSize)
	}

	return u
}

// IndexWriteFor returns how a write from the item before to the item after (the item as projected
// into the index, nil when it is not indexed there) is billed to one secondary index. Entering the
// index writes the new entry, leaving it deletes the old one, rewriting an entry bills the larger
// of the two, and changing the index key (keyChanged) deletes the old entry and writes the new.
func IndexWriteFor(name, kind string, before, after map[string]*types.Item, keyChanged bool) IndexWrite {
	w := IndexWrite{Name: name, Kind: kind}

	switch {
	case len(before) == 0:
		w.Size = Size(after)
	case len(after) == 0:
		w.Size = Size(before)
	case keyChanged:
		w.Size, w.ReplacedSize = Size(after), Size(before)
	default:
		w.Size = max(Size(before), Size(after))
	}

	return w
}

// Consumed is a client-agnostic consumed-capacity result. Each client maps it to its own output
// shape. When Breakdown is set (ModeIndexes), DynamoDB reports a per-table/index breakdown:
// TableUnits is the base-table sub-object's units (always present at the INDEXES level, even when
// an index served the request) and Indexes carries the per-index entries.
type Consumed struct {
	TableName          string
	CapacityUnits      float64
	ReadCapacityUnits  float64
	WriteCapacityUnits float64
	Breakdown          bool
	TableUnits         float64
	Indexes            []IndexUnits
}

func newConsumed(mode Mode, table string, total float64, isRead bool) *Consumed {
	if mode == ModeNone {
		return nil
	}

	c := &Consumed{
		TableName:     table,
		CapacityUnits: total,
		Breakdown:     mode == ModeIndexes,
	}

	if isRead {
		c.ReadCapacityUnits = total
	} else {
		c.WriteCapacityUnits = total
	}

	return c
}

// ForRead builds the consumed capacity for a read of total byte size served by a table or a
// secondary index. For an index read the capacity is attributed to the index; the base-table
// sub-object is still reported (with zero units) because DynamoDB always includes it at the
// INDEXES level.
func ForRead(mode Mode, table, index, indexKind string, size int, consistent bool) *Consumed {
	units := ReadUnits(size, consistent)

	c := newConsumed(mode, table, units, true)
	if c == nil || !c.Breakdown {
		return c
	}

	if index == "" {
		c.TableUnits = units
		return c
	}

	c.Indexes = []IndexUnits{{Name: index, Kind: indexKind, Units: units}}

	return c
}

// ForWrite builds the consumed capacity for a single write of the given base item size, plus the
// per-index write capacity for each secondary index the write touches. The total always includes
// the index cost (DynamoDB charges every affected index, even at the TOTAL level); the per-index
// breakdown is only surfaced at the INDEXES level.
func ForWrite(mode Mode, table string, size int, indexes []IndexWrite) *Consumed {
	tableUnits := WriteUnits(size)
	total := tableUnits

	entries := make([]IndexUnits, 0, len(indexes))

	for _, iw := range indexes {
		u := iw.units()
		total += u
		entries = append(entries, IndexUnits{Name: iw.Name, Kind: iw.Kind, Units: u})
	}

	c := newConsumed(mode, table, total, false)
	if c == nil || !c.Breakdown {
		return c
	}

	c.TableUnits = tableUnits
	if len(entries) > 0 {
		c.Indexes = entries
	}

	return c
}

// ForReadUnits builds read consumed capacity from already-aggregated units. Used by batch
// reads, which sum per-item rounded units across a table.
func ForReadUnits(mode Mode, table string, units float64) *Consumed {
	return forUnits(mode, table, units, true)
}

// ForWriteUnits builds write consumed capacity from already-aggregated units. Used by batch
// and transactional writes, which sum per-item rounded units across a table.
func ForWriteUnits(mode Mode, table string, units float64) *Consumed {
	return forUnits(mode, table, units, false)
}

// forUnits builds a table-level Consumed from already-aggregated units. At the INDEXES level the
// units are attributed to the base-table sub-object; batch and transactional operations report
// table totals only, not a per-index breakdown.
func forUnits(mode Mode, table string, units float64, isRead bool) *Consumed {
	c := newConsumed(mode, table, units, isRead)
	if c != nil && c.Breakdown {
		c.TableUnits = units
	}

	return c
}

// transactFactor is how much a transactional read or write costs compared with the plain one.
const transactFactor = 2

// TransactReadUnits returns the read capacity units of one item in a TransactGetItems call:
// transactional reads are strongly consistent and cost twice a plain read.
func TransactReadUnits(size int) float64 {
	return transactFactor * ReadUnits(size, true)
}

// WriteAccumulator aggregates write capacity per table and per secondary index across the
// sub-requests of a batch or transactional write, so the INDEXES breakdown can report a
// per-index split (which single-item writes derive directly).
type WriteAccumulator struct {
	tables map[string]*tableWrite
}

type tableWrite struct {
	table   float64
	order   []string
	indexes map[string]*IndexUnits
}

// NewWriteAccumulator returns an empty WriteAccumulator.
func NewWriteAccumulator() *WriteAccumulator {
	return &WriteAccumulator{tables: map[string]*tableWrite{}}
}

// Add records a single write to table: its base item size plus the per-index write footprint.
// factor scales the units (1 for batch writes).
func (a *WriteAccumulator) Add(table string, size int, indexes []IndexWrite, factor float64) {
	tw, ok := a.tables[table]
	if !ok {
		tw = &tableWrite{indexes: map[string]*IndexUnits{}}
		a.tables[table] = tw
	}

	tw.table += factor * WriteUnits(size)

	for _, iw := range indexes {
		e, ok := tw.indexes[iw.Name]
		if !ok {
			e = &IndexUnits{Name: iw.Name, Kind: iw.Kind}
			tw.indexes[iw.Name] = e
			tw.order = append(tw.order, iw.Name)
		}

		e.Units += factor * iw.units()
	}
}

// AddTransact records one write of a TransactWriteItems call, which costs twice a plain write.
func (a *WriteAccumulator) AddTransact(table string, size int, indexes []IndexWrite) {
	a.Add(table, size, indexes, transactFactor)
}

// Consumed builds the per-table Consumed list for the given mode: nil for ModeNone or when
// nothing was charged, table-sorted otherwise, with zero-unit tables omitted. At the INDEXES
// level each entry carries the base-table units and the aggregated per-index entries.
func (a *WriteAccumulator) Consumed(mode Mode) []*Consumed {
	if mode == ModeNone || len(a.tables) == 0 {
		return nil
	}

	names := make([]string, 0, len(a.tables))
	for name := range a.tables {
		names = append(names, name)
	}

	sort.Strings(names)

	out := make([]*Consumed, 0, len(names))

	for _, table := range names {
		if c := a.tables[table].consumed(mode, table); c != nil {
			out = append(out, c)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (tw *tableWrite) consumed(mode Mode, table string) *Consumed {
	total := tw.table
	for _, name := range tw.order {
		total += tw.indexes[name].Units
	}

	if total == 0 {
		return nil
	}

	c := newConsumed(mode, table, total, false)
	if !c.Breakdown {
		return c
	}

	c.TableUnits = tw.table

	if len(tw.order) > 0 {
		entries := make([]IndexUnits, 0, len(tw.order))
		for _, name := range tw.order {
			entries = append(entries, *tw.indexes[name])
		}

		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		c.Indexes = entries
	}

	return c
}

// WriteBilling returns the base item size a write is billed for. DynamoDB bills a write by the
// larger of the item before and after it (a delete has no item after, so it bills the deleted
// item). Before and after must be measured around the write, key is the fallback size for a delete
// of a missing item.
func WriteBilling(key, before, after map[string]*types.Item) int {
	if size := max(Size(before), Size(after)); size > 0 {
		return size
	}

	return Size(key)
}
