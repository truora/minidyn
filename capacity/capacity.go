package capacity

import "math"

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

// ParseMode maps a ReturnConsumedCapacity string ("", "NONE", "TOTAL", "INDEXES") to a Mode.
// Unset and any unknown value map to ModeNone.
func ParseMode(s string) Mode {
	switch s {
	case string(ModeTotal):
		return ModeTotal
	case string(ModeIndexes):
		return ModeIndexes
	default:
		return ModeNone
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
		u := WriteUnits(iw.Size)
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

// ForTransactRead builds the consumed capacity for a transactional read (2x, strongly consistent).
func ForTransactRead(mode Mode, table string, size int) *Consumed {
	return forUnits(mode, table, 2*ReadUnits(size, true), true)
}

// ForTransactWrite builds the consumed capacity for a transactional write (2x).
func ForTransactWrite(mode Mode, table string, size int) *Consumed {
	return forUnits(mode, table, 2*WriteUnits(size), false)
}
