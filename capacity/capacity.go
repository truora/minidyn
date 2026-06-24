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

// Consumed is a client-agnostic consumed-capacity result. Each client maps it to its own
// output shape. Breakdown is set for ModeIndexes; IndexName/IndexKind identify the index
// charged for an index read (otherwise capacity is attributed to the base table).
type Consumed struct {
	TableName          string
	CapacityUnits      float64
	ReadCapacityUnits  float64
	WriteCapacityUnits float64
	Breakdown          bool
	IndexName          string
	IndexKind          string // "GSI" | "LSI" | ""
}

func build(mode Mode, table, index, indexKind string, units float64, isRead bool) *Consumed {
	if mode == ModeNone {
		return nil
	}

	c := &Consumed{
		TableName:     table,
		CapacityUnits: units,
		Breakdown:     mode == ModeIndexes,
	}

	if isRead {
		c.ReadCapacityUnits = units
	} else {
		c.WriteCapacityUnits = units
	}

	if mode == ModeIndexes && index != "" {
		c.IndexName = index
		c.IndexKind = indexKind
	}

	return c
}

// ForRead builds the consumed capacity for a read of total byte size from a table or index.
func ForRead(mode Mode, table, index, indexKind string, size int, consistent bool) *Consumed {
	return build(mode, table, index, indexKind, ReadUnits(size, consistent), true)
}

// ForWrite builds the consumed capacity for a single write of the given byte size.
func ForWrite(mode Mode, table string, size int) *Consumed {
	return build(mode, table, "", "", WriteUnits(size), false)
}

// ForReadUnits builds read consumed capacity from already-aggregated units. Used by batch
// reads, which sum per-item rounded units across a table.
func ForReadUnits(mode Mode, table string, units float64) *Consumed {
	return build(mode, table, "", "", units, true)
}

// ForWriteUnits builds write consumed capacity from already-aggregated units. Used by batch
// and transactional writes, which sum per-item rounded units across a table.
func ForWriteUnits(mode Mode, table string, units float64) *Consumed {
	return build(mode, table, "", "", units, false)
}

// ForTransactRead builds the consumed capacity for a transactional read (2x, strongly consistent).
func ForTransactRead(mode Mode, table string, size int) *Consumed {
	return build(mode, table, "", "", 2*ReadUnits(size, true), true)
}

// ForTransactWrite builds the consumed capacity for a transactional write (2x).
func ForTransactWrite(mode Mode, table string, size int) *Consumed {
	return build(mode, table, "", "", 2*WriteUnits(size), false)
}
