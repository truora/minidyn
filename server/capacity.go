package server

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/types"
)

// consumedMode resolves the request's ReturnConsumedCapacity enum into a capacity.Mode.
func consumedMode(s string) capacity.Mode {
	return capacity.ParseMode(s)
}

// writeConsumed bills a single-item write to table: before is the item as measured ahead of the
// write and after the item it left (nil for a delete). See capacity.WriteBilling.
func writeConsumed(mode capacity.Mode, table *core.Table, name string, key, before, after map[string]*types.Item) *capacity.Consumed {
	if mode == capacity.ModeNone {
		return nil
	}

	size, indexed := capacity.WriteBilling(key, before, after)

	return capacity.ForWrite(mode, name, size, indexWrites(table, indexed))
}

// storedItem returns a copy of the item stored under the key of keyOrItem in the named table, or
// nil when the table or item is absent. The caller must hold c.mu.
func (c *Client) storedItem(tableName string, keyOrItem map[string]*types.Item) map[string]*types.Item {
	table, err := c.getTable(tableName)
	if err != nil {
		return nil
	}

	return table.StoredItem(keyOrItem)
}

// storedItemLocked is storedItem for callers that do not hold c.mu.
func (c *Client) storedItemLocked(tableName string, keyOrItem map[string]*types.Item) map[string]*types.Item {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.storedItem(tableName, keyOrItem)
}

// billWrite returns the base size and per-index footprint of a write to the named table (see
// capacity.WriteBilling). The caller must hold c.mu.
func (c *Client) billWrite(tableName string, key, before, after map[string]*types.Item) (int, []capacity.IndexWrite) {
	size, indexed := capacity.WriteBilling(key, before, after)

	table, err := c.getTable(tableName)
	if err != nil {
		return size, nil
	}

	return size, indexWrites(table, indexed)
}

// billWriteLocked is billWrite for callers that do not hold c.mu.
func (c *Client) billWriteLocked(tableName string, key, before, after map[string]*types.Item) (int, []capacity.IndexWrite) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.billWrite(tableName, key, before, after)
}

// wireConsumedList maps a per-table Consumed list (e.g. a WriteAccumulator's output) into the
// server's JSON ConsumedCapacity slice. Returns nil when nothing is reported.
func wireConsumedList(cs []*capacity.Consumed) []ConsumedCapacity {
	if len(cs) == 0 {
		return nil
	}

	out := make([]ConsumedCapacity, 0, len(cs))
	for _, c := range cs {
		if cc := toWireConsumed(c); cc != nil {
			out = append(out, *cc)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// indexWrites returns the per-index write footprint of writing item to table: for each secondary
// index the write touches, the size of the item as projected into that index. Feeds the
// per-index ConsumedCapacity breakdown at the INDEXES level.
func indexWrites(table *core.Table, item map[string]*types.Item) []capacity.IndexWrite {
	projections := table.WriteIndexProjections(item)
	if len(projections) == 0 {
		return nil
	}

	out := make([]capacity.IndexWrite, 0, len(projections))
	for _, p := range projections {
		out = append(out, capacity.IndexWrite{Name: p.Name, Kind: p.Kind, Size: capacity.Size(p.Item)})
	}

	return out
}

// toWireConsumed maps a capacity.Consumed into the server's JSON ConsumedCapacity shape.
// It returns nil for a nil input so callers can assign it directly and omit the field.
func toWireConsumed(c *capacity.Consumed) *ConsumedCapacity {
	if c == nil {
		return nil
	}

	cc := &ConsumedCapacity{
		TableName:     c.TableName,
		CapacityUnits: aws.Float64(c.CapacityUnits),
	}

	if c.ReadCapacityUnits != 0 {
		cc.ReadCapacityUnits = aws.Float64(c.ReadCapacityUnits)
	}

	if c.WriteCapacityUnits != 0 {
		cc.WriteCapacityUnits = aws.Float64(c.WriteCapacityUnits)
	}

	if c.Breakdown {
		applyWireBreakdown(cc, c)
	}

	return cc
}

// applyWireBreakdown renders the INDEXES-level per-table/index breakdown. DynamoDB always reports
// the Table sub-object (even when an index served the request) plus an entry per affected index.
func applyWireBreakdown(cc *ConsumedCapacity, c *capacity.Consumed) {
	table := wireCapacityEntry(c, c.TableUnits)
	cc.Table = &table

	for _, idx := range c.Indexes {
		entry := wireCapacityEntry(c, idx.Units)

		switch idx.Kind {
		case "GSI":
			if cc.GlobalSecondaryIndexes == nil {
				cc.GlobalSecondaryIndexes = map[string]Capacity{}
			}

			cc.GlobalSecondaryIndexes[idx.Name] = entry
		case "LSI":
			if cc.LocalSecondaryIndexes == nil {
				cc.LocalSecondaryIndexes = map[string]Capacity{}
			}

			cc.LocalSecondaryIndexes[idx.Name] = entry
		}
	}
}

// wireCapacityEntry builds a single Capacity sub-object carrying the given units. Read/Write
// columns mirror whichever the operation reports, and only when non-zero.
func wireCapacityEntry(c *capacity.Consumed, units float64) Capacity {
	entry := Capacity{CapacityUnits: aws.Float64(units)}

	if c.ReadCapacityUnits != 0 && units != 0 {
		entry.ReadCapacityUnits = aws.Float64(units)
	}

	if c.WriteCapacityUnits != 0 && units != 0 {
		entry.WriteCapacityUnits = aws.Float64(units)
	}

	return entry
}

// wireConsumedSlice assembles the per-table ConsumedCapacity slice for batch and
// transactional operations from already-aggregated units. Zero-unit tables are omitted.
func wireConsumedSlice(mode capacity.Mode, unitsByTable map[string]float64, isRead bool) []ConsumedCapacity {
	if mode == capacity.ModeNone || len(unitsByTable) == 0 {
		return nil
	}

	out := make([]ConsumedCapacity, 0, len(unitsByTable))

	for table, units := range unitsByTable {
		if units == 0 {
			continue
		}

		var c *capacity.Consumed
		if isRead {
			c = capacity.ForReadUnits(mode, table, units)
		} else {
			c = capacity.ForWriteUnits(mode, table, units)
		}

		if cc := toWireConsumed(c); cc != nil {
			out = append(out, *cc)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
