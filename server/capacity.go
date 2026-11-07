package server

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/core"
)

// tableOrNil is the capacity.TableLookup of the client's tables. The caller must hold c.mu.
func (c *Client) tableOrNil(name string) *core.Table {
	table, err := c.getTable(name)
	if err != nil {
		return nil
	}

	return table
}

// snapshotLocked measures a write target's stored item, for callers that do not hold c.mu.
func (c *Client) snapshotLocked(t *capacity.WriteTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t.Snapshot(c.tableOrNil)
}

// billLocked bills a write target once applied, for callers that do not hold c.mu.
func (c *Client) billLocked(t *capacity.WriteTarget) (int, []capacity.IndexWrite) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return t.Bill(c.tableOrNil)
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

// wireConsumedSlice assembles the per-table ConsumedCapacity slice of batch and transactional reads from the
// units aggregated per table, sorted by table name.
func wireConsumedSlice(mode capacity.Mode, unitsByTable map[string]float64, isRead bool) []ConsumedCapacity {
	return wireConsumedList(capacity.ConsumedByTable(mode, unitsByTable, isRead))
}
