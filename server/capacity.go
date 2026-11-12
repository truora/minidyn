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

	if c.Breakdown {
		applyWireBreakdown(cc, c)
	}

	return cc
}

// applyWireBreakdown renders the INDEXES-level per-table/index breakdown. DynamoDB always reports
// the Table sub-object (even when an index served the request) plus an entry per affected index.
func applyWireBreakdown(cc *ConsumedCapacity, c *capacity.Consumed) {
	table := wireCapacityEntry(c.TableUnits)
	cc.Table = &table

	for _, idx := range c.Indexes {
		entry := wireCapacityEntry(idx.Units)

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

// wireCapacityEntry builds a single Capacity sub-object carrying the given units. DynamoDB Local reports only
// CapacityUnits there, and the ConsumedCapacity reference marks the read and write fields optional.
func wireCapacityEntry(units float64) Capacity {
	return Capacity{CapacityUnits: aws.Float64(units)}
}

// wireConsumedSlice assembles the per-table ConsumedCapacity slice of batch and transactional reads from the
// units aggregated per table, sorted by table name.
func wireConsumedSlice(mode capacity.Mode, unitsByTable map[string]float64) []ConsumedCapacity {
	return wireConsumedList(capacity.ConsumedByTable(mode, unitsByTable))
}
