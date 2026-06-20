package server

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/truora/minidyn/capacity"
)

// consumedMode resolves the request's ReturnConsumedCapacity enum into a capacity.Mode.
func consumedMode(s string) capacity.Mode {
	return capacity.ParseMode(s)
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

func applyWireBreakdown(cc *ConsumedCapacity, c *capacity.Consumed) {
	entry := Capacity{CapacityUnits: aws.Float64(c.CapacityUnits)}

	if c.ReadCapacityUnits != 0 {
		entry.ReadCapacityUnits = aws.Float64(c.ReadCapacityUnits)
	}

	if c.WriteCapacityUnits != 0 {
		entry.WriteCapacityUnits = aws.Float64(c.WriteCapacityUnits)
	}

	switch {
	case c.IndexName != "" && c.IndexKind == "GSI":
		cc.GlobalSecondaryIndexes = map[string]Capacity{c.IndexName: entry}
	case c.IndexName != "" && c.IndexKind == "LSI":
		cc.LocalSecondaryIndexes = map[string]Capacity{c.IndexName: entry}
	default:
		cc.Table = &entry
	}
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
