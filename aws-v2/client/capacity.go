package client

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/core"
	mtypes "github.com/truora/minidyn/types"
)

// indexWrites returns the per-index write footprint of writing item to table: for each secondary
// index the write touches, the size of the item as projected into that index. Feeds the
// per-index ConsumedCapacity breakdown at the INDEXES level.
func indexWrites(table *core.Table, item map[string]*mtypes.Item) []capacity.IndexWrite {
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

// toSDKConsumed maps a capacity.Consumed into the AWS SDK ConsumedCapacity shape.
// It returns nil for a nil input (ReturnConsumedCapacity=NONE/unset), so callers can
// assign the result directly and leave the field omitted.
func toSDKConsumed(c *capacity.Consumed) *types.ConsumedCapacity {
	if c == nil {
		return nil
	}

	cc := &types.ConsumedCapacity{
		TableName:     aws.String(c.TableName),
		CapacityUnits: aws.Float64(c.CapacityUnits),
	}

	if c.ReadCapacityUnits != 0 {
		cc.ReadCapacityUnits = aws.Float64(c.ReadCapacityUnits)
	}

	if c.WriteCapacityUnits != 0 {
		cc.WriteCapacityUnits = aws.Float64(c.WriteCapacityUnits)
	}

	if c.Breakdown {
		applySDKBreakdown(cc, c)
	}

	return cc
}

// applySDKBreakdown renders the INDEXES-level per-table/index breakdown. DynamoDB always reports
// the Table sub-object (even when an index served the request) plus an entry per affected index.
func applySDKBreakdown(cc *types.ConsumedCapacity, c *capacity.Consumed) {
	table := sdkCapacityEntry(c, c.TableUnits)
	cc.Table = &table

	for _, idx := range c.Indexes {
		entry := sdkCapacityEntry(c, idx.Units)

		switch idx.Kind {
		case "GSI":
			if cc.GlobalSecondaryIndexes == nil {
				cc.GlobalSecondaryIndexes = map[string]types.Capacity{}
			}

			cc.GlobalSecondaryIndexes[idx.Name] = entry
		case "LSI":
			if cc.LocalSecondaryIndexes == nil {
				cc.LocalSecondaryIndexes = map[string]types.Capacity{}
			}

			cc.LocalSecondaryIndexes[idx.Name] = entry
		}
	}
}

// sdkCapacityEntry builds a single Capacity sub-object carrying the given units. Read/Write
// columns mirror whichever the operation reports, and only when non-zero.
func sdkCapacityEntry(c *capacity.Consumed, units float64) types.Capacity {
	entry := types.Capacity{CapacityUnits: aws.Float64(units)}

	if c.ReadCapacityUnits != 0 && units != 0 {
		entry.ReadCapacityUnits = aws.Float64(units)
	}

	if c.WriteCapacityUnits != 0 && units != 0 {
		entry.WriteCapacityUnits = aws.Float64(units)
	}

	return entry
}

// consumedMode resolves the request's ReturnConsumedCapacity enum into a capacity.Mode.
func consumedMode(s string) capacity.Mode {
	return capacity.ParseMode(s)
}

// sdkConsumedList maps a per-table Consumed list (e.g. a WriteAccumulator's output) into the SDK
// ConsumedCapacity slice. Returns nil when nothing is reported.
func sdkConsumedList(cs []*capacity.Consumed) []types.ConsumedCapacity {
	if len(cs) == 0 {
		return nil
	}

	out := make([]types.ConsumedCapacity, 0, len(cs))
	for _, c := range cs {
		if cc := toSDKConsumed(c); cc != nil {
			out = append(out, *cc)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// sdkConsumedSlice assembles the per-table ConsumedCapacity slice used by batch and
// transactional operations from already-aggregated units. Tables with zero units are
// omitted; nil is returned when nothing is reported (NONE/unset or empty).
func sdkConsumedSlice(mode capacity.Mode, unitsByTable map[string]float64, isRead bool) []types.ConsumedCapacity {
	if mode == capacity.ModeNone || len(unitsByTable) == 0 {
		return nil
	}

	out := make([]types.ConsumedCapacity, 0, len(unitsByTable))

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

		if cc := toSDKConsumed(c); cc != nil {
			out = append(out, *cc)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
