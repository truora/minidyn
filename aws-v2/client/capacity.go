package client

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/capacity"
)

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

func applySDKBreakdown(cc *types.ConsumedCapacity, c *capacity.Consumed) {
	entry := types.Capacity{CapacityUnits: aws.Float64(c.CapacityUnits)}

	if c.ReadCapacityUnits != 0 {
		entry.ReadCapacityUnits = aws.Float64(c.ReadCapacityUnits)
	}

	if c.WriteCapacityUnits != 0 {
		entry.WriteCapacityUnits = aws.Float64(c.WriteCapacityUnits)
	}

	switch {
	case c.IndexName != "" && c.IndexKind == "GSI":
		cc.GlobalSecondaryIndexes = map[string]types.Capacity{c.IndexName: entry}
	case c.IndexName != "" && c.IndexKind == "LSI":
		cc.LocalSecondaryIndexes = map[string]types.Capacity{c.IndexName: entry}
	default:
		cc.Table = &entry
	}
}

// consumedMode resolves the request's ReturnConsumedCapacity enum into a capacity.Mode.
func consumedMode(s string) capacity.Mode {
	return capacity.ParseMode(s)
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
