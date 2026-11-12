package client

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/core"
)

// tableOrNil is the capacity.TableLookup of the client's tables. The caller must hold fd.mu.
func (fd *Client) tableOrNil(name string) *core.Table {
	table, err := fd.getTable(name)
	if err != nil {
		return nil
	}

	return table
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

	if c.Breakdown {
		applySDKBreakdown(cc, c)
	}

	return cc
}

// applySDKBreakdown renders the INDEXES-level per-table/index breakdown. DynamoDB always reports
// the Table sub-object (even when an index served the request) plus an entry per affected index.
func applySDKBreakdown(cc *types.ConsumedCapacity, c *capacity.Consumed) {
	table := sdkCapacityEntry(c.TableUnits)
	cc.Table = &table

	for _, idx := range c.Indexes {
		entry := sdkCapacityEntry(idx.Units)

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

// sdkCapacityEntry builds a single Capacity sub-object carrying the given units. DynamoDB Local reports only
// CapacityUnits there, and the ConsumedCapacity reference marks the read and write fields optional.
func sdkCapacityEntry(units float64) types.Capacity {
	return types.Capacity{CapacityUnits: aws.Float64(units)}
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

// sdkConsumedSlice assembles the per-table ConsumedCapacity slice of batch and transactional reads from the
// units aggregated per table, sorted by table name.
func sdkConsumedSlice(mode capacity.Mode, unitsByTable map[string]float64) []types.ConsumedCapacity {
	return sdkConsumedList(capacity.ConsumedByTable(mode, unitsByTable))
}
