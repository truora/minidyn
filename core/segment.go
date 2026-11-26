package core

import (
	"fmt"
	"hash/fnv"

	"github.com/truora/minidyn/types"
)

// A parallel Scan (#155) reads one of TotalSegments disjoint parts of the table. DynamoDB does
// not document how items are assigned to a segment, only that the segments together cover the
// table and that a segment is a set of whole partitions. DynamoDB Local's assignment is its
// own and cannot be reproduced (D1, level 3): minidyn hashes the encoded partition key of the
// table, modulo TotalSegments. An item therefore lands in the same segment whatever the index
// the Scan reads, the items of one partition key stay together, and each item is read by
// exactly one segment. The parity tests compare the union and the disjointness of the
// segments, never which segment holds which item.

// maxTotalSegments is the largest TotalSegments DynamoDB accepts.
const maxTotalSegments = 1000000

const (
	segmentWithoutTotalMessage = "If segment is specified, total segment must also be specified"
	totalWithoutSegmentMessage = "If total segments is specified, a particular segment must also be specified for this request"
	startKeyOutsideSegment     = "Exclusive start key must lie within the segment"
)

// segmentRange is a validated Segment and TotalSegments pair.
type segmentRange struct {
	segment int64
	total   int64
}

// validateSegment checks Segment and TotalSegments of a Scan in the order DynamoDB Local does,
// which is after the start key checks (#171) and the Select check but before the projection of
// a GSI (see validateSearch). A Query ignores both fields. Each field needs the other; Segment
// must lie in [0, TotalSegments-1] (Local words a negative Segment like one above the range);
// and TotalSegments is at most maxTotalSegments. Then the ExclusiveStartKey must lie in the
// segment. It returns the pair, or nil for an unsegmented request.
func (t *Table) validateSegment(in QueryInput) (*segmentRange, error) {
	if !in.Scan {
		return nil, nil
	}

	r, err := in.segmentRange()
	if err != nil || r == nil {
		return nil, err
	}

	return r, t.validateStartKeySegment(r, in.ExclusiveStartKey)
}

func (in QueryInput) segmentRange() (*segmentRange, error) {
	switch {
	case in.Segment == nil && in.TotalSegments == nil:
		return nil, nil
	case in.TotalSegments == nil:
		return nil, validationError(segmentWithoutTotalMessage, "segment", fmt.Sprint(*in.Segment))
	case in.Segment == nil:
		return nil, validationError(totalWithoutSegmentMessage, "total segments", fmt.Sprint(*in.TotalSegments))
	}

	segment, total := int64(*in.Segment), int64(*in.TotalSegments)

	if segment < 0 || segment > total-1 {
		return nil, segmentConstraintError("segment", segment, total-1)
	}

	if total > maxTotalSegments {
		return nil, segmentConstraintError("totalSegments", total, maxTotalSegments)
	}

	return &segmentRange{segment: segment, total: total}, nil
}

func segmentConstraintError(attr string, value, maxValue int64) error {
	return validationError(fmt.Sprintf(
		"1 validation error detected: Value '%d' at '%s' failed to satisfy constraint: Member must have value less than or equal to %d",
		value, attr, maxValue))
}

// outsideSegment reports whether the item stored under the encoded key pk is in another
// segment than r. An unsegmented search (nil r) leaves no item outside.
func (t *Table) outsideSegment(r *segmentRange, pk string) bool {
	return r != nil && !t.segmentContains(r, t.Data[pk])
}

// segmentContains reports whether item, an item of the table, belongs to the segment. An item
// whose partition key cannot be encoded belongs to no segment.
func (t *Table) segmentContains(r *segmentRange, item map[string]*types.Item) bool {
	got, ok := t.segmentOf(r.total, item)

	return ok && got == r.segment
}

// segmentOf returns the segment, out of total, that the partition key of item hashes to. It
// reports false when the partition key cannot be encoded.
func (t *Table) segmentOf(total int64, item map[string]*types.Item) (int64, bool) {
	partition, err := t.KeySchema.encodeHashKey(t.AttributesDef, item)
	if err != nil {
		return 0, false
	}

	h := fnv.New32a()
	_, _ = h.Write(partition)

	return int64(h.Sum32()) % total, true
}

// validateStartKeySegment rejects an ExclusiveStartKey of another segment, as DynamoDB Local
// does. The start key need not be an item, only name a partition of the segment. The start key
// has been validated by then (validateStartKey), so its partition key is present and typed.
func (t *Table) validateStartKeySegment(r *segmentRange, start map[string]*types.Item) error {
	if len(start) == 0 {
		return nil
	}

	if got, ok := t.segmentOf(r.total, start); ok && got != r.segment {
		return validationError(startKeyOutsideSegment,
			"segment", fmt.Sprint(r.segment), "total segments", fmt.Sprint(r.total), "start key segment", fmt.Sprint(got))
	}

	return nil
}
