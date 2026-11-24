package core

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// A parallel Scan (#155) splits the table into TotalSegments disjoint parts and reads one of
// them. DynamoDB does not document how items are assigned to a segment, and DynamoDB Local's
// assignment cannot be reproduced, so these tests only rely on the properties both share:
// the segments partition the table, and the items of one partition key stay together.

const segmentTestPartitions = 20

// segmentTestTable returns table pk (S) / sk (S) with GSI "by-g" on g, holding three items
// for each of segmentTestPartitions partition keys.
func segmentTestTable(t *testing.T) *Table {
	t.Helper()

	table := NewTable("segments")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "S", "g": "S", "n": "N"}
	table.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	all := "ALL"
	require.NoError(t, table.AddIndexes(
		[]*types.GlobalSecondaryIndex{{
			IndexName:             aws.String("by-g"),
			Projection:            &types.Projection{ProjectionType: &all},
			ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			KeySchema:             []*types.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}},
		}},
		nil,
	))

	for i := range segmentTestPartitions {
		for j := range 3 {
			_, err := table.Put(&types.PutItemInput{TableName: aws.String("segments"), Item: map[string]*types.Item{
				"pk": {S: aws.String(fmt.Sprintf("p%02d", i))},
				"sk": {S: aws.String(fmt.Sprintf("s%d", j))},
				"g":  {S: aws.String(fmt.Sprintf("g%d", i%4))},
				"n":  {N: aws.String(fmt.Sprint(j))},
			}})
			require.NoError(t, err)
		}
	}

	return table
}

func segmentScan(index string, segment, total int32) QueryInput {
	return QueryInput{Scan: true, ScanIndexForward: true, Index: index, Segment: &segment, TotalSegments: &total}
}

// errMessage returns the DynamoDB message of err, without minidyn's detail after " | ".
func errMessage(t *testing.T, err error) string {
	t.Helper()

	var typed interface{ Message() string }

	require.ErrorAs(t, err, &typed)

	msg, _, _ := strings.Cut(typed.Message(), " | ")

	return msg
}

func itemLabels(items []map[string]*types.Item) []string {
	labels := make([]string, 0, len(items))

	for _, it := range items {
		labels = append(labels, *it["pk"].S+"/"+*it["sk"].S)
	}

	slices.Sort(labels)

	return labels
}

func TestSearchData_scanSegmentsPartitionTheTable(t *testing.T) {
	table := segmentTestTable(t)

	whole, err := table.SearchData(QueryInput{Scan: true, ScanIndexForward: true})
	require.NoError(t, err)
	require.Len(t, whole.Items, segmentTestPartitions*3)

	for _, index := range []string{"", "by-g"} {
		for _, total := range []int32{1, 2, 3, 7, 50} {
			var all []string

			partitionSegment := map[string]int32{}

			for segment := range total {
				res, serr := table.SearchData(segmentScan(index, segment, total))
				require.NoError(t, serr)
				require.Equal(t, int64(len(res.Items)), res.ScannedCount, "no filter: every item read is returned")

				all = append(all, itemLabels(res.Items)...)

				for _, it := range res.Items {
					prev, seen := partitionSegment[*it["pk"].S]
					require.True(t, !seen || prev == segment, "partition %s split over segments", *it["pk"].S)

					partitionSegment[*it["pk"].S] = segment
				}
			}

			slices.Sort(all)

			require.Equal(t, itemLabels(whole.Items), all, "index %q, %d segments: each item exactly once", index, total)
		}
	}
}

func TestSearchData_scanSegmentIsDeterministic(t *testing.T) {
	table := segmentTestTable(t)

	first, err := table.SearchData(segmentScan("", 1, 3))
	require.NoError(t, err)

	second, err := table.SearchData(segmentScan("", 1, 3))
	require.NoError(t, err)

	require.NotEmpty(t, first.Items)
	require.Equal(t, itemLabels(first.Items), itemLabels(second.Items))
}

func TestSearchData_scanSegmentPaging(t *testing.T) {
	table := segmentTestTable(t)

	for _, index := range []string{"", "by-g"} {
		for _, limit := range []int64{1, 2, 4} {
			for segment := range int32(3) {
				want, err := table.SearchData(segmentScan(index, segment, 3))
				require.NoError(t, err)

				var got []string

				var start map[string]*types.Item

				for pages := 0; ; pages++ {
					require.Less(t, pages, 100, "paging must end")

					in := segmentScan(index, segment, 3)
					in.Limit = limit
					in.ExclusiveStartKey = start

					res, serr := table.SearchData(in)
					require.NoError(t, serr)
					require.LessOrEqual(t, int64(len(res.Items)), limit)

					got = append(got, itemLabels(res.Items)...)

					if len(res.LastEvaluatedKey) == 0 {
						break
					}

					start = res.LastEvaluatedKey
				}

				slices.Sort(got)

				require.Equal(t, itemLabels(want.Items), got, "index %q, limit %d, segment %d", index, limit, segment)
			}
		}
	}
}

func TestSearchData_scanSegmentFilterCountsOnlyTheSegment(t *testing.T) {
	table := segmentTestTable(t)

	in := segmentScan("", 1, 3)
	in.FilterExpression = "n = :zero"
	in.ExpressionAttributeValues = map[string]*types.Item{":zero": {N: aws.String("0")}}

	res, err := table.SearchData(in)
	require.NoError(t, err)

	inSegment, err := table.SearchData(segmentScan("", 1, 3))
	require.NoError(t, err)

	require.Equal(t, int64(len(inSegment.Items)), res.ScannedCount)
	require.Len(t, res.Items, len(inSegment.Items)/3)
}

func TestSearchData_scanSegmentValidation(t *testing.T) {
	table := segmentTestTable(t)

	const (
		segmentWithoutTotal = "If segment is specified, total segment must also be specified"
		totalWithoutSegment = "If total segments is specified, a particular segment must also be specified for this request"
	)

	rangeError := func(attr string, value, max int32) string {
		return fmt.Sprintf("1 validation error detected: Value '%d' at '%s' failed to satisfy constraint: "+
			"Member must have value less than or equal to %d", value, attr, max)
	}

	tests := []struct {
		name          string
		segment       *int32
		totalSegments *int32
		want          string
	}{
		{"segment alone", aws.Int32(0), nil, segmentWithoutTotal},
		{"negative segment alone", aws.Int32(-1), nil, segmentWithoutTotal},
		{"total alone", nil, aws.Int32(2), totalWithoutSegment},
		{"zero total alone", nil, aws.Int32(0), totalWithoutSegment},
		{"too many total alone", nil, aws.Int32(1000001), totalWithoutSegment},
		{"segment equal to total", aws.Int32(2), aws.Int32(2), rangeError("segment", 2, 1)},
		{"segment above total", aws.Int32(5), aws.Int32(2), rangeError("segment", 5, 1)},
		{"negative segment", aws.Int32(-1), aws.Int32(2), rangeError("segment", -1, 1)},
		{"zero total", aws.Int32(0), aws.Int32(0), rangeError("segment", 0, -1)},
		{"negative total", aws.Int32(0), aws.Int32(-5), rangeError("segment", 0, -6)},
		{"too many segments", aws.Int32(0), aws.Int32(1000001), rangeError("totalSegments", 1000001, 1000000)},
		{"segment checked before total", aws.Int32(-1), aws.Int32(1000001), rangeError("segment", -1, 1000000)},
		{"last of the most segments", aws.Int32(999999), aws.Int32(1000000), ""},
		{"only segment", aws.Int32(0), aws.Int32(1), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := table.SearchData(QueryInput{Scan: true, ScanIndexForward: true, Segment: tt.segment, TotalSegments: tt.totalSegments})

			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Equal(t, tt.want, errMessage(t, err))
		})
	}
}

// DynamoDB Local reports the segment after every other check of the request.
func TestSearchData_scanSegmentValidatedAfterTheRequest(t *testing.T) {
	table := segmentTestTable(t)

	bad := aws.Int32(9)
	total := aws.Int32(2)

	tests := []struct {
		name string
		in   QueryInput
		want string
	}{
		{"unknown index", QueryInput{Index: "nope"}, "The table does not have the specified index: nope"},
		{"consistent read on a GSI", QueryInput{Index: "by-g", ConsistentRead: true}, "Consistent reads are not supported on global secondary indexes"},
		{"filter placeholder", QueryInput{FilterExpression: "n = :x"}, "Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :x"},
		{"select", QueryInput{Select: "ALL_PROJECTED_ATTRIBUTES"}, "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.in.Scan = true
			tt.in.ScanIndexForward = true
			tt.in.Segment = bad
			tt.in.TotalSegments = total

			_, err := table.SearchData(tt.in)
			require.Error(t, err)
			require.Equal(t, tt.want, errMessage(t, err))
		})
	}
}

// DynamoDB Local rejects a start key that belongs to another segment.
func TestSearchData_scanSegmentStartKeyOutsideTheSegment(t *testing.T) {
	table := segmentTestTable(t)

	first, err := table.SearchData(segmentScan("", 0, 3))
	require.NoError(t, err)
	require.NotEmpty(t, first.Items)

	other, err := table.SearchData(segmentScan("", 1, 3))
	require.NoError(t, err)
	require.NotEmpty(t, other.Items)

	foreign := segmentScan("", 0, 3)
	foreign.ExclusiveStartKey = map[string]*types.Item{"pk": other.Items[0]["pk"], "sk": other.Items[0]["sk"]}

	_, err = table.SearchData(foreign)
	require.Error(t, err)
	require.Equal(t, "Exclusive start key must lie within the segment", errMessage(t, err))

	// The start key need not be an item, only lie in the segment.
	inside := segmentScan("", 0, 3)
	inside.ExclusiveStartKey = map[string]*types.Item{"pk": first.Items[0]["pk"], "sk": {S: aws.String("zz")}}

	_, err = table.SearchData(inside)
	require.NoError(t, err)
}

// An entry of the table that has no usable partition key belongs to no segment, instead of
// failing the Scan.
func TestSearchData_scanSegmentSkipsEntryWithoutPartitionKey(t *testing.T) {
	table := segmentTestTable(t)

	table.Data["orphan"] = map[string]*types.Item{"sk": {S: aws.String("s0")}}
	table.SortedKeys = append(table.SortedKeys, "orphan")
	slices.Sort(table.SortedKeys)

	var all []string

	for segment := range int32(3) {
		res, err := table.SearchData(segmentScan("", segment, 3))
		require.NoError(t, err)

		all = append(all, itemLabels(res.Items)...)
	}

	require.Len(t, all, segmentTestPartitions*3)
}

// Users persist a LastEvaluatedKey and resume a segment later, so the segment a partition key
// hashes to is a contract: a change of hash function or key encoding must fail here.
func TestSearchData_scanSegmentGolden(t *testing.T) {
	table := segmentTestTable(t)

	tests := []struct {
		pk    string
		total int64
		want  int64
	}{
		{"p00", 3, 2}, {"p00", 7, 4}, {"p01", 3, 0}, {"p01", 7, 3}, {"p02", 3, 2}, {"p02", 7, 5},
	}

	for _, tt := range tests {
		got, ok := table.segmentOf(tt.total, map[string]*types.Item{"pk": {S: aws.String(tt.pk)}})
		require.True(t, ok)
		require.Equal(t, tt.want, got, "%s of %d", tt.pk, tt.total)
	}
}

func TestSearchData_scanSegmentErrorsCarryDetail(t *testing.T) {
	table := segmentTestTable(t)

	other, err := table.SearchData(segmentScan("", 1, 3))
	require.NoError(t, err)

	foreign := segmentScan("", 0, 3)
	foreign.ExclusiveStartKey = map[string]*types.Item{"pk": other.Items[0]["pk"], "sk": other.Items[0]["sk"]}

	segment, total := int32(4), int32(2)

	tests := []struct {
		name string
		in   QueryInput
		want string
	}{
		{"segment alone", QueryInput{Segment: &segment}, "segment: 4"},
		{"total alone", QueryInput{TotalSegments: &total}, "total segments: 2"},
		{"foreign start key", foreign, "segment: 0; total segments: 3; start key segment: 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.in.Scan, tt.in.ScanIndexForward = true, true

			_, err := table.SearchData(tt.in)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// DynamoDB Local checks the segment before the projection of a GSI scan, and the start key
// (#171) before the segment.
func TestSearchData_scanSegmentBeforeIndexProjection(t *testing.T) {
	table := segmentTestTable(t)

	keysOnly := "KEYS_ONLY"
	require.NoError(t, table.AddIndexes(
		[]*types.GlobalSecondaryIndex{{
			IndexName:             aws.String("keys"),
			Projection:            &types.Projection{ProjectionType: &keysOnly},
			ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			KeySchema:             []*types.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}},
		}},
		nil,
	))

	in := segmentScan("keys", 5, 2)
	in.ProjectionExpression = "n"

	_, err := table.SearchData(in)
	require.Error(t, err)
	require.Contains(t, errMessage(t, err), "failed to satisfy constraint")

	in.Segment = aws.Int32(0)

	_, err = table.SearchData(in)
	require.Error(t, err)
	require.Equal(t, "One or more parameter values were invalid: Global secondary index keys does not project [n]", errMessage(t, err))

	in = segmentScan("", 5, 2)
	in.ExclusiveStartKey = map[string]*types.Item{"pk": {S: aws.String("p00")}}

	_, err = table.SearchData(in)
	require.Error(t, err)
	require.Equal(t, "Exclusive Start Key must have same size as table's key schema", errMessage(t, err))
}

// DynamoDB Local ignores Segment and TotalSegments on a Query.
func TestSearchData_queryIgnoresSegment(t *testing.T) {
	table := segmentTestTable(t)

	in := QueryInput{
		ScanIndexForward:          true,
		KeyConditionExpression:    "pk = :pk",
		ExpressionAttributeValues: map[string]*types.Item{":pk": {S: aws.String("p00")}},
		Segment:                   aws.Int32(1),
		TotalSegments:             aws.Int32(3),
	}

	res, err := table.SearchData(in)
	require.NoError(t, err)
	require.Len(t, res.Items, 3)

	// Not even an invalid pair is reported.
	in.Segment = aws.Int32(9)

	res, err = table.SearchData(in)
	require.NoError(t, err)
	require.Len(t, res.Items, 3)
}

// A filter that rejects every item of a page still advances LastEvaluatedKey, and paging a
// segment with a filter and a Limit ends with the whole, unduplicated result.
func TestSearchData_scanSegmentFilterAndLimitPaging(t *testing.T) {
	table := segmentTestTable(t)

	filter := func(in QueryInput) QueryInput {
		in.FilterExpression = "sk = :sk"
		in.ExpressionAttributeValues = map[string]*types.Item{":sk": {S: aws.String("s1")}}

		return in
	}

	for _, index := range []string{"", "by-g"} {
		for segment := range int32(3) {
			want, err := table.SearchData(filter(segmentScan(index, segment, 3)))
			require.NoError(t, err)

			var (
				got         []string
				start       map[string]*types.Item
				emptyPages  int
				lastEvalKey map[string]*types.Item
			)

			for pages := 0; ; pages++ {
				require.Less(t, pages, 100, "paging must end")

				in := filter(segmentScan(index, segment, 3))
				in.Limit = 1
				in.ExclusiveStartKey = start

				res, serr := table.SearchData(in)
				require.NoError(t, serr)

				got = append(got, itemLabels(res.Items)...)

				if len(res.Items) == 0 && len(res.LastEvaluatedKey) > 0 {
					emptyPages++
				}

				if len(res.LastEvaluatedKey) == 0 {
					break
				}

				require.NotEqual(t, lastEvalKey, res.LastEvaluatedKey, "LastEvaluatedKey must advance")

				lastEvalKey = res.LastEvaluatedKey
				start = res.LastEvaluatedKey
			}

			slices.Sort(got)

			require.Equal(t, itemLabels(want.Items), got, "index %q, segment %d", index, segment)

			if len(want.Items) < 3*segmentTestPartitions/3 {
				require.Positive(t, emptyPages, "index %q, segment %d: some page filters out every item", index, segment)
			}
		}
	}
}

// On a GSI scan the segment is the one of the table's partition key, so a start key with the
// table and index keys of another segment's item is rejected.
func TestSearchData_scanSegmentGSIStartKeyOfAnotherSegment(t *testing.T) {
	table := segmentTestTable(t)

	other, err := table.SearchData(segmentScan("by-g", 1, 3))
	require.NoError(t, err)
	require.NotEmpty(t, other.Items)

	it := other.Items[0]

	in := segmentScan("by-g", 0, 3)
	in.ExclusiveStartKey = map[string]*types.Item{"pk": it["pk"], "sk": it["sk"], "g": it["g"]}

	_, err = table.SearchData(in)
	require.Error(t, err)
	require.Equal(t, "Exclusive start key must lie within the segment", errMessage(t, err))
}

// Every spelling of a number encodes the same, so an N partition key lands in one segment.
func TestSearchData_scanSegmentNumberPartitionKeySpellings(t *testing.T) {
	table := NewTable("numbers")
	table.AttributesDef = map[string]string{"id": "N"}
	table.KeySchema = keySchema{HashKey: "id"}

	for _, total := range []int64{2, 3, 7, 50} {
		a, ok := table.segmentOf(total, map[string]*types.Item{"id": {N: aws.String("1")}})
		require.True(t, ok)

		b, ok := table.segmentOf(total, map[string]*types.Item{"id": {N: aws.String("1.0")}})
		require.True(t, ok)

		require.Equal(t, a, b, "%d segments", total)
	}

	// The same holds for an ExclusiveStartKey: it is inside the segment of the item whichever
	// way the number is spelled.
	_, err := table.Put(&types.PutItemInput{TableName: aws.String("numbers"), Item: map[string]*types.Item{"id": {N: aws.String("1")}}})
	require.NoError(t, err)

	seg, ok := table.segmentOf(3, map[string]*types.Item{"id": {N: aws.String("1")}})
	require.True(t, ok)

	for _, spelling := range []string{"1", "1.0", "1.00"} {
		in := segmentScan("", int32(seg), 3)
		in.ExclusiveStartKey = map[string]*types.Item{"id": {N: aws.String(spelling)}}

		_, err = table.SearchData(in)
		require.NoError(t, err, spelling)

		in = segmentScan("", int32((seg+1)%3), 3)
		in.ExclusiveStartKey = map[string]*types.Item{"id": {N: aws.String(spelling)}}

		_, err = table.SearchData(in)
		require.Error(t, err, spelling)
		require.Equal(t, "Exclusive start key must lie within the segment", errMessage(t, err))
	}
}
