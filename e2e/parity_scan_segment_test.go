package e2e

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Parallel Scan (#155). DynamoDB does not document how items are assigned to a segment and
// DynamoDB Local's assignment cannot be reproduced, so these cases never compare which segment
// holds which item (D1, level 3). They compare what both engines guarantee: the union of the
// segments is the table, each item is in exactly one segment, the items of a partition key stay
// together, paging and filters work inside a segment, and the validation messages.

const (
	paritySegmentTable      = "segments"
	paritySegmentIndex      = "by-status"
	paritySegmentKeysIndex  = "by-status-keys"
	paritySegmentPartitions = 10
	paritySegmentPerPart    = 3
)

func parityCreateSegmentTable(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(paritySegmentTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("customer"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("customer"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String(paritySegmentIndex),
			KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: dynamodbtypes.KeyTypeHash}},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}, {
			IndexName:  aws.String(paritySegmentKeysIndex),
			KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: dynamodbtypes.KeyTypeHash}},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeKeysOnly},
		}},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, paritySegmentTable, paritySegmentIndex)
	parityWaitUntilGSIActive(ctx, t, client, paritySegmentTable, paritySegmentKeysIndex)

	for i := range paritySegmentPartitions {
		for j := range paritySegmentPerPart {
			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(paritySegmentTable),
				Item: map[string]dynamodbtypes.AttributeValue{
					"customer": &dynamodbtypes.AttributeValueMemberS{Value: fmt.Sprintf("c%02d", i)},
					"sk":       &dynamodbtypes.AttributeValueMemberS{Value: fmt.Sprintf("s%d", j)},
					"status":   &dynamodbtypes.AttributeValueMemberS{Value: fmt.Sprintf("st%d", i%4)},
					"n":        &dynamodbtypes.AttributeValueMemberN{Value: fmt.Sprint(j)},
				},
			})
			require.NoError(t, err)
		}
	}
}

func paritySegmentLabel(it map[string]dynamodbtypes.AttributeValue) string {
	return parityAttrValueString(it["customer"]) + "/" + parityAttrValueString(it["sk"])
}

// parityScanSegments reads every segment of an n-way parallel scan to the end, in pages of
// limit items (0 for no limit), and returns the sorted union and the number of items read
// (ScannedCount) across all of them. It fails if an item is read twice, or if the items of a
// partition key are spread over several segments.
func parityScanSegments(
	ctx context.Context, t *testing.T, client *dynamodb.Client, base dynamodb.ScanInput, total, limit int32,
) ([]string, int32) {
	t.Helper()

	var (
		all     []string
		scanned int32
	)

	partitionSegment := map[string]int32{}

	for segment := range total {
		var start map[string]dynamodbtypes.AttributeValue

		for pages := 0; ; pages++ {
			require.Less(t, pages, 200, "paging must end")

			in := base
			in.TableName = aws.String(paritySegmentTable)
			in.Segment = aws.Int32(segment)
			in.TotalSegments = aws.Int32(total)
			in.ExclusiveStartKey = start

			if limit > 0 {
				in.Limit = aws.Int32(limit)
			}

			out, err := client.Scan(ctx, &in)
			require.NoError(t, err)

			if limit > 0 {
				require.LessOrEqual(t, out.ScannedCount, limit)
			}

			scanned += out.ScannedCount

			for _, it := range out.Items {
				all = append(all, paritySegmentLabel(it))

				customer := parityAttrValueString(it["customer"])
				prev, seen := partitionSegment[customer]
				require.True(t, !seen || prev == segment, "partition %s split over segments", customer)

				partitionSegment[customer] = segment
			}

			if len(out.LastEvaluatedKey) == 0 {
				break
			}

			start = out.LastEvaluatedKey
		}
	}

	sort.Strings(all)

	for i := 1; i < len(all); i++ {
		require.NotEqual(t, all[i-1], all[i], "item read by more than one segment")
	}

	return all, scanned
}

func TestE2E_ScanSegments(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "SegmentsAreTheTable",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				out := map[int32][]string{}

				for _, total := range []int32{1, 2, 3, 7, 25} {
					all, _ := parityScanSegments(ctx, t, client, dynamodb.ScanInput{}, total, 0)
					out[total] = all
				}

				return out
			},
		},
		{
			name: "PagingInsideASegment",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				all, scanned := parityScanSegments(ctx, t, client, dynamodb.ScanInput{}, 3, 2)

				return map[string]any{"items": all, "scanned": scanned}
			},
		},
		{
			name: "GSIScan",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				out := map[string]any{}

				for _, limit := range []int32{0, 2} {
					all, scanned := parityScanSegments(ctx, t, client,
						dynamodb.ScanInput{IndexName: aws.String(paritySegmentIndex)}, 3, limit)
					out[fmt.Sprintf("limit%d", limit)] = map[string]any{"items": all, "scanned": scanned}
				}

				return out
			},
		},
		{
			name: "FilterInsideASegment",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				all, scanned := parityScanSegments(ctx, t, client, dynamodb.ScanInput{
					FilterExpression:          aws.String("n = :zero"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":zero": &dynamodbtypes.AttributeValueMemberN{Value: "0"}},
				}, 3, 0)

				// The filter runs on the items the segments read, which together are the table.
				return map[string]any{"items": all, "scanned": scanned}
			},
		},
		{
			name: "SegmentSelectCount",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				var count int32

				for segment := range int32(3) {
					out, err := client.Scan(ctx, &dynamodb.ScanInput{
						TableName: aws.String(paritySegmentTable), Select: dynamodbtypes.SelectCount,
						Segment: aws.Int32(segment), TotalSegments: aws.Int32(3),
					})
					require.NoError(t, err)

					count += out.Count
				}

				return count
			},
		},
		{
			name: "StartKeyOfAnotherSegment",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				scan := func(segment int32) *dynamodb.ScanOutput {
					out, err := client.Scan(ctx, &dynamodb.ScanInput{
						TableName: aws.String(paritySegmentTable),
						Segment:   aws.Int32(segment), TotalSegments: aws.Int32(2),
					})
					require.NoError(t, err)

					return out
				}

				// The table has 30 items, so both segments hold some, whatever the assignment.
				seg, outs := int32(0), []*dynamodb.ScanOutput{scan(0), scan(1)}
				require.NotEmpty(t, outs[0].Items)
				require.NotEmpty(t, outs[1].Items)

				inSegment, other := outs[seg], outs[1-seg]

				// A start key that lies in the segment reads on after it.
				it := inSegment.Items[0]
				_, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(paritySegmentTable),
					Segment:   aws.Int32(seg), TotalSegments: aws.Int32(2),
					ExclusiveStartKey: map[string]dynamodbtypes.AttributeValue{"customer": it["customer"], "sk": it["sk"]},
				})
				require.NoError(t, err)

				// A start key of the other segment is rejected.
				foreign := other.Items[0]
				_, err = client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(paritySegmentTable),
					Segment:   aws.Int32(seg), TotalSegments: aws.Int32(2),
					ExclusiveStartKey: map[string]dynamodbtypes.AttributeValue{"customer": foreign["customer"], "sk": foreign["sk"]},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			},
		},
		{
			name: "Validation",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateSegmentTable(ctx, t, client)

				type req struct {
					name           string
					segment, total *int32
					mutate         func(*dynamodb.ScanInput)
				}

				i32 := aws.Int32
				none := func(*dynamodb.ScanInput) {}

				reqs := []req{
					{"SegmentWithoutTotal", i32(0), nil, none},
					{"NegativeSegmentWithoutTotal", i32(-1), nil, none},
					{"TotalWithoutSegment", nil, i32(2), none},
					{"ZeroTotalWithoutSegment", nil, i32(0), none},
					{"TooManyTotalWithoutSegment", nil, i32(1000001), none},
					{"SegmentEqualToTotal", i32(2), i32(2), none},
					{"SegmentAboveTotal", i32(5), i32(2), none},
					{"NegativeSegment", i32(-1), i32(2), none},
					{"NegativeSegmentAndTotal", i32(-9), i32(-5), none},
					{"ZeroTotal", i32(0), i32(0), none},
					{"NegativeTotal", i32(0), i32(-1), none},
					{"TooManySegments", i32(0), i32(1000001), none},
					{"SegmentBeforeTotal", i32(-1), i32(1000001), none},
					{"MaxSegments", i32(999999), i32(1000000), none},
					{"UnknownIndexFirst", i32(5), i32(2), func(in *dynamodb.ScanInput) { in.IndexName = aws.String("nope") }},
					{"ConsistentReadOnGSIFirst", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.IndexName = aws.String(paritySegmentIndex)
						in.ConsistentRead = aws.Bool(true)
					}},
					{"UndefinedValueFirst", i32(5), i32(2), func(in *dynamodb.ScanInput) { in.FilterExpression = aws.String("n = :x") }},
					{"UndeclaredNameFirst", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.FilterExpression = aws.String("#a = :x")
						in.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{":x": &dynamodbtypes.AttributeValueMemberS{Value: "x"}}
					}},
					{"SelectFirst", i32(5), i32(2), func(in *dynamodb.ScanInput) { in.Select = dynamodbtypes.SelectAllProjectedAttributes }},
					{"InvalidNumberInStartKeyBeforeSegment", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.ExclusiveStartKey = map[string]dynamodbtypes.AttributeValue{
							"customer": &dynamodbtypes.AttributeValueMemberN{Value: "abc"},
							"sk":       &dynamodbtypes.AttributeValueMemberS{Value: "s0"},
						}
					}},
					{"StartKeyMissingRangeKeyBeforeSegment", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.ExclusiveStartKey = map[string]dynamodbtypes.AttributeValue{
							"customer": &dynamodbtypes.AttributeValueMemberS{Value: "c00"},
						}
					}},
					{"WrongHashKeyTypeInStartKeyBeforeSegment", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.ExclusiveStartKey = map[string]dynamodbtypes.AttributeValue{
							"customer": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
							"sk":       &dynamodbtypes.AttributeValueMemberS{Value: "s0"},
						}
					}},
					{"UnprojectedAttributeOnGSIBeforeSegment", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.IndexName = aws.String(paritySegmentKeysIndex)
						in.ProjectionExpression = aws.String("n")
					}},
					{"UnprojectedFilterAttributeOnGSIBeforeSegment", i32(5), i32(2), func(in *dynamodb.ScanInput) {
						in.IndexName = aws.String(paritySegmentKeysIndex)
						in.FilterExpression = aws.String("n = :x")
						in.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{":x": &dynamodbtypes.AttributeValueMemberN{Value: "1"}}
					}},
					{"UnusedValueFirst", nil, i32(2), func(in *dynamodb.ScanInput) {
						in.ExpressionAttributeValues = map[string]dynamodbtypes.AttributeValue{":x": &dynamodbtypes.AttributeValueMemberS{Value: "x"}}
					}},
				}

				out := map[string]string{}

				for _, r := range reqs {
					in := &dynamodb.ScanInput{TableName: aws.String(paritySegmentTable), Segment: r.segment, TotalSegments: r.total}
					r.mutate(in)

					_, err := client.Scan(ctx, in)
					if err != nil {
						out[r.name] = normalizeSDKErrorString(err.Error())
					} else {
						out[r.name] = "ok"
					}
				}

				return out
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
