package server

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func newSegmentTestClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	ts := httptest.NewServer(NewServer())
	t.Cleanup(ts.Close)

	cli := newTestDynamoClient(t, ts.URL)

	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:            aws.String("orders"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("customer"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("customer"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return cli
}

// A parallel Scan (#155) reads one segment: the segments are disjoint and together hold the
// whole table. Which segment holds which item is not specified.
func TestServerScanSegments(t *testing.T) {
	cli := newSegmentTestClient(t)
	ctx := context.Background()

	const items = 12

	for i := range items {
		_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("orders"),
			Item:      map[string]ddbtypes.AttributeValue{"customer": &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("c%02d", i)}},
		})
		require.NoError(t, err)
	}

	const total = 4

	var all []string

	for segment := range int32(total) {
		var start map[string]ddbtypes.AttributeValue

		for pages := 0; ; pages++ {
			require.Less(t, pages, 50)

			out, err := cli.Scan(ctx, &dynamodb.ScanInput{
				TableName:         aws.String("orders"),
				Segment:           aws.Int32(segment),
				TotalSegments:     aws.Int32(total),
				Limit:             aws.Int32(2),
				ExclusiveStartKey: start,
			})
			require.NoError(t, err)
			require.LessOrEqual(t, len(out.Items), 2)

			for _, it := range out.Items {
				all = append(all, it["customer"].(*ddbtypes.AttributeValueMemberS).Value)
			}

			if len(out.LastEvaluatedKey) == 0 {
				break
			}

			start = out.LastEvaluatedKey
		}
	}

	require.Len(t, all, items)
	slices.Sort(all)
	require.Len(t, slices.Compact(all), items, "each item in exactly one segment")
}

func TestServerScanSegmentsValidation(t *testing.T) {
	cli := newSegmentTestClient(t)
	ctx := context.Background()

	for _, tt := range []struct {
		name           string
		segment, total *int32
		want           string
	}{
		{"segment alone", aws.Int32(0), nil, "If segment is specified, total segment must also be specified"},
		{"total alone", nil, aws.Int32(2), "If total segments is specified, a particular segment must also be specified for this request"},
		{"out of range", aws.Int32(2), aws.Int32(2), "Value '2' at 'segment' failed to satisfy constraint: Member must have value less than or equal to 1"},
		{"too many", aws.Int32(0), aws.Int32(1000001), "Value '1000001' at 'totalSegments' failed to satisfy constraint: Member must have value less than or equal to 1000000"},
	} {
		_, err := cli.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("orders"), Segment: tt.segment, TotalSegments: tt.total})
		require.ErrorContains(t, err, tt.want, tt.name)
		require.ErrorContains(t, err, "ValidationException", tt.name)
	}
}
