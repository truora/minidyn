package client

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// A parallel Scan (#155) reads one segment: the segments are disjoint and together hold the
// whole table. Which segment holds which item is not specified.
func TestScanSegments(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	const items = 12

	for i := range items {
		c.NoError(createPokemon(client, pokemon{ID: fmt.Sprintf("%03d", i), Type: "grass", Name: fmt.Sprintf("p%d", i)}))
	}

	const total = 4

	var all []string

	for segment := range int32(total) {
		var start map[string]dynamodbtypes.AttributeValue

		for pages := 0; ; pages++ {
			c.Less(pages, 50)

			out, err := client.Scan(ctx, &dynamodb.ScanInput{
				TableName:         aws.String(tableName),
				Segment:           aws.Int32(segment),
				TotalSegments:     aws.Int32(total),
				Limit:             aws.Int32(2),
				ExclusiveStartKey: start,
			})
			c.NoError(err)
			c.LessOrEqual(len(out.Items), 2)

			for _, it := range out.Items {
				all = append(all, it["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
			}

			if len(out.LastEvaluatedKey) == 0 {
				break
			}

			start = out.LastEvaluatedKey
		}
	}

	c.Len(all, items)
	slices.Sort(all)
	c.Len(slices.Compact(all), items, "each item in exactly one segment")
}

func TestScanSegmentsValidation(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

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
		_, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName), Segment: tt.segment, TotalSegments: tt.total})
		c.ErrorContains(err, tt.want, tt.name)
		c.ErrorContains(err, "ValidationException", tt.name)
	}
}
