package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestScanAndQueryCounts(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))
	c.NoError(ensurePokemonTypeIndex(client))

	for _, p := range []pokemon{
		{ID: "001", Type: "grass", Name: "Bulbasaur"},
		{ID: "002", Type: "grass", Name: "Ivysaur"},
		{ID: "003", Type: "grass", Name: "Venusaur"},
		{ID: "004", Type: "fire", Name: "Charmander"},
	} {
		c.NoError(createPokemon(client, p))
	}

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	scan := func(mutate func(*dynamodb.ScanInput)) *dynamodb.ScanOutput {
		in := &dynamodb.ScanInput{
			TableName:                aws.String(tableName),
			FilterExpression:         aws.String("#n = :n"),
			ExpressionAttributeNames: map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":n": str("Venusaur"),
			},
		}
		mutate(in)

		out, err := client.Scan(ctx, in)
		c.NoError(err)

		return out
	}

	out := scan(func(*dynamodb.ScanInput) {})
	c.Equal([]int32{1, 4}, []int32{out.Count, out.ScannedCount})
	c.Len(out.Items, 1)

	out = scan(func(in *dynamodb.ScanInput) { in.Select = dynamodbtypes.SelectCount })
	c.Equal([]int32{1, 4}, []int32{out.Count, out.ScannedCount})
	c.Empty(out.Items)

	out = scan(func(in *dynamodb.ScanInput) { in.Limit = aws.Int32(2) })
	c.Equal([]int32{0, 2}, []int32{out.Count, out.ScannedCount})
	c.NotNil(out.Items)
	c.NotEmpty(out.LastEvaluatedKey)

	out = scan(func(in *dynamodb.ScanInput) { in.ExclusiveStartKey = out.LastEvaluatedKey })
	c.Equal([]int32{1, 2}, []int32{out.Count, out.ScannedCount})

	query := func(mutate func(*dynamodb.QueryInput)) *dynamodb.QueryOutput {
		in := &dynamodb.QueryInput{
			TableName:                aws.String(tableName),
			IndexName:                aws.String("by-type"),
			KeyConditionExpression:   aws.String("#t = :t"),
			FilterExpression:         aws.String("#n = :n"),
			ExpressionAttributeNames: map[string]string{"#t": "type", "#n": "name"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":t": str("grass"),
				":n": str("Ivysaur"),
			},
		}
		mutate(in)

		qout, err := client.Query(ctx, in)
		c.NoError(err)

		return qout
	}

	qout := query(func(*dynamodb.QueryInput) {})
	c.Equal([]int32{1, 3}, []int32{qout.Count, qout.ScannedCount})
	c.Len(qout.Items, 1)

	qout = query(func(in *dynamodb.QueryInput) { in.Limit = aws.Int32(1) })
	c.Equal([]int32{0, 1}, []int32{qout.Count, qout.ScannedCount})
	c.NotNil(qout.Items)

	qout = query(func(in *dynamodb.QueryInput) { in.Select = dynamodbtypes.SelectCount })
	c.Equal([]int32{1, 3}, []int32{qout.Count, qout.ScannedCount})
	c.Empty(qout.Items)

	qout = query(func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues[":t"] = str("water") })
	c.Equal([]int32{0, 0}, []int32{qout.Count, qout.ScannedCount})
	c.NotNil(qout.Items)
}
