package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An ExclusiveStartKey does not have to be an existing item (#258).
func TestExclusiveStartKeyOfMissingItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))
	c.NoError(ensurePokemonTypeIndex(client))

	for _, p := range []pokemon{
		{ID: "001", Type: "grass", Name: "Bulbasaur"},
		{ID: "003", Type: "grass", Name: "Venusaur"},
		{ID: "005", Type: "grass", Name: "Charmeleon"},
	} {
		c.NoError(createPokemon(client, p))
	}

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	ids := func(items []map[string]dynamodbtypes.AttributeValue) []string {
		out := []string{}
		for _, it := range items {
			out = append(out, it["id"].(*dynamodbtypes.AttributeValueMemberS).Value)
		}

		return out
	}

	scan, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:         aws.String(tableName),
		ExclusiveStartKey: map[string]dynamodbtypes.AttributeValue{"id": str("002")},
	})
	c.NoError(err)
	c.Equal([]string{"003", "005"}, ids(scan.Items))

	for forward, want := range map[bool][]string{true: {"003", "005"}, false: {"001"}} {
		out, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(tableName),
			IndexName:                 aws.String("by-type"),
			KeyConditionExpression:    aws.String("#t = :t"),
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":t": str("grass")},
			ExclusiveStartKey:         map[string]dynamodbtypes.AttributeValue{"id": str("002"), "type": str("grass")},
			ScanIndexForward:          aws.Bool(forward),
		})
		c.NoError(err)
		c.Equal(want, ids(out.Items))
	}
}
