package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestSearchDataScannedCount(t *testing.T) {
	c := require.New(t)

	table, err := createPokemonTable()
	c.NoError(err)

	for _, p := range []pokemon{
		{ID: "001", Type: "grass", Name: "Bulbasaur"},
		{ID: "001", Type: "grass", Name: "Ivysaur"},
		{ID: "001", Type: "grass", Name: "Venusaur"},
		{ID: "002", Type: "fire", Name: "Charmander"},
	} {
		_, err = table.Put(&types.PutItemInput{Item: createPokemon(p), TableName: &table.Name})
		c.NoError(err)
	}

	query := func(limit int64, start map[string]*types.Item) SearchResult {
		res, qerr := table.SearchData(QueryInput{
			KeyConditionExpression: "id = :id",
			FilterExpression:       "#n = :n",
			Aliases:                map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]*types.Item{
				":id": {S: new("001")},
				":n":  {S: new("Venusaur")},
			},
			Limit:             limit,
			ExclusiveStartKey: start,
			ScanIndexForward:  true,
		})
		c.NoError(qerr)

		return res
	}

	// Only the 3 items of partition 001 are read; the filter keeps 1.
	res := query(0, nil)
	c.Len(res.Items, 1)
	c.Equal(int64(3), res.ScannedCount)

	// Limit bounds the items read, not the items returned.
	res = query(2, nil)
	c.Empty(res.Items)
	c.Equal(int64(2), res.ScannedCount)
	c.NotEmpty(res.LastEvaluatedKey)

	// Items before the start key are not read on this page.
	res = query(0, res.LastEvaluatedKey)
	c.Len(res.Items, 1)
	c.Equal(int64(1), res.ScannedCount)

	// A scan reads every item, whatever the filter keeps.
	scan, err := table.SearchData(QueryInput{
		Scan:             true,
		FilterExpression: "#n = :n",
		Aliases:          map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]*types.Item{
			":n": {S: new("Venusaur")},
		},
		ScanIndexForward: true,
	})
	c.NoError(err)
	c.Len(scan.Items, 1)
	c.Equal(int64(4), scan.ScannedCount)
}

// searchData unpacks a SearchResult for tests that only look at the items and the last key.
func searchData(table *Table, input QueryInput) ([]map[string]*types.Item, map[string]*types.Item, error) {
	res, err := table.SearchData(input)

	return res.Items, res.LastEvaluatedKey, err
}
