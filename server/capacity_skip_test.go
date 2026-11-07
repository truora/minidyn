package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/capacity"
)

// Sizing an item converts it to a document and walks it, so a read that reports no consumed
// capacity must not do it. capacity.SizeCalls counts the items sized.
func TestServerReadsSkipSizingWhenConsumedCapacityIsNotReturned(t *testing.T) {
	const items = 40

	client := newServerWithPokemonTable(t)
	table := aws.String("pokemons")

	keys := make([]map[string]*AttributeValue, 0, items)

	for i := range items {
		id := fmt.Sprintf("item-%02d", i)
		putBigPokemonItem(t, client, id, 50)

		keys = append(keys, idKey(id))
	}

	ctx := context.Background()

	ops := map[string]func(mode ddbtypes.ReturnConsumedCapacity){
		"GetItem": func(mode ddbtypes.ReturnConsumedCapacity) {
			_, _ = client.GetItem(ctx, &GetItemInput{TableName: table, Key: keys[0], ReturnConsumedCapacity: mode})
		},
		"Scan": func(mode ddbtypes.ReturnConsumedCapacity) {
			_, _ = client.Scan(ctx, &ScanInput{TableName: table, ReturnConsumedCapacity: mode})
		},
		"Query": func(mode ddbtypes.ReturnConsumedCapacity) {
			_, _ = client.Query(ctx, &QueryInput{
				TableName: table, KeyConditionExpression: aws.String("id = :id"),
				ExpressionAttributeValues: map[string]*AttributeValue{":id": {S: aws.String("item-00")}},
				ReturnConsumedCapacity:    mode,
			})
		},
		"BatchGetItem": func(mode ddbtypes.ReturnConsumedCapacity) {
			_, _ = client.BatchGetItem(ctx, &BatchGetItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems:           map[string]KeysAndAttributes{"pokemons": {Keys: keys}},
			})
		},
		"TransactGetItems": func(mode ddbtypes.ReturnConsumedCapacity) {
			gets := make([]TransactGetItem, 0, len(keys))
			for _, k := range keys {
				gets = append(gets, TransactGetItem{Get: &Get{TableName: table, Key: k}})
			}

			_, _ = client.TransactGetItems(ctx, &TransactGetItemsInput{ReturnConsumedCapacity: mode, TransactItems: gets})
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			sized := func(mode ddbtypes.ReturnConsumedCapacity) int64 {
				before := capacity.SizeCalls()
				op(mode)

				return capacity.SizeCalls() - before
			}

			require.Positive(t, sized(ddbtypes.ReturnConsumedCapacityTotal), "TOTAL must size the items it reports")

			for _, mode := range []ddbtypes.ReturnConsumedCapacity{"", ddbtypes.ReturnConsumedCapacityNone} {
				require.Zero(t, sized(mode), "mode %q must not size any item", mode)
			}
		})
	}
}
