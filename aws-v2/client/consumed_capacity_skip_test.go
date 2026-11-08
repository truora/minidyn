package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/capacity"
)

// Sizing an item converts it to a document and walks it, so a read that reports no consumed
// capacity must not do it. capacity.SizeCalls counts the items sized.
func TestReadsSkipSizingWhenConsumedCapacityIsNotReturned(t *testing.T) {
	const items = 40

	client := setupCapacityClient(t)

	keys := make([]map[string]dynamodbtypes.AttributeValue, 0, items)

	for i := range items {
		id := fmt.Sprintf("item-%02d", i)
		putBigPokemon(t, client, id, 50)

		keys = append(keys, keyID(id))
	}

	ctx := context.Background()

	ops := map[string]func(mode dynamodbtypes.ReturnConsumedCapacity){
		"GetItem": func(mode dynamodbtypes.ReturnConsumedCapacity) {
			_, _ = client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tableName), Key: keys[0], ReturnConsumedCapacity: mode})
		},
		"Scan": func(mode dynamodbtypes.ReturnConsumedCapacity) {
			_, _ = client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName), ReturnConsumedCapacity: mode})
		},
		"Query": func(mode dynamodbtypes.ReturnConsumedCapacity) {
			_, _ = client.Query(ctx, &dynamodb.QueryInput{
				TableName:                aws.String(tableName),
				KeyConditionExpression:   aws.String("#id = :id"),
				ExpressionAttributeNames: map[string]string{"#id": "id"},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":id": &dynamodbtypes.AttributeValueMemberS{Value: "item-00"},
				},
				ReturnConsumedCapacity: mode,
			})
		},
		"BatchGetItem": func(mode dynamodbtypes.ReturnConsumedCapacity) {
			_, _ = client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems:           map[string]dynamodbtypes.KeysAndAttributes{tableName: {Keys: keys}},
			})
		},
		"TransactGetItems": func(mode dynamodbtypes.ReturnConsumedCapacity) {
			gets := make([]dynamodbtypes.TransactGetItem, 0, len(keys))
			for _, k := range keys {
				gets = append(gets, dynamodbtypes.TransactGetItem{Get: &dynamodbtypes.Get{TableName: aws.String(tableName), Key: k}})
			}

			_, _ = client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{ReturnConsumedCapacity: mode, TransactItems: gets})
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			sized := func(mode dynamodbtypes.ReturnConsumedCapacity) int64 {
				before := capacity.SizeCalls()
				op(mode)

				return capacity.SizeCalls() - before
			}

			require.Positive(t, sized(dynamodbtypes.ReturnConsumedCapacityTotal), "TOTAL must size the items it reports")

			for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{"", dynamodbtypes.ReturnConsumedCapacityNone} {
				require.Zero(t, sized(mode), "mode %q must not size any item", mode)
			}
		})
	}
}
