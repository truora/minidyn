package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// DynamoDB validates ReturnConsumedCapacity against its enum before anything else, on every
// operation that accepts it, and matches the value case sensitively.
func TestServerReturnConsumedCapacityOutsideEnumIsRejected(t *testing.T) {
	ctx := context.Background()
	table := aws.String("pokemons")

	ops := map[string]func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error{
		"GetItem": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.GetItem(ctx, &GetItemInput{TableName: table, Key: idKey("001"), ReturnConsumedCapacity: mode})

			return err
		},
		"PutItem": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.PutItem(ctx, &PutItemInput{TableName: table, Item: bigPokemonItem("new", 1), ReturnConsumedCapacity: mode})

			return err
		},
		"UpdateItem": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.UpdateItem(ctx, &UpdateItemInput{
				TableName: table, Key: idKey("001"), UpdateExpression: aws.String("SET #n = :n"),
				ExpressionAttributeNames:  map[string]string{"#n": "name"},
				ExpressionAttributeValues: map[string]*AttributeValue{":n": {S: aws.String("s")}},
				ReturnConsumedCapacity:    mode,
			})

			return err
		},
		"DeleteItem": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.DeleteItem(ctx, &DeleteItemInput{TableName: table, Key: idKey("001"), ReturnConsumedCapacity: mode})

			return err
		},
		"Query": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.Query(ctx, &QueryInput{
				TableName: table, KeyConditionExpression: aws.String("id = :id"),
				ExpressionAttributeValues: map[string]*AttributeValue{":id": {S: aws.String("001")}},
				ReturnConsumedCapacity:    mode,
			})

			return err
		},
		"Scan": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.Scan(ctx, &ScanInput{TableName: table, ReturnConsumedCapacity: mode})

			return err
		},
		"BatchGetItem": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.BatchGetItem(ctx, &BatchGetItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems:           map[string]KeysAndAttributes{"pokemons": {Keys: []map[string]*AttributeValue{idKey("001")}}},
			})

			return err
		},
		"BatchWriteItem": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.BatchWriteItem(ctx, &BatchWriteItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems:           map[string][]WriteRequest{"pokemons": {{PutRequest: &PutRequest{Item: bigPokemonItem("new", 1)}}}},
			})

			return err
		},
		"TransactGetItems": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.TransactGetItems(ctx, &TransactGetItemsInput{
				ReturnConsumedCapacity: mode,
				TransactItems:          []TransactGetItem{{Get: &Get{TableName: table, Key: idKey("001")}}},
			})

			return err
		},
		"TransactWriteItems": func(client *Client, mode ddbtypes.ReturnConsumedCapacity) error {
			_, err := client.TransactWriteItems(ctx, &TransactWriteItemsInput{
				ReturnConsumedCapacity: mode,
				TransactItems:          []TransactWriteItem{{Put: &Put{TableName: table, Item: bigPokemonItem("new", 1)}}},
			})

			return err
		},
	}

	for name, op := range ops {
		for _, mode := range []ddbtypes.ReturnConsumedCapacity{"FOO", "total"} {
			t.Run(name+"/"+string(mode), func(t *testing.T) {
				c := require.New(t)
				client := newServerWithPokemonTable(t)
				putBigPokemonItem(t, client, "001", 1)

				err := op(client, mode)
				c.Error(err)

				var apiErr smithy.APIError

				c.ErrorAs(err, &apiErr)
				c.Equal("ValidationException", apiErr.ErrorCode())
				c.Equal("1 validation error detected: Value '"+string(mode)+"' at 'returnConsumedCapacity' failed to "+
					"satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]", apiErr.ErrorMessage())
			})
		}
	}
}
