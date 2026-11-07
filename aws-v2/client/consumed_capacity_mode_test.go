package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// DynamoDB validates ReturnConsumedCapacity against its enum before anything else, on every
// operation that accepts it, and matches the value case sensitively.
func TestReturnConsumedCapacityOutsideEnumIsRejected(t *testing.T) {
	ops := map[string]func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error{
		"GetItem": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
				TableName: aws.String(tableName), Key: keyID("001"), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"PutItem": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName), Item: bigPokemon("new", 1), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"UpdateItem": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			u := setNameUpdate(1)
			_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
				TableName: u.TableName, Key: keyID("001"), UpdateExpression: u.UpdateExpression,
				ExpressionAttributeNames: u.ExpressionAttributeNames, ExpressionAttributeValues: u.ExpressionAttributeValues,
				ReturnConsumedCapacity: mode,
			})

			return err
		},
		"DeleteItem": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
				TableName: aws.String(tableName), Key: keyID("001"), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"Query": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.Query(context.Background(), &dynamodb.QueryInput{
				TableName:                aws.String(tableName),
				KeyConditionExpression:   aws.String("#id = :id"),
				ExpressionAttributeNames: map[string]string{"#id": "id"},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":id": &dynamodbtypes.AttributeValueMemberS{Value: "001"},
				},
				ReturnConsumedCapacity: mode,
			})

			return err
		},
		"Scan": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.Scan(context.Background(), &dynamodb.ScanInput{
				TableName: aws.String(tableName), ReturnConsumedCapacity: mode,
			})

			return err
		},
		"BatchGetItem": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
					tableName: {Keys: []map[string]dynamodbtypes.AttributeValue{keyID("001")}},
				},
			})

			return err
		},
		"BatchWriteItem": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				ReturnConsumedCapacity: mode,
				RequestItems: map[string][]dynamodbtypes.WriteRequest{
					tableName: {{PutRequest: &dynamodbtypes.PutRequest{Item: bigPokemon("new", 1)}}},
				},
			})

			return err
		},
		"TransactGetItems": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
				ReturnConsumedCapacity: mode,
				TransactItems: []dynamodbtypes.TransactGetItem{
					{Get: &dynamodbtypes.Get{TableName: aws.String(tableName), Key: keyID("001")}},
				},
			})

			return err
		},
		"TransactWriteItems": func(client *Client, mode dynamodbtypes.ReturnConsumedCapacity) error {
			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				ReturnConsumedCapacity: mode,
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{Put: &dynamodbtypes.Put{TableName: aws.String(tableName), Item: bigPokemon("new", 1)}},
				},
			})

			return err
		},
	}

	for name, op := range ops {
		for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{"FOO", "total"} {
			t.Run(name+"/"+string(mode), func(t *testing.T) {
				c := require.New(t)
				client := setupCapacityClient(t)

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

func TestReturnConsumedCapacityRejectedBeforeWriting(t *testing.T) {
	c := require.New(t)
	client := setupCapacityClient(t)

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName), Item: bigPokemon("new", 1), ReturnConsumedCapacity: "FOO",
	})
	c.Error(err)

	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{TableName: aws.String(tableName), Key: keyID("new")})
	c.NoError(err)
	c.Empty(out.Item)
}
