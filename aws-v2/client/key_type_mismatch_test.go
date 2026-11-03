package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const keyTypeMismatchMsg = "ValidationException: One or more parameter values were invalid: Type mismatch for key"

func TestKeyTypeMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wrongKey := map[string]dynamodbtypes.AttributeValue{
		"id": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
	}

	ops := map[string]func(c FakeClient) error{
		"PutItem": func(c FakeClient) error {
			_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: wrongKey})
			return err
		},
		"GetItem": func(c FakeClient) error {
			_, err := c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tableName), Key: wrongKey})
			return err
		},
		"DeleteItem": func(c FakeClient) error {
			_, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(tableName), Key: wrongKey})
			return err
		},
		"UpdateItem": func(c FakeClient) error {
			_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:        aws.String(tableName),
				Key:              wrongKey,
				UpdateExpression: aws.String("SET #n = :n"),
				ExpressionAttributeNames: map[string]string{
					"#n": "name",
				},
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":n": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
				},
			})
			return err
		},
		"BatchGetItem": func(c FakeClient) error {
			_, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
					tableName: {Keys: []map[string]dynamodbtypes.AttributeValue{wrongKey}},
				},
			})
			return err
		},
		"BatchWriteItemPut": func(c FakeClient) error {
			_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodbtypes.WriteRequest{
					tableName: {{PutRequest: &dynamodbtypes.PutRequest{Item: wrongKey}}},
				},
			})
			return err
		},
		"BatchWriteItemDelete": func(c FakeClient) error {
			_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodbtypes.WriteRequest{
					tableName: {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: wrongKey}}},
				},
			})
			return err
		},
		"TransactWriteItemsPut": func(c FakeClient) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{
					{Put: &dynamodbtypes.Put{TableName: aws.String(tableName), Item: wrongKey}},
				},
			})
			return err
		},
		"TransactGetItems": func(c FakeClient) error {
			_, err := c.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
				TransactItems: []dynamodbtypes.TransactGetItem{
					{Get: &dynamodbtypes.Get{TableName: aws.String(tableName), Key: wrongKey}},
				},
			})
			return err
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := setupClient(tableName)
			require.NoError(t, ensurePokemonTable(client))

			err := op(client)
			require.Error(t, err)
			require.Contains(t, err.Error(), keyTypeMismatchMsg)
		})
	}
}
