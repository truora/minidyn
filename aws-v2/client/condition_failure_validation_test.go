package client

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

const bogusModeMessage = "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"

func putPokemon(t *testing.T, client FakeClient, id, name string) {
	t.Helper()

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id":   &dynamodbtypes.AttributeValueMemberS{Value: id},
			"name": &dynamodbtypes.AttributeValueMemberS{Value: name},
		},
	})
	require.NoError(t, err)
}

func TestUpdateItemInvalidReturnValuesOnConditionCheckFailure(t *testing.T) {
	client := setupClient(tableName)

	require.NoError(t, ensurePokemonTable(client))
	putPokemon(t, client, "001", "Bulbasaur")

	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                           aws.String(tableName),
		Key:                                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
		UpdateExpression:                    aws.String("SET #n = :v"),
		ConditionExpression:                 aws.String("attribute_not_exists(id)"),
		ExpressionAttributeNames:            map[string]string{"#n": "name"},
		ExpressionAttributeValues:           map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"}},
		ReturnValuesOnConditionCheckFailure: "BOGUS",
	})
	require.ErrorContains(t, err, bogusModeMessage)
}

// DynamoDB Local accepts the mode inside a transaction; minidyn follows the API reference
// (Valid Values: ALL_OLD | NONE) and rejects the whole transaction before any write.
func TestTransactionInvalidReturnValuesOnConditionCheckFailureIsRejected(t *testing.T) {
	ctx := context.Background()
	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}
	const bogus = dynamodbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")
	cond := aws.String("attribute_not_exists(id)")

	items := map[string]dynamodbtypes.TransactWriteItem{
		"Put": {Put: &dynamodbtypes.Put{
			TableName: aws.String(tableName), Item: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
		}},
		"Delete": {Delete: &dynamodbtypes.Delete{
			TableName: aws.String(tableName), Key: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
		}},
		"Update": {Update: &dynamodbtypes.Update{
			TableName: aws.String(tableName), Key: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
			UpdateExpression:          aws.String("SET #n = :v"),
			ExpressionAttributeNames:  map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"}},
		}},
		"ConditionCheck": {ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(tableName), Key: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
		}},
	}

	shapes := map[string]string{"Put": "put", "Delete": "delete", "Update": "update", "ConditionCheck": "conditionCheck"}

	for name, item := range items {
		t.Run(name, func(t *testing.T) {
			client := setupClient(tableName)

			require.NoError(t, ensurePokemonTable(client))
			putPokemon(t, client, "001", "Bulbasaur")

			_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{item},
			})
			require.Error(t, err)

			_, isCancelled := errors.AsType[*dynamodbtypes.TransactionCanceledException](err)
			require.False(t, isCancelled, "%v", err)

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "ValidationException", apiErr.ErrorCode())
			require.Equal(t,
				"1 validation error detected: Value 'BOGUS' at 'transactItems.1.member."+shapes[name]+".returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
				apiErr.ErrorMessage())

			// Rejected before any write: the stored item is untouched.
			got, gerr := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tableName), Key: key})
			require.NoError(t, gerr)
			require.Equal(t, "Bulbasaur", got.Item["name"].(*dynamodbtypes.AttributeValueMemberS).Value)
		})
	}
}

// A condition that matches item A must not let DeleteItem(B) succeed.
func TestDeleteItemConditionMatchingOtherItemFails(t *testing.T) {
	ctx := context.Background()
	client := setupClient(tableName)

	require.NoError(t, ensurePokemonTable(client))
	putPokemon(t, client, "A", "grass")
	putPokemon(t, client, "B", "fire")

	_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 aws.String(tableName),
		Key:                       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "B"}},
		ConditionExpression:       aws.String("#n = :n"),
		ExpressionAttributeNames:  map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberS{Value: "grass"}},
	})

	var ccf *dynamodbtypes.ConditionalCheckFailedException

	require.ErrorAs(t, err, &ccf)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "B"}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Item, "B must not be deleted")
}

// With several invalid inputs at once the mode enum message wins over an invalid
// ReturnValues and an undeclared expression name. The mode is checked before the
// table lookup, so it also wins on a missing table (pinned current behavior). UpdateItem
// does not send an invalid ReturnValues here: it reports that first (see
// TestUpdateItemInvalidReturnValues).
func TestReturnValuesOnConditionCheckFailureValidationOrder(t *testing.T) {
	ctx := context.Background()
	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}
	cond := aws.String("#undeclared = :v")
	vals := map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"}}

	ops := map[string]func(cli FakeClient, table string) error{
		"PutItem": func(cli FakeClient, table string) error {
			_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(table), Item: key, ConditionExpression: cond, ExpressionAttributeValues: vals,
				ReturnValues: "BOGUS", ReturnValuesOnConditionCheckFailure: "BOGUS",
			})

			return err
		},
		"UpdateItem": func(cli FakeClient, table string) error {
			_, err := cli.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(table), Key: key, ConditionExpression: cond,
				UpdateExpression:                    aws.String("SET #n = :v"),
				ExpressionAttributeNames:            map[string]string{"#n": "name"},
				ExpressionAttributeValues:           vals,
				ReturnValuesOnConditionCheckFailure: "BOGUS",
			})

			return err
		},
		"DeleteItem": func(cli FakeClient, table string) error {
			_, err := cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(table), Key: key, ConditionExpression: cond, ExpressionAttributeValues: vals,
				ReturnValues: "BOGUS", ReturnValuesOnConditionCheckFailure: "BOGUS",
			})

			return err
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			cli := setupClient(tableName)

			require.NoError(t, ensurePokemonTable(cli))
			putPokemon(t, cli, "001", "Bulbasaur")

			require.ErrorContains(t, op(cli, tableName), bogusModeMessage)
			require.ErrorContains(t, op(cli, "missing"), bogusModeMessage)
		})
	}
}
