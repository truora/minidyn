package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

const bogusModeMessage = "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"

func putNamedItem(t *testing.T, cli *dynamodb.Client, id, name string) {
	t.Helper()

	_, err := cli.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"),
		Item: map[string]ddbtypes.AttributeValue{
			"id":   &ddbtypes.AttributeValueMemberS{Value: id},
			"name": &ddbtypes.AttributeValueMemberS{Value: name},
		},
	})
	require.NoError(t, err)
}

func newPokemonsClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	ts := httptest.NewServer(NewServer())
	t.Cleanup(ts.Close)

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	return cli
}

func TestServerUpdateItemInvalidReturnValuesOnConditionCheckFailure(t *testing.T) {
	cli := newPokemonsClient(t)

	putNamedItem(t, cli, "001", "Bulbasaur")

	_, err := cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                           aws.String("pokemons"),
		Key:                                 map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}},
		UpdateExpression:                    aws.String("SET #n = :v"),
		ConditionExpression:                 aws.String("attribute_not_exists(id)"),
		ExpressionAttributeNames:            map[string]string{"#n": "name"},
		ExpressionAttributeValues:           map[string]ddbtypes.AttributeValue{":v": &ddbtypes.AttributeValueMemberS{Value: "x"}},
		ReturnValuesOnConditionCheckFailure: "BOGUS",
	})
	require.ErrorContains(t, err, bogusModeMessage)
}

// DynamoDB Local accepts the mode inside a transaction; minidyn follows the API reference
// (Valid Values: ALL_OLD | NONE) and rejects the whole transaction before any write.
func TestServerTransactionInvalidReturnValuesOnConditionCheckFailureIsRejected(t *testing.T) {
	ctx := context.Background()
	key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}}
	const bogus = ddbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")
	cond := aws.String("attribute_not_exists(id)")

	items := map[string]ddbtypes.TransactWriteItem{
		"Put": {Put: &ddbtypes.Put{
			TableName: aws.String("pokemons"), Item: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
		}},
		"Delete": {Delete: &ddbtypes.Delete{
			TableName: aws.String("pokemons"), Key: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
		}},
		"Update": {Update: &ddbtypes.Update{
			TableName: aws.String("pokemons"), Key: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
			UpdateExpression:          aws.String("SET #n = :v"),
			ExpressionAttributeNames:  map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": &ddbtypes.AttributeValueMemberS{Value: "x"}},
		}},
		"ConditionCheck": {ConditionCheck: &ddbtypes.ConditionCheck{
			TableName: aws.String("pokemons"), Key: key, ConditionExpression: cond, ReturnValuesOnConditionCheckFailure: bogus,
		}},
	}

	shapes := map[string]string{"Put": "put", "Delete": "delete", "Update": "update", "ConditionCheck": "conditionCheck"}

	for name, item := range items {
		t.Run(name, func(t *testing.T) {
			cli := newPokemonsClient(t)

			putNamedItem(t, cli, "001", "Bulbasaur")

			_, err := cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []ddbtypes.TransactWriteItem{item},
			})
			require.Error(t, err)

			_, isCancelled := errors.AsType[*ddbtypes.TransactionCanceledException](err)
			require.False(t, isCancelled, "%v", err)

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "ValidationException", apiErr.ErrorCode())
			require.Equal(t,
				"1 validation error detected: Value 'BOGUS' at 'transactItems.1.member."+shapes[name]+".returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]",
				apiErr.ErrorMessage())

			// Rejected before any write: the stored item is untouched.
			got, gerr := cli.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("pokemons"), Key: key})
			require.NoError(t, gerr)
			require.Equal(t, "Bulbasaur", got.Item["name"].(*ddbtypes.AttributeValueMemberS).Value)
		})
	}
}

// A condition that matches item A must not let DeleteItem(B) succeed.
func TestServerDeleteItemConditionMatchingOtherItemFails(t *testing.T) {
	ctx := context.Background()
	cli := newPokemonsClient(t)

	putNamedItem(t, cli, "A", "grass")
	putNamedItem(t, cli, "B", "fire")

	_, err := cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 aws.String("pokemons"),
		Key:                       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "B"}},
		ConditionExpression:       aws.String("#n = :n"),
		ExpressionAttributeNames:  map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":n": &ddbtypes.AttributeValueMemberS{Value: "grass"}},
	})

	_, ok := errors.AsType[*ddbtypes.ConditionalCheckFailedException](err)
	require.True(t, ok, "%v", err)

	out, err := cli.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("pokemons"),
		Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "B"}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Item, "B must not be deleted")
}

// With several invalid inputs at once the mode enum message wins over an invalid
// ReturnValues and an undeclared expression name. The mode is checked before the
// table lookup, so it also wins on a missing table (pinned current behavior).
func TestServerReturnValuesOnConditionCheckFailureValidationOrder(t *testing.T) {
	ctx := context.Background()
	key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}}
	cond := aws.String("#undeclared = :v")
	vals := map[string]ddbtypes.AttributeValue{":v": &ddbtypes.AttributeValueMemberS{Value: "x"}}

	ops := map[string]func(cli *dynamodb.Client, table string) error{
		"PutItem": func(cli *dynamodb.Client, table string) error {
			_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(table), Item: key, ConditionExpression: cond, ExpressionAttributeValues: vals,
				ReturnValues: "BOGUS", ReturnValuesOnConditionCheckFailure: "BOGUS",
			})

			return err
		},
		"UpdateItem": func(cli *dynamodb.Client, table string) error {
			_, err := cli.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(table), Key: key, ConditionExpression: cond,
				UpdateExpression:          aws.String("SET #n = :v"),
				ExpressionAttributeNames:  map[string]string{"#n": "name"},
				ExpressionAttributeValues: vals,
				ReturnValues:              "BOGUS", ReturnValuesOnConditionCheckFailure: "BOGUS",
			})

			return err
		},
		"DeleteItem": func(cli *dynamodb.Client, table string) error {
			_, err := cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(table), Key: key, ConditionExpression: cond, ExpressionAttributeValues: vals,
				ReturnValues: "BOGUS", ReturnValuesOnConditionCheckFailure: "BOGUS",
			})

			return err
		},
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			cli := newPokemonsClient(t)

			putNamedItem(t, cli, "001", "Bulbasaur")

			require.ErrorContains(t, op(cli, "pokemons"), bogusModeMessage)
			require.ErrorContains(t, op(cli, "missing"), bogusModeMessage)
		})
	}
}
