package client

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestReturnValuesOnConditionCheckFailure(t *testing.T) {
	const (
		allOld = dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld
		none   = dynamodbtypes.ReturnValuesOnConditionCheckFailureNone
	)

	ctx := context.Background()
	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}
	newItem := map[string]dynamodbtypes.AttributeValue{
		"id":   key["id"],
		"name": &dynamodbtypes.AttributeValueMemberS{Value: "Impostor"},
	}

	ops := map[string]func(c FakeClient, mode dynamodbtypes.ReturnValuesOnConditionCheckFailure, cond string) error{
		"PutItem": func(c FakeClient, mode dynamodbtypes.ReturnValuesOnConditionCheckFailure, cond string) error {
			_, err := c.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(tableName), Item: newItem,
				ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: mode,
			})

			return err
		},
		"DeleteItem": func(c FakeClient, mode dynamodbtypes.ReturnValuesOnConditionCheckFailure, cond string) error {
			_, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(tableName), Key: key,
				ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: mode,
			})

			return err
		},
		"TransactPut": func(c FakeClient, mode dynamodbtypes.ReturnValuesOnConditionCheckFailure, cond string) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{{Put: &dynamodbtypes.Put{
					TableName: aws.String(tableName), Item: newItem,
					ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: mode,
				}}},
			})

			return err
		},
		"TransactDelete": func(c FakeClient, mode dynamodbtypes.ReturnValuesOnConditionCheckFailure, cond string) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{{Delete: &dynamodbtypes.Delete{
					TableName: aws.String(tableName), Key: key,
					ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: mode,
				}}},
			})

			return err
		},
		"TransactConditionCheck": func(c FakeClient, mode dynamodbtypes.ReturnValuesOnConditionCheckFailure, cond string) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
					TableName: aws.String(tableName), Key: key,
					ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: mode,
				}}},
			})

			return err
		},
	}

	// failedItem returns the item attached to the failure, whichever shape the operation uses.
	failedItem := func(t *testing.T, err error) map[string]dynamodbtypes.AttributeValue {
		t.Helper()

		if ccf, ok := errors.AsType[*dynamodbtypes.ConditionalCheckFailedException](err); ok {
			return ccf.Item
		}

		var tce *dynamodbtypes.TransactionCanceledException

		require.ErrorAs(t, err, &tce)
		require.Len(t, tce.CancellationReasons, 1)

		return tce.CancellationReasons[0].Item
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			setup := func(seed bool) FakeClient {
				client := setupClient(tableName)
				require.NoError(t, ensurePokemonTable(client))

				if seed {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]dynamodbtypes.AttributeValue{
							"id":   key["id"],
							"name": &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
						},
					})
					require.NoError(t, err)
				}

				return client
			}

			item := failedItem(t, op(setup(true), allOld, "attribute_not_exists(id)"))
			require.Equal(t, &dynamodbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, item["name"])

			for _, mode := range []dynamodbtypes.ReturnValuesOnConditionCheckFailure{"", none} {
				require.Empty(t, failedItem(t, op(setup(true), mode, "attribute_not_exists(id)")))
			}

			require.Nil(t, failedItem(t, op(setup(false), allOld, "attribute_exists(id)")), "a missing item has nothing to return")
		})
	}
}

func TestPutDeleteInvalidReturnValuesOnConditionCheckFailure(t *testing.T) {
	ctx := context.Background()
	client := setupClient(tableName)

	require.NoError(t, ensurePokemonTable(client))

	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}}
	bogus := dynamodbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")
	want := "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName), Item: key, ReturnValuesOnConditionCheckFailure: bogus,
	})
	require.ErrorContains(t, err, want)

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName), Key: key, ReturnValuesOnConditionCheckFailure: bogus,
	})
	require.ErrorContains(t, err, want)
}
