package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestServerReturnValuesOnConditionCheckFailure(t *testing.T) {
	const (
		allOld = ddbtypes.ReturnValuesOnConditionCheckFailureAllOld
		none   = ddbtypes.ReturnValuesOnConditionCheckFailureNone
	)

	ctx := context.Background()
	key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}}
	newItem := map[string]ddbtypes.AttributeValue{
		"id":   key["id"],
		"name": &ddbtypes.AttributeValueMemberS{Value: "Impostor"},
	}

	type mode = ddbtypes.ReturnValuesOnConditionCheckFailure

	ops := map[string]func(cli *dynamodb.Client, m mode, cond string) error{
		"PutItem": func(cli *dynamodb.Client, m mode, cond string) error {
			_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String("pokemons"), Item: newItem,
				ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: m,
			})

			return err
		},
		"DeleteItem": func(cli *dynamodb.Client, m mode, cond string) error {
			_, err := cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String("pokemons"), Key: key,
				ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: m,
			})

			return err
		},
		"TransactPut": func(cli *dynamodb.Client, m mode, cond string) error {
			_, err := cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []ddbtypes.TransactWriteItem{{Put: &ddbtypes.Put{
					TableName: aws.String("pokemons"), Item: newItem,
					ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: m,
				}}},
			})

			return err
		},
		"TransactDelete": func(cli *dynamodb.Client, m mode, cond string) error {
			_, err := cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []ddbtypes.TransactWriteItem{{Delete: &ddbtypes.Delete{
					TableName: aws.String("pokemons"), Key: key,
					ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: m,
				}}},
			})

			return err
		},
		"TransactConditionCheck": func(cli *dynamodb.Client, m mode, cond string) error {
			_, err := cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []ddbtypes.TransactWriteItem{{ConditionCheck: &ddbtypes.ConditionCheck{
					TableName: aws.String("pokemons"), Key: key,
					ConditionExpression: aws.String(cond), ReturnValuesOnConditionCheckFailure: m,
				}}},
			})

			return err
		},
	}

	failedItem := func(t *testing.T, err error) map[string]ddbtypes.AttributeValue {
		t.Helper()

		if ccf, ok := errors.AsType[*ddbtypes.ConditionalCheckFailedException](err); ok {
			return ccf.Item
		}

		var tce *ddbtypes.TransactionCanceledException

		require.ErrorAs(t, err, &tce)
		require.Len(t, tce.CancellationReasons, 1)

		return tce.CancellationReasons[0].Item
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			setup := func(seed bool) *dynamodb.Client {
				ts := httptest.NewServer(NewServer())
				t.Cleanup(ts.Close)

				cli := newTestDynamoClient(t, ts.URL)
				makeBasicTable(t, cli, "pokemons", "id")

				if seed {
					_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String("pokemons"),
						Item: map[string]ddbtypes.AttributeValue{
							"id":   key["id"],
							"name": &ddbtypes.AttributeValueMemberS{Value: "Bulbasaur"},
						},
					})
					require.NoError(t, err)
				}

				return cli
			}

			item := failedItem(t, op(setup(true), allOld, "attribute_not_exists(id)"))
			require.Equal(t, &ddbtypes.AttributeValueMemberS{Value: "Bulbasaur"}, item["name"])

			for _, m := range []mode{"", none} {
				require.Empty(t, failedItem(t, op(setup(true), m, "attribute_not_exists(id)")))
			}

			require.Nil(t, failedItem(t, op(setup(false), allOld, "attribute_exists(id)")), "a missing item has nothing to return")
		})
	}
}

func TestServerPutDeleteInvalidReturnValuesOnConditionCheckFailure(t *testing.T) {
	ctx := context.Background()
	ts := httptest.NewServer(NewServer())

	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "001"}}
	bogus := ddbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")
	want := "1 validation error detected: Value 'BOGUS' at 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"

	_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("pokemons"), Item: key, ReturnValuesOnConditionCheckFailure: bogus,
	})
	require.ErrorContains(t, err, want)

	_, err = cli.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("pokemons"), Key: key, ReturnValuesOnConditionCheckFailure: bogus,
	})
	require.ErrorContains(t, err, want)
}
