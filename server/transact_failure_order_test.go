package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// failureScope arms an emulated failure on a client, either for every call or for the
// "pokemons" table only.
type failureScope struct {
	name string
	arm  func(c *Client)
}

var transactFailureScopes = []failureScope{
	{"Global", func(c *Client) { c.setFailureCondition(emulatedInternalServerError) }},
	{"Table", func(c *Client) { c.setTableFailureCondition("pokemons", "", emulatedInternalServerError) }},
}

const bogusShapeMessage = "returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"

const invalidModeMessage = "1 validation error detected: Value 'FOO' at 'returnConsumedCapacity' failed to " +
	"satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"

func requireValidation(t *testing.T, err error, message string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
	require.Contains(t, apiErr.ErrorMessage(), message)
}

func putTransactItem(table, id string) TransactWriteItem {
	return TransactWriteItem{Put: &Put{TableName: aws.String(table), Item: idKey(id)}}
}

func getTransactItem(table, id string) TransactGetItem {
	return TransactGetItem{Get: &Get{TableName: aws.String(table), Key: idKey(id)}}
}

// The transaction operations check, in order: the input shape, ReturnConsumedCapacity, the
// emulated failure, then each item (#275). That is the order of the batches (D13).
func TestServerTransactWriteItemsCheckOrder(t *testing.T) {
	bogus := ddbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")

	for _, scope := range transactFailureScopes {
		t.Run(scope.name, func(t *testing.T) {
			run := func(input *TransactWriteItemsInput) error {
				client := newServerWithPokemonTable(t)
				scope.arm(client)

				_, err := client.TransactWriteItems(context.Background(), input)

				return err
			}

			// ReturnValuesOnConditionCheckFailure is an item-level check, after the failure, as in
			// the single-item operations.
			t.Run("FailureBeatsInvalidRVOCCF", func(t *testing.T) {
				item := putTransactItem("pokemons", "001")
				item.Put.ReturnValuesOnConditionCheckFailure = bogus

				err := run(&TransactWriteItemsInput{TransactItems: []TransactWriteItem{item}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})

			t.Run("InvalidModeBeatsInvalidRVOCCF", func(t *testing.T) {
				item := putTransactItem("pokemons", "001")
				item.Put.ReturnValuesOnConditionCheckFailure = bogus

				requireValidation(t, run(&TransactWriteItemsInput{
					ReturnConsumedCapacity: "FOO", TransactItems: []TransactWriteItem{item},
				}), invalidModeMessage)
			})

			t.Run("FailureBeatsInvalidKey", func(t *testing.T) {
				err := run(&TransactWriteItemsInput{TransactItems: []TransactWriteItem{
					{Put: &Put{TableName: aws.String("pokemons"), Item: map[string]*AttributeValue{}}},
				}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})

			t.Run("InvalidModeBeatsFailure", func(t *testing.T) {
				requireValidation(t, run(&TransactWriteItemsInput{
					ReturnConsumedCapacity: "FOO", TransactItems: []TransactWriteItem{putTransactItem("pokemons", "001")},
				}), invalidModeMessage)
			})

			t.Run("FailureBeatsMissingTable", func(t *testing.T) {
				err := run(&TransactWriteItemsInput{TransactItems: []TransactWriteItem{
					putTransactItem("missing", "001"), putTransactItem("pokemons", "002"),
				}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})

			t.Run("FailureBeatsDuplicateItems", func(t *testing.T) {
				err := run(&TransactWriteItemsInput{TransactItems: []TransactWriteItem{
					putTransactItem("pokemons", "001"), putTransactItem("pokemons", "001"),
				}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})
		})
	}
}

func TestServerTransactGetItemsCheckOrder(t *testing.T) {
	for _, scope := range transactFailureScopes {
		t.Run(scope.name, func(t *testing.T) {
			run := func(input *TransactGetItemsInput) error {
				client := newServerWithPokemonTable(t)
				scope.arm(client)

				_, err := client.TransactGetItems(context.Background(), input)

				return err
			}

			// The second item has no Get, so the failure scoped to pokemons would apply to the
			// first one if the order were wrong.
			malformed := []TransactGetItem{getTransactItem("pokemons", "001"), {Get: nil}}

			t.Run("ShapeErrorBeatsFailure", func(t *testing.T) {
				requireValidation(t, run(&TransactGetItemsInput{TransactItems: malformed}), "transaction item must include Get")
			})

			t.Run("ShapeErrorBeatsInvalidMode", func(t *testing.T) {
				requireValidation(t, run(&TransactGetItemsInput{ReturnConsumedCapacity: "FOO", TransactItems: malformed}),
					"transaction item must include Get")
			})

			t.Run("InvalidModeBeatsFailure", func(t *testing.T) {
				requireValidation(t, run(&TransactGetItemsInput{
					ReturnConsumedCapacity: "FOO", TransactItems: []TransactGetItem{getTransactItem("pokemons", "001")},
				}), invalidModeMessage)
			})

			t.Run("FailureBeatsMissingTable", func(t *testing.T) {
				err := run(&TransactGetItemsInput{TransactItems: []TransactGetItem{
					getTransactItem("missing", "001"), getTransactItem("pokemons", "002"),
				}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})

			t.Run("FailureBeatsDuplicateItems", func(t *testing.T) {
				err := run(&TransactGetItemsInput{TransactItems: []TransactGetItem{
					getTransactItem("pokemons", "001"), getTransactItem("pokemons", "001"),
				}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})

			t.Run("FailureBeatsInvalidKey", func(t *testing.T) {
				err := run(&TransactGetItemsInput{TransactItems: []TransactGetItem{
					{Get: &Get{TableName: aws.String("pokemons"), Key: map[string]*AttributeValue{}}},
				}})

				require.ErrorIs(t, err, emulatedInternalServerError)
			})
		})
	}
}

// The shape check runs over every item before any table lookup (#275), so a malformed later
// item answers before an earlier item's missing table.
func TestServerTransactGetItemsShapeBeatsTableLookup(t *testing.T) {
	client := newServerWithPokemonTable(t)

	_, err := client.TransactGetItems(context.Background(), &TransactGetItemsInput{
		TransactItems: []TransactGetItem{getTransactItem("missing", "001"), {Get: nil}},
	})

	requireValidation(t, err, "transaction item must include Get")
}
