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

// transactFailureScope arms an emulated failure, either for every call or for the pokemons
// table only.
type transactFailureScope struct {
	name string
	arm  func(c FakeClient)
}

var transactFailureScopes = []transactFailureScope{
	{"Global", func(c FakeClient) { EmulateFailure(c, FailureConditionInternalServerError) }},
	{"Table", func(c FakeClient) { EmulateFailureForTable(c, tableName, FailureConditionInternalServerError) }},
}

const (
	transactBogusShapeMessage  = "returnValuesOnConditionCheckFailure' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]"
	transactInvalidModeMessage = "1 validation error detected: Value 'FOO' at 'returnConsumedCapacity' failed to " +
		"satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]"
	transactMissingGetMessage = "transaction item must include Get"
)

func requireTransactValidation(t *testing.T, err error, message string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
	require.Contains(t, apiErr.ErrorMessage(), message)
}

func putTransactItem(table, id string) dynamodbtypes.TransactWriteItem {
	return dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{TableName: aws.String(table), Item: keyID(id)}}
}

func getTransactItem(table, id string) dynamodbtypes.TransactGetItem {
	return dynamodbtypes.TransactGetItem{Get: &dynamodbtypes.Get{TableName: aws.String(table), Key: keyID(id)}}
}

// The transaction operations check, in order: the input shape, ReturnConsumedCapacity, the
// emulated failure, then each item (#275). That is the order of the batches (D13).
func TestTransactWriteItemsCheckOrder(t *testing.T) {
	bogus := dynamodbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")

	for _, scope := range transactFailureScopes {
		t.Run(scope.name, func(t *testing.T) {
			run := func(input *dynamodb.TransactWriteItemsInput) error {
				client := NewClient()
				require.NoError(t, ensurePokemonTable(client))
				scope.arm(client)

				_, err := client.TransactWriteItems(context.Background(), input)

				return err
			}

			// ReturnValuesOnConditionCheckFailure is an item-level check, after the failure, as in
			// the single-item operations.
			t.Run("FailureBeatsInvalidRVOCCF", func(t *testing.T) {
				item := putTransactItem(tableName, "001")
				item.Put.ReturnValuesOnConditionCheckFailure = bogus

				err := run(&dynamodb.TransactWriteItemsInput{
					TransactItems: []dynamodbtypes.TransactWriteItem{item},
				})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})

			t.Run("InvalidModeBeatsInvalidRVOCCF", func(t *testing.T) {
				item := putTransactItem(tableName, "001")
				item.Put.ReturnValuesOnConditionCheckFailure = bogus

				requireTransactValidation(t, run(&dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: "FOO", TransactItems: []dynamodbtypes.TransactWriteItem{item},
				}), transactInvalidModeMessage)
			})

			t.Run("FailureBeatsInvalidKey", func(t *testing.T) {
				err := run(&dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
					{Put: &dynamodbtypes.Put{TableName: aws.String(tableName), Item: map[string]dynamodbtypes.AttributeValue{}}},
				}})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})

			t.Run("InvalidModeBeatsFailure", func(t *testing.T) {
				requireTransactValidation(t, run(&dynamodb.TransactWriteItemsInput{
					ReturnConsumedCapacity: "FOO",
					TransactItems:          []dynamodbtypes.TransactWriteItem{putTransactItem(tableName, "001")},
				}), transactInvalidModeMessage)
			})

			t.Run("FailureBeatsMissingTable", func(t *testing.T) {
				err := run(&dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
					putTransactItem("missing", "001"), putTransactItem(tableName, "002"),
				}})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})

			t.Run("FailureBeatsDuplicateItems", func(t *testing.T) {
				err := run(&dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
					putTransactItem(tableName, "001"), putTransactItem(tableName, "001"),
				}})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})
		})
	}
}

func TestTransactGetItemsCheckOrder(t *testing.T) {
	for _, scope := range transactFailureScopes {
		t.Run(scope.name, func(t *testing.T) {
			run := func(input *dynamodb.TransactGetItemsInput) error {
				client := NewClient()
				require.NoError(t, ensurePokemonTable(client))
				scope.arm(client)

				_, err := client.TransactGetItems(context.Background(), input)

				return err
			}

			// The second item has no Get, so the failure scoped to the table would apply to the
			// first one if the order were wrong.
			malformed := []dynamodbtypes.TransactGetItem{getTransactItem(tableName, "001"), {Get: nil}}

			t.Run("ShapeErrorBeatsFailure", func(t *testing.T) {
				requireTransactValidation(t, run(&dynamodb.TransactGetItemsInput{TransactItems: malformed}), transactMissingGetMessage)
			})

			t.Run("ShapeErrorBeatsInvalidMode", func(t *testing.T) {
				requireTransactValidation(t, run(&dynamodb.TransactGetItemsInput{
					ReturnConsumedCapacity: "FOO", TransactItems: malformed,
				}), transactMissingGetMessage)
			})

			t.Run("InvalidModeBeatsFailure", func(t *testing.T) {
				requireTransactValidation(t, run(&dynamodb.TransactGetItemsInput{
					ReturnConsumedCapacity: "FOO",
					TransactItems:          []dynamodbtypes.TransactGetItem{getTransactItem(tableName, "001")},
				}), transactInvalidModeMessage)
			})

			t.Run("FailureBeatsMissingTable", func(t *testing.T) {
				err := run(&dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{
					getTransactItem("missing", "001"), getTransactItem(tableName, "002"),
				}})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})

			t.Run("FailureBeatsDuplicateItems", func(t *testing.T) {
				err := run(&dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{
					getTransactItem(tableName, "001"), getTransactItem(tableName, "001"),
				}})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})

			t.Run("FailureBeatsInvalidKey", func(t *testing.T) {
				err := run(&dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{
					{Get: &dynamodbtypes.Get{TableName: aws.String(tableName), Key: map[string]dynamodbtypes.AttributeValue{}}},
				}})

				require.ErrorIs(t, err, &emulatedInternalServeError)
			})
		})
	}
}

// The shape check runs over every item before any table lookup (#275), so a malformed later
// item answers before an earlier item's missing table.
func TestTransactGetItemsShapeBeatsTableLookup(t *testing.T) {
	client := NewClient()
	require.NoError(t, ensurePokemonTable(client))

	_, err := client.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
		TransactItems: []dynamodbtypes.TransactGetItem{getTransactItem("missing", "001"), {Get: nil}},
	})

	requireTransactValidation(t, err, transactMissingGetMessage)
}
