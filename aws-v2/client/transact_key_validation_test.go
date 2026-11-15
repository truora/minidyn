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

// A transactional ConditionCheck or Get with an invalid key fails as a ValidationException
// (not as a cancelled transaction), with the message of the same key in a GetItem. This pins
// current behavior; DynamoDB Local answers TransactionCanceledException for most
// TransactGetItems key errors and for a ConditionCheck key type mismatch (see #181).
func TestTransactInvalidKeys(t *testing.T) {
	client := setupClient(tableName)

	require.NoError(t, ensurePokemonTable(client))
	putPokemon(t, client, "001", "Bulbasaur")

	s := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	n := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

	tests := []struct {
		name string
		key  map[string]dynamodbtypes.AttributeValue
		want string
	}{
		{"NoAttributes", map[string]dynamodbtypes.AttributeValue{}, "The number of conditions on the keys is invalid | expected keys: 1; got keys: 0"},
		{"ExtraAttribute", map[string]dynamodbtypes.AttributeValue{"id": s("1"), "x": s("2")}, "The number of conditions on the keys is invalid | expected keys: 1; got keys: 2"},
		{"NotTheKeyAttribute", map[string]dynamodbtypes.AttributeValue{"zzz": s("1")}, "One of the required keys was not given a value | missing: hash key"},
		{"WrongType", map[string]dynamodbtypes.AttributeValue{"id": n("1")}, "One or more parameter values were invalid: Type mismatch for key | field: id; expected: S; actual: N"},
		{"InvalidNumber", map[string]dynamodbtypes.AttributeValue{"id": n("abc")}, "One or more parameter values were invalid: Type mismatch for key | field: id; expected: S; actual: N"},
		{"EmptyString", map[string]dynamodbtypes.AttributeValue{"id": s("")}, "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: id"},
	}

	requireTransactKeyErrors(t, client, tableName, tests)
}

// A ConditionCheck on a missing item evaluates its condition against an empty item: it passes
// attribute_not_exists, and reports no item with ReturnValuesOnConditionCheckFailure ALL_OLD.
func TestTransactConditionCheckMissingItem(t *testing.T) {
	client := setupClient(tableName)

	require.NoError(t, ensurePokemonTable(client))
	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "404"}}

	_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(tableName), Key: key, ConditionExpression: aws.String("attribute_not_exists(id)"),
		}}},
	})
	require.NoError(t, err)

	_, err = client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(tableName), Key: key, ConditionExpression: aws.String("attribute_exists(id)"),
			ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
		}}},
	})

	var canceled *dynamodbtypes.TransactionCanceledException

	require.ErrorAs(t, err, &canceled)
	require.Len(t, canceled.CancellationReasons, 1)
	require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[0].Code))
	require.Empty(t, canceled.CancellationReasons[0].Item)
}

func requireTransactKeyErrors(t *testing.T, client FakeClient, table string, tests []struct {
	name string
	key  map[string]dynamodbtypes.AttributeValue
	want string
},
) {
	t.Helper()

	for _, tt := range tests {
		t.Run("ConditionCheck"+tt.name, func(t *testing.T) {
			_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
					TableName: aws.String(table), Key: tt.key, ConditionExpression: aws.String("attribute_exists(id)"),
				}}},
			})
			requireValidationError(t, err, tt.want)
		})

		t.Run("Get"+tt.name, func(t *testing.T) {
			_, err := client.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
				TransactItems: []dynamodbtypes.TransactGetItem{{Get: &dynamodbtypes.Get{TableName: aws.String(table), Key: tt.key}}},
			})
			requireValidationError(t, err, tt.want)
		})
	}
}

// The same guard on a table with a hash and a range key.
func TestTransactInvalidKeysHashAndRange(t *testing.T) {
	client := setupClient(tableName)

	require.NoError(t, AddTable(context.Background(), client, tableName+"-ranged", "id", "name"))

	s := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	n := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

	tests := []struct {
		name string
		key  map[string]dynamodbtypes.AttributeValue
		want string
	}{
		{"MissingRangeKey", map[string]dynamodbtypes.AttributeValue{"id": s("1")}, "The number of conditions on the keys is invalid | expected keys: 2; got keys: 1"},
		{"WrongRangeType", map[string]dynamodbtypes.AttributeValue{"id": s("1"), "name": n("1")}, "One or more parameter values were invalid: Type mismatch for key | field: name; expected: S; actual: N"},
		{"EmptyHash", map[string]dynamodbtypes.AttributeValue{"id": s(""), "name": s("x")}, "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: id"},
	}

	requireTransactKeyErrors(t, client, tableName+"-ranged", tests)
}

func requireValidationError(t *testing.T, err error, want string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
	require.Equal(t, want, apiErr.ErrorMessage())
}
