package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// A transactional ConditionCheck or Get with an invalid key fails as a ValidationException
// (not as a cancelled transaction), with the message of the same key in a GetItem. This pins
// current behavior; DynamoDB Local answers TransactionCanceledException for most
// TransactGetItems key errors and for a ConditionCheck key type mismatch (see #181).
func TestServerTransactInvalidKeys(t *testing.T) {
	cli := newPokemonsClient(t)
	putNamedItem(t, cli, "001", "Bulbasaur")

	s := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
	n := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: v} }

	tests := []struct {
		name string
		key  map[string]ddbtypes.AttributeValue
		want string
	}{
		{"NoAttributes", map[string]ddbtypes.AttributeValue{}, "The number of conditions on the keys is invalid | expected keys: 1; got keys: 0"},
		{"ExtraAttribute", map[string]ddbtypes.AttributeValue{"id": s("1"), "x": s("2")}, "The number of conditions on the keys is invalid | expected keys: 1; got keys: 2"},
		{"NotTheKeyAttribute", map[string]ddbtypes.AttributeValue{"zzz": s("1")}, "One of the required keys was not given a value | missing: hash key"},
		{"WrongType", map[string]ddbtypes.AttributeValue{"id": n("1")}, "One or more parameter values were invalid: Type mismatch for key | field: id; expected: S; actual: N"},
		{"InvalidNumber", map[string]ddbtypes.AttributeValue{"id": n("abc")}, "One or more parameter values were invalid: Type mismatch for key | field: id; expected: S; actual: N"},
		{"EmptyString", map[string]ddbtypes.AttributeValue{"id": s("")}, "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: id"},
	}

	requireTransactKeyErrors(t, cli, "pokemons", tests)
}

// A ConditionCheck on a missing item evaluates its condition against an empty item: it passes
// attribute_not_exists, and reports no item with ReturnValuesOnConditionCheckFailure ALL_OLD.
func TestServerTransactConditionCheckMissingItem(t *testing.T) {
	cli := newPokemonsClient(t)
	key := map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "404"}}

	_, err := cli.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbtypes.TransactWriteItem{{ConditionCheck: &ddbtypes.ConditionCheck{
			TableName: aws.String("pokemons"), Key: key, ConditionExpression: aws.String("attribute_not_exists(id)"),
		}}},
	})
	require.NoError(t, err)

	_, err = cli.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddbtypes.TransactWriteItem{{ConditionCheck: &ddbtypes.ConditionCheck{
			TableName: aws.String("pokemons"), Key: key, ConditionExpression: aws.String("attribute_exists(id)"),
			ReturnValuesOnConditionCheckFailure: ddbtypes.ReturnValuesOnConditionCheckFailureAllOld,
		}}},
	})

	var canceled *ddbtypes.TransactionCanceledException

	require.ErrorAs(t, err, &canceled)
	require.Len(t, canceled.CancellationReasons, 1)
	require.Equal(t, "ConditionalCheckFailed", aws.ToString(canceled.CancellationReasons[0].Code))
	require.Empty(t, canceled.CancellationReasons[0].Item)
}

func requireTransactKeyErrors(t *testing.T, cli *dynamodb.Client, table string, tests []struct {
	name string
	key  map[string]ddbtypes.AttributeValue
	want string
},
) {
	t.Helper()

	for _, tt := range tests {
		t.Run("ConditionCheck"+tt.name, func(t *testing.T) {
			_, err := cli.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
				TransactItems: []ddbtypes.TransactWriteItem{{ConditionCheck: &ddbtypes.ConditionCheck{
					TableName: aws.String(table), Key: tt.key, ConditionExpression: aws.String("attribute_exists(id)"),
				}}},
			})
			requireValidationError(t, err, tt.want)
		})

		t.Run("Get"+tt.name, func(t *testing.T) {
			_, err := cli.TransactGetItems(context.Background(), &dynamodb.TransactGetItemsInput{
				TransactItems: []ddbtypes.TransactGetItem{{Get: &ddbtypes.Get{TableName: aws.String(table), Key: tt.key}}},
			})
			requireValidationError(t, err, tt.want)
		})
	}
}

// The same guard on a table with a hash and a range key.
func TestServerTransactInvalidKeysHashAndRange(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	t.Cleanup(ts.Close)

	cli := newTestDynamoClient(t, ts.URL)

	_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("ranged"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("name"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("name"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	s := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
	n := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: v} }

	tests := []struct {
		name string
		key  map[string]ddbtypes.AttributeValue
		want string
	}{
		{"MissingRangeKey", map[string]ddbtypes.AttributeValue{"id": s("1")}, "The number of conditions on the keys is invalid | expected keys: 2; got keys: 1"},
		{"WrongRangeType", map[string]ddbtypes.AttributeValue{"id": s("1"), "name": n("1")}, "One or more parameter values were invalid: Type mismatch for key | field: name; expected: S; actual: N"},
		{"EmptyHash", map[string]ddbtypes.AttributeValue{"id": s(""), "name": s("x")}, "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: id"},
	}

	requireTransactKeyErrors(t, cli, "ranged", tests)
}

func requireValidationError(t *testing.T, err error, want string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode())
	require.Equal(t, want, apiErr.ErrorMessage())
}
