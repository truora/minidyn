package e2e

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// ReturnValuesOnConditionCheckFailure on PutItem, DeleteItem and the transactional Put and
// Delete: ALL_OLD returns the existing item, NONE (or unset) returns nothing, and a missing
// item has nothing to return.

type ccfMode = dynamodbtypes.ReturnValuesOnConditionCheckFailure

func parityPokemonKey(id string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: id}}
}

// parityCCFItemName summarizes a failed conditional write: the normalized error and the name of
// the item DynamoDB attached to it ("<none>" when there is no item).
func parityCCFItemName(t *testing.T, err error) []any {
	t.Helper()

	var ccf *dynamodbtypes.ConditionalCheckFailedException
	if !errors.As(err, &ccf) {
		return []any{normalizeSDKErrorString(err.Error())}
	}

	name := "<none>"
	if ccf.Item != nil {
		name = parityAttrValueString(ccf.Item["name"])
	}

	return []any{normalizeSDKErrorString(err.Error()), name, len(ccf.Item)}
}

// parityCancelReasonSummary summarizes a canceled transaction: the normalized error and, for
// each cancellation reason, its code and the item name ("<none>" when absent).
func parityCancelReasonSummary(t *testing.T, err error) []any {
	t.Helper()

	var tce *dynamodbtypes.TransactionCanceledException
	if !errors.As(err, &tce) {
		return []any{normalizeSDKErrorString(err.Error())}
	}

	out := []any{normalizeSDKErrorString(err.Error())}

	for _, reason := range tce.CancellationReasons {
		name := "<none>"
		if reason.Item != nil {
			name = parityAttrValueString(reason.Item["name"])
		}

		out = append(out, aws.ToString(reason.Code), name, len(reason.Item))
	}

	return out
}

func parityCCFSetup(ctx context.Context, t *testing.T, client *dynamodb.Client, seed bool) {
	t.Helper()

	parityCreatePokemonTable(ctx, t, client)

	if seed {
		parityCreatePokemon(ctx, t, client, parityPokemon{ID: "001", Type: "grass", Name: "Bulbasaur"})
	}
}

func parityCCFPut(mode ccfMode, condition string, seed bool) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()
		ctx := context.Background()

		parityCCFSetup(ctx, t, client, seed)

		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:                           aws.String(parityPokemonTable),
			Item:                                parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "fire", Name: "Impostor"}),
			ConditionExpression:                 aws.String(condition),
			ReturnValuesOnConditionCheckFailure: mode,
		})
		require.Error(t, err)

		return parityCCFItemName(t, err)
	}
}

func parityCCFDelete(mode ccfMode, condition string, seed bool) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()
		ctx := context.Background()

		parityCCFSetup(ctx, t, client, seed)

		_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName:                           aws.String(parityPokemonTable),
			Key:                                 parityPokemonKey("001"),
			ConditionExpression:                 aws.String(condition),
			ReturnValuesOnConditionCheckFailure: mode,
		})
		require.Error(t, err)

		return parityCCFItemName(t, err)
	}
}

func parityCCFTransactPut(mode ccfMode, condition string, seed bool) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()
		ctx := context.Background()

		parityCCFSetup(ctx, t, client, seed)

		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{
					TableName:                           aws.String(parityPokemonTable),
					Item:                                parityMarshalPokemon(t, parityPokemon{ID: "001", Type: "fire", Name: "Impostor"}),
					ConditionExpression:                 aws.String(condition),
					ReturnValuesOnConditionCheckFailure: mode,
				}},
			},
		})
		require.Error(t, err)

		return parityCancelReasonSummary(t, err)
	}
}

func parityCCFTransactDelete(mode ccfMode, condition string, seed bool) func(t *testing.T, client *dynamodb.Client) any {
	return func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()
		ctx := context.Background()

		parityCCFSetup(ctx, t, client, seed)

		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []dynamodbtypes.TransactWriteItem{
				{Delete: &dynamodbtypes.Delete{
					TableName:                           aws.String(parityPokemonTable),
					Key:                                 parityPokemonKey("001"),
					ConditionExpression:                 aws.String(condition),
					ReturnValuesOnConditionCheckFailure: mode,
				}},
			},
		})
		require.Error(t, err)

		return parityCancelReasonSummary(t, err)
	}
}

func TestE2E_ReturnValuesOnConditionCheckFailure(t *testing.T) {
	const (
		allOld = dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld
		none   = dynamodbtypes.ReturnValuesOnConditionCheckFailureNone
		bogus  = ccfMode("BOGUS")
		// failsOnExisting fails when the item exists, failsOnMissing when it does not.
		failsOnExisting = "attribute_not_exists(id)"
		failsOnMissing  = "attribute_exists(id)"
	)

	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{"PutItemAllOld", parityCCFPut(allOld, failsOnExisting, true)},
		{"PutItemNone", parityCCFPut(none, failsOnExisting, true)},
		{"PutItemUnset", parityCCFPut("", failsOnExisting, true)},
		{"PutItemAllOldMissingItem", parityCCFPut(allOld, failsOnMissing, false)},
		{"PutItemInvalidValue", parityCCFPut(bogus, failsOnExisting, true)},
		{"DeleteItemAllOld", parityCCFDelete(allOld, failsOnExisting, true)},
		{"DeleteItemNone", parityCCFDelete(none, failsOnExisting, true)},
		{"DeleteItemUnset", parityCCFDelete("", failsOnExisting, true)},
		{"DeleteItemAllOldMissingItem", parityCCFDelete(allOld, failsOnMissing, false)},
		{"DeleteItemInvalidValue", parityCCFDelete(bogus, failsOnExisting, true)},
		{"TransactPutAllOld", parityCCFTransactPut(allOld, failsOnExisting, true)},
		{"TransactPutNone", parityCCFTransactPut(none, failsOnExisting, true)},
		{"TransactPutAllOldMissingItem", parityCCFTransactPut(allOld, failsOnMissing, false)},
		{"TransactPutInvalidValue", parityCCFTransactPut(bogus, failsOnExisting, true)},
		{"TransactDeleteAllOld", parityCCFTransactDelete(allOld, failsOnExisting, true)},
		{"TransactDeleteNone", parityCCFTransactDelete(none, failsOnExisting, true)},
		{"TransactDeleteAllOldMissingItem", parityCCFTransactDelete(allOld, failsOnMissing, false)},
		{"TransactDeleteInvalidValue", parityCCFTransactDelete(bogus, failsOnExisting, true)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
