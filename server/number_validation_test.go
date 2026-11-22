package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// N values are validated like DynamoDB Local does (#226), whichever operation carries them.

const (
	numberMsgInvalid   = "A value provided cannot be converted into a number"
	numberMsgOverflow  = "Number overflow. Attempting to store a number with magnitude larger than supported range"
	numberMsgUnderflow = "Number underflow. Attempting to store a number with magnitude smaller than supported range"
	numberMsgPrecision = "DynamoDB only supports precision up to 38 digits"
	numberMsgEAV       = "ExpressionAttributeValues contains invalid value: "
	numberDigits39     = "123456789012345678901234567890123456789"
)

var invalidNumberCases = []struct{ in, msg string }{
	{"abc", numberMsgInvalid},
	{"", numberMsgInvalid},
	{"1e2147483648", numberMsgInvalid},
	{"1e126", numberMsgOverflow},
	{"1e-131", numberMsgUnderflow},
	{numberDigits39, numberMsgPrecision},
}

func requireNumberValidationError(t *testing.T, err error, msg string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.ErrorCode(), err.Error())
	require.Contains(t, apiErr.ErrorMessage(), msg)
}

func numberItemWith(attr ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	item := numberPrecisionKey()
	item["x"] = attr

	return item
}

func TestNumbersAreValidatedOnEveryWrite(t *testing.T) {
	ctx := context.Background()
	table := aws.String("numbers")

	for _, tc := range invalidNumberCases {
		t.Run(tc.in, func(t *testing.T) {
			client := numberPrecisionClient(t)
			vals := map[string]ddbtypes.AttributeValue{":v": numberAV(tc.in)}
			eav := numberMsgEAV + tc.msg

			shapes := map[string]ddbtypes.AttributeValue{
				"top level": numberAV(tc.in),
				"list":      &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{numberAV(tc.in)}},
				"map":       &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"a": numberAV(tc.in)}},
				"set":       &ddbtypes.AttributeValueMemberNS{Value: []string{"1", tc.in}},
			}

			for _, attr := range shapes {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: table, Item: numberItemWith(attr)})
				requireNumberValidationError(t, err, tc.msg)

				_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]ddbtypes.WriteRequest{
					"numbers": {{PutRequest: &ddbtypes.PutRequest{Item: numberItemWith(attr)}}},
				}})
				requireNumberValidationError(t, err, tc.msg)

				_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []ddbtypes.TransactWriteItem{
					{Put: &ddbtypes.Put{TableName: table, Item: numberItemWith(attr)}},
				}})
				requireNumberValidationError(t, err, tc.msg)

				_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: table, Key: numberPrecisionKey(), UpdateExpression: aws.String("SET x = :v"),
					ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": attr},
				})
				requireNumberValidationError(t, err, numberMsgEAV+tc.msg)
			}

			badKey := map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "a"}, "sk": numberAV(tc.in)}

			_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: table, Key: badKey})
			requireNumberValidationError(t, err, tc.msg)

			_, err = client.GetItem(ctx, &dynamodb.GetItemInput{TableName: table, Key: badKey})
			requireNumberValidationError(t, err, tc.msg)

			_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]ddbtypes.WriteRequest{
				"numbers": {{DeleteRequest: &ddbtypes.DeleteRequest{Key: badKey}}},
			}})
			requireNumberValidationError(t, err, tc.msg)

			_, err = client.Scan(ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.Query(ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p AND sk = :v"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":p": &ddbtypes.AttributeValueMemberS{Value: "a"}, ":v": numberAV(tc.in),
				},
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: table, Key: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: table, Item: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []ddbtypes.TransactWriteItem{
				{Update: &ddbtypes.Update{
					TableName: table, Key: numberPrecisionKey(), UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: vals,
				}},
			}})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []ddbtypes.TransactWriteItem{
				{ConditionCheck: &ddbtypes.ConditionCheck{
					TableName: table, Key: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
				}},
			}})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []ddbtypes.TransactWriteItem{
				{Delete: &ddbtypes.Delete{TableName: table, Key: badKey}},
			}})
			requireNumberValidationError(t, err, tc.msg)

			scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: table})
			require.NoError(t, err)
			require.Zero(t, scan.Count, "nothing was written")
		})
	}
}

func TestNumbersAreAcceptedWhenDynamoDBStoresThem(t *testing.T) {
	ctx := context.Background()
	client := numberPrecisionClient(t)

	for _, in := range []string{"0", "-0", "5.", ".5", "+5", "1.50", "1e2", "1e125", "1e-130", numberDigits38, "1.00000000000000000000000000000000000000000"} {
		item := numberPrecisionKey()
		item["x"] = numberAV(in)

		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("numbers"), Item: item})
		require.NoError(t, err, in)
	}
}

// A transaction reports an invalid number anywhere in it before any other problem and before
// it writes anything.
func TestTransactWriteItemsReportsNumbersFirst(t *testing.T) {
	ctx := context.Background()
	table := aws.String("numbers")
	client := numberPrecisionClient(t)

	emptyKey := map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: ""}, "sk": numberAV("2")}
	good := numberPrecisionKey()
	bad := numberItemWith(numberAV("abc"))
	bad["sk"] = numberAV("3")

	tests := []struct {
		name  string
		items []ddbtypes.TransactWriteItem
		msg   string
	}{
		{"after a valid item", []ddbtypes.TransactWriteItem{
			{Put: &ddbtypes.Put{TableName: table, Item: good}},
			{Put: &ddbtypes.Put{TableName: table, Item: bad}},
		}, numberMsgInvalid},
		{"before an empty key", []ddbtypes.TransactWriteItem{
			{Put: &ddbtypes.Put{TableName: table, Item: emptyKey}},
			{Put: &ddbtypes.Put{TableName: table, Item: bad}},
		}, numberMsgInvalid},
		{"before a duplicate target", []ddbtypes.TransactWriteItem{
			{Put: &ddbtypes.Put{TableName: table, Item: numberItemWith(numberAV("1"))}},
			{Put: &ddbtypes.Put{TableName: table, Item: numberItemWith(numberAV("abc"))}},
		}, numberMsgInvalid},
		{"before ReturnValuesOnConditionCheckFailure", []ddbtypes.TransactWriteItem{
			{Put: &ddbtypes.Put{TableName: table, Item: bad, ReturnValuesOnConditionCheckFailure: "BOGUS"}},
		}, numberMsgInvalid},
		{"values of the expression before the item", []ddbtypes.TransactWriteItem{
			{Put: &ddbtypes.Put{
				TableName: table, Item: numberItemWith(numberAV("1e126")), ConditionExpression: aws.String("x = :v"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": numberAV("abc")},
			}},
		}, numberMsgEAV + numberMsgInvalid},
		{"first operation first", []ddbtypes.TransactWriteItem{
			{Put: &ddbtypes.Put{TableName: table, Item: numberItemWith(numberAV("1e126"))}},
			{Put: &ddbtypes.Put{
				TableName: table, Item: good, ConditionExpression: aws.String("x = :v"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": numberAV("abc")},
			}},
		}, numberMsgOverflow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: tt.items})
			requireNumberValidationError(t, err, tt.msg)
		})
	}

	scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: table})
	require.NoError(t, err)
	require.Zero(t, scan.Count)
}

// An UpdateItem reports an invalid number in its key before the shape of the key.
func TestUpdateItemReportsKeyNumbersBeforeKeyShape(t *testing.T) {
	client := numberPrecisionClient(t)
	key := map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: ""}, "sk": numberAV("abc")}

	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("numbers"), Key: key, UpdateExpression: aws.String("SET x = :v"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": numberAV("1")},
	})
	requireNumberValidationError(t, err, numberMsgInvalid)
}

// A batch write reports an invalid number anywhere in it before any other problem.
func TestBatchWriteItemReportsNumbersFirst(t *testing.T) {
	client := numberPrecisionClient(t)
	emptyKey := map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: ""}, "sk": numberAV("2")}

	_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{RequestItems: map[string][]ddbtypes.WriteRequest{
		"numbers": {
			{PutRequest: &ddbtypes.PutRequest{Item: emptyKey}},
			{PutRequest: &ddbtypes.PutRequest{Item: numberItemWith(numberAV("abc"))}},
		},
	}})
	requireNumberValidationError(t, err, numberMsgInvalid)
}

// PutItem reports a missing table, and an invalid number, before an invalid
// ReturnValuesOnConditionCheckFailure, as DynamoDB Local does.
func TestPutItemReturnValuesOnConditionCheckFailureComesAfterTableAndNumbers(t *testing.T) {
	ctx := context.Background()
	client := numberPrecisionClient(t)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("numbers"), Item: numberItemWith(numberAV("abc")), ReturnValuesOnConditionCheckFailure: "BOGUS",
	})
	requireNumberValidationError(t, err, numberMsgInvalid)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("missing"), Item: numberItemWith(numberAV("1")), ReturnValuesOnConditionCheckFailure: "BOGUS",
	})
	require.ErrorContains(t, err, "non-existent table")
}
