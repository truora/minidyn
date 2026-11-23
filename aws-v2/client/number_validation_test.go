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

func numberItemWith(attr dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
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
			vals := map[string]dynamodbtypes.AttributeValue{":v": numberAV(tc.in)}
			eav := numberMsgEAV + tc.msg

			shapes := map[string]dynamodbtypes.AttributeValue{
				"top level": numberAV(tc.in),
				"list":      &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{numberAV(tc.in)}},
				"map":       &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"a": numberAV(tc.in)}},
				"set":       &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", tc.in}},
			}

			for _, attr := range shapes {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: table, Item: numberItemWith(attr)})
				requireNumberValidationError(t, err, tc.msg)

				_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
					"numbers": {{PutRequest: &dynamodbtypes.PutRequest{Item: numberItemWith(attr)}}},
				}})
				requireNumberValidationError(t, err, tc.msg)

				_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
					{Put: &dynamodbtypes.Put{TableName: table, Item: numberItemWith(attr)}},
				}})
				requireNumberValidationError(t, err, tc.msg)

				_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: table, Key: numberPrecisionKey(), UpdateExpression: aws.String("SET x = :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": attr},
				})
				requireNumberValidationError(t, err, numberMsgEAV+tc.msg)
			}

			badKey := map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}, "sk": numberAV(tc.in)}

			_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: table, Key: badKey})
			requireNumberValidationError(t, err, tc.msg)

			_, err = client.GetItem(ctx, &dynamodb.GetItemInput{TableName: table, Key: badKey})
			requireNumberValidationError(t, err, tc.msg)

			_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				"numbers": {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: badKey}}},
			}})
			requireNumberValidationError(t, err, tc.msg)

			_, err = client.Scan(ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.Query(ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p AND sk = :v"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":p": &dynamodbtypes.AttributeValueMemberS{Value: "a"}, ":v": numberAV(tc.in),
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

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Update: &dynamodbtypes.Update{
					TableName: table, Key: numberPrecisionKey(), UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: vals,
				}},
			}})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{ConditionCheck: &dynamodbtypes.ConditionCheck{
					TableName: table, Key: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
				}},
			}})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Delete: &dynamodbtypes.Delete{TableName: table, Key: badKey}},
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

	emptyKey := map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: ""}, "sk": numberAV("2")}
	good := numberPrecisionKey()
	bad := numberItemWith(numberAV("abc"))
	bad["sk"] = numberAV("3")

	tests := []struct {
		name  string
		items []dynamodbtypes.TransactWriteItem
		msg   string
	}{
		{"after a valid item", []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: table, Item: good}},
			{Put: &dynamodbtypes.Put{TableName: table, Item: bad}},
		}, numberMsgInvalid},
		{"before an empty key", []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: table, Item: emptyKey}},
			{Put: &dynamodbtypes.Put{TableName: table, Item: bad}},
		}, numberMsgInvalid},
		{"before a duplicate target", []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: table, Item: numberItemWith(numberAV("1"))}},
			{Put: &dynamodbtypes.Put{TableName: table, Item: numberItemWith(numberAV("abc"))}},
		}, numberMsgInvalid},
		{"before ReturnValuesOnConditionCheckFailure", []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: table, Item: bad, ReturnValuesOnConditionCheckFailure: "BOGUS"}},
		}, numberMsgInvalid},
		{"values of the expression before the item", []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{
				TableName: table, Item: numberItemWith(numberAV("1e126")), ConditionExpression: aws.String("x = :v"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": numberAV("abc")},
			}},
		}, numberMsgEAV + numberMsgInvalid},
		{"first operation first", []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: table, Item: numberItemWith(numberAV("1e126"))}},
			{Put: &dynamodbtypes.Put{
				TableName: table, Item: good, ConditionExpression: aws.String("x = :v"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": numberAV("abc")},
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
	key := map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: ""}, "sk": numberAV("abc")}

	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String("numbers"), Key: key, UpdateExpression: aws.String("SET x = :v"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": numberAV("1")},
	})
	requireNumberValidationError(t, err, numberMsgInvalid)
}

// A batch write reports an invalid number anywhere in it before any other problem.
func TestBatchWriteItemReportsNumbersFirst(t *testing.T) {
	client := numberPrecisionClient(t)
	emptyKey := map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: ""}, "sk": numberAV("2")}

	_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
		"numbers": {
			{PutRequest: &dynamodbtypes.PutRequest{Item: emptyKey}},
			{PutRequest: &dynamodbtypes.PutRequest{Item: numberItemWith(numberAV("abc"))}},
		},
	}})
	requireNumberValidationError(t, err, numberMsgInvalid)
}
