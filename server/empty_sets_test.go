package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An empty String, Number or Binary Set is rejected with Local's text, whichever operation
// carries it and wherever it sits (#294). A nil Go slice is an empty set on the wire.

var emptySetCases = []struct {
	kind string
	av   ddbtypes.AttributeValue
	msg  string
}{
	{"SS", &ddbtypes.AttributeValueMemberSS{Value: []string{}}, "One or more parameter values were invalid: An string set  may not be empty"},
	{"NS", &ddbtypes.AttributeValueMemberNS{Value: []string{}}, "One or more parameter values were invalid: An number set  may not be empty"},
	{"BS", &ddbtypes.AttributeValueMemberBS{Value: [][]byte{}}, "One or more parameter values were invalid: Binary sets should not be empty"},
	{"SS nil", &ddbtypes.AttributeValueMemberSS{}, "One or more parameter values were invalid: An string set  may not be empty"},
	{"NS nil", &ddbtypes.AttributeValueMemberNS{}, "One or more parameter values were invalid: An number set  may not be empty"},
	{"BS nil", &ddbtypes.AttributeValueMemberBS{}, "One or more parameter values were invalid: Binary sets should not be empty"},
}

func TestEmptySetsAreRejectedOnEveryWrite(t *testing.T) {
	ctx := context.Background()
	table := aws.String("numbers")

	for _, tc := range emptySetCases {
		t.Run(tc.kind, func(t *testing.T) {
			client := numberPrecisionClient(t)
			vals := map[string]ddbtypes.AttributeValue{":v": tc.av}
			eav := numberMsgEAV + tc.msg + " for key :v"

			shapes := map[string]ddbtypes.AttributeValue{
				"top level": tc.av,
				"list":      &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{tc.av}},
				"map":       &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"a": tc.av}},
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

			_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numberPrecisionKey(), UpdateExpression: aws.String("ADD x :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.Scan(ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.Query(ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p"), FilterExpression: aws.String("x = :v"),
				ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
					":p": &ddbtypes.AttributeValueMemberS{Value: "a"}, ":v": tc.av,
				},
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: table, Item: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []ddbtypes.TransactWriteItem{
				{ConditionCheck: &ddbtypes.ConditionCheck{
					TableName: table, Key: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
				}},
			}})
			requireNumberValidationError(t, err, eav)

			scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: table})
			require.NoError(t, err)
			require.Zero(t, scan.Count, "nothing was written")
		})
	}
}

// A set with a member and a member that is empty are stored.
func TestNonEmptySetsAreAccepted(t *testing.T) {
	ctx := context.Background()
	client := numberPrecisionClient(t)

	for _, av := range []ddbtypes.AttributeValue{
		&ddbtypes.AttributeValueMemberSS{Value: []string{""}},
		&ddbtypes.AttributeValueMemberNS{Value: []string{"1"}},
		&ddbtypes.AttributeValueMemberBS{Value: [][]byte{{}}},
		&ddbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
	} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("numbers"), Item: numberItemWith(av)})
		require.NoError(t, err)
	}
}
