package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An empty String, Number or Binary Set is rejected with Local's text, whichever operation
// carries it and wherever it sits (#294). A nil Go slice is an empty set on the wire.

var emptySetCases = []struct {
	kind string
	av   dynamodbtypes.AttributeValue
	msg  string
}{
	{"SS", &dynamodbtypes.AttributeValueMemberSS{Value: []string{}}, "One or more parameter values were invalid: An string set  may not be empty"},
	{"NS", &dynamodbtypes.AttributeValueMemberNS{Value: []string{}}, "One or more parameter values were invalid: An number set  may not be empty"},
	{"BS", &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{}}, "One or more parameter values were invalid: Binary sets should not be empty"},
	{"SS nil", &dynamodbtypes.AttributeValueMemberSS{}, "One or more parameter values were invalid: An string set  may not be empty"},
	{"NS nil", &dynamodbtypes.AttributeValueMemberNS{}, "One or more parameter values were invalid: An number set  may not be empty"},
	{"BS nil", &dynamodbtypes.AttributeValueMemberBS{}, "One or more parameter values were invalid: Binary sets should not be empty"},
}

func TestEmptySetsAreRejectedOnEveryWrite(t *testing.T) {
	ctx := context.Background()
	table := aws.String("numbers")

	for _, tc := range emptySetCases {
		t.Run(tc.kind, func(t *testing.T) {
			client := numberPrecisionClient(t)
			vals := map[string]dynamodbtypes.AttributeValue{":v": tc.av}
			eav := numberMsgEAV + tc.msg + " for key :v"

			shapes := map[string]dynamodbtypes.AttributeValue{
				"top level": tc.av,
				"list":      &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{tc.av}},
				"map":       &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"a": tc.av}},
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
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":p": &dynamodbtypes.AttributeValueMemberS{Value: "a"}, ":v": tc.av,
				},
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: table, Item: numberPrecisionKey(), ConditionExpression: aws.String("x = :v"), ExpressionAttributeValues: vals,
			})
			requireNumberValidationError(t, err, eav)

			_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{ConditionCheck: &dynamodbtypes.ConditionCheck{
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

	for _, av := range []dynamodbtypes.AttributeValue{
		&dynamodbtypes.AttributeValueMemberSS{Value: []string{""}},
		&dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}},
		&dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{{}}},
		&dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("a")}},
	} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("numbers"), Item: numberItemWith(av)})
		require.NoError(t, err)
	}
}
