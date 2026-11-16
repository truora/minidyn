package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const numberDigits38 = "12345678901234567890123456789012345678"

func numberAV(s string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberN{Value: s}
}

func numberPrecisionClient(t *testing.T) *Client {
	t.Helper()

	client := NewClient()
	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("numbers"),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: dynamodbtypes.ScalarAttributeTypeN},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return client
}

func numberPrecisionKey() map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}, "sk": numberAV(numberDigits38)}
}

// A GetItem with a ProjectionExpression and an UpdateItem that creates the item keep the 38
// digits of an N key.
func TestNumberPrecisionProjectionAndUpsert(t *testing.T) {
	ctx := context.Background()
	client := numberPrecisionClient(t)

	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String("numbers"),
		Key:                       numberPrecisionKey(),
		UpdateExpression:          aws.String("SET n = :n"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":n": numberAV(numberDigits38)},
	})
	require.NoError(t, err)

	for _, projection := range []string{"", "sk, n"} {
		in := &dynamodb.GetItemInput{TableName: aws.String("numbers"), Key: numberPrecisionKey()}
		if projection != "" {
			in.ProjectionExpression = aws.String(projection)
		}

		out, getErr := client.GetItem(ctx, in)
		require.NoError(t, getErr)
		require.Equal(t, numberAV(numberDigits38), out.Item["sk"], projection)
		require.Equal(t, numberAV(numberDigits38), out.Item["n"], projection)
	}

	scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("numbers"), ProjectionExpression: aws.String("sk, n")})
	require.NoError(t, err)
	require.Equal(t, numberAV(numberDigits38), scan.Items[0]["sk"])
}

// Arithmetic is exact to 38 digits and fails with DynamoDB's message beyond.
func TestNumberPrecisionArithmetic(t *testing.T) {
	ctx := context.Background()
	client := numberPrecisionClient(t)

	update := func(expr, v string) (*dynamodb.UpdateItemOutput, error) {
		return client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String("numbers"),
			Key:                       numberPrecisionKey(),
			UpdateExpression:          aws.String(expr),
			ReturnValues:              dynamodbtypes.ReturnValueUpdatedNew,
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": numberAV(v)},
		})
	}

	out, err := update("SET n = :v", numberDigits38)
	require.NoError(t, err)
	require.Equal(t, numberAV(numberDigits38), out.Attributes["n"])

	out, err = update("SET n = n + :v", "1")
	require.NoError(t, err)
	require.Equal(t, numberAV("12345678901234567890123456789012345679"), out.Attributes["n"])

	_, err = update("ADD n :v", "0.5")
	require.ErrorContains(t, err, "ValidationException: DynamoDB only supports precision up to 38 digits")
}
