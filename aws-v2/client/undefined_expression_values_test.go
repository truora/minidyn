package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const undefinedValuePrefix = "api error ValidationException: Invalid "

func undefinedValueMsg(label, value string) string {
	return undefinedValuePrefix + label +
		": An expression attribute value used in expression is not defined; attribute value: " + value
}

// An undefined placeholder is reported even when another declared value is unused, and on an
// empty table, as DynamoDB does (#190).
func TestUndefinedExpressionAttributeValues(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	key := map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "undefined-values"}}
	unused := map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"}}

	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName), Key: key, UpdateExpression: aws.String("SET a = :nope"),
		ExpressionAttributeValues: unused,
	})
	c.EqualError(err, undefinedValueMsg("UpdateExpression", ":nope"))

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName), Key: key, UpdateExpression: aws.String("REMOVE a"),
		ConditionExpression: aws.String("b = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName), Item: key, ConditionExpression: aws.String("a = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName), Key: key, ConditionExpression: aws.String("a = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))

	_, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName), FilterExpression: aws.String("a = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("FilterExpression", ":nope"))

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(tableName), KeyConditionExpression: aws.String("id = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("KeyConditionExpression", ":nope"))

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(tableName), Key: key, ConditionExpression: aws.String("a = :nope"),
		}}},
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))
}

func TestValidateExpressionAttributesSkipsUnusedWhenUndefined(t *testing.T) {
	c := require.New(t)
	values := map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"}}

	c.NoError(validateExpressionAttributes(nil, values, "b = :nope"))
	c.Error(validateExpressionAttributes(nil, values, "a = b"))
}
