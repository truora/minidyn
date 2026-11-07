package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func undefinedValueMsg(label, value string) string {
	return "api error ValidationException: Invalid " + label +
		": An expression attribute value used in expression is not defined; attribute value: " + value
}

// An undefined placeholder is reported even when another declared value is unused, and on an
// empty table, as DynamoDB does (#190).
func TestServerUndefinedExpressionAttributeValues(t *testing.T) {
	c := require.New(t)
	srv := NewServer()
	ctx := context.Background()

	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	table := aws.String("pokemons")
	key := map[string]*AttributeValue{"id": {S: aws.String("001")}}
	unused := map[string]*AttributeValue{":v": {S: aws.String("x")}}

	_, err = srv.client.UpdateItem(ctx, &UpdateItemInput{
		TableName: table, Key: key, UpdateExpression: aws.String("SET a = :nope"), ExpressionAttributeValues: unused,
	})
	c.EqualError(err, undefinedValueMsg("UpdateExpression", ":nope"))

	_, err = srv.client.UpdateItem(ctx, &UpdateItemInput{
		TableName: table, Key: key, UpdateExpression: aws.String("REMOVE a"), ConditionExpression: aws.String("b = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))

	_, err = srv.client.PutItem(ctx, &PutItemInput{
		TableName: table, Item: key, ConditionExpression: aws.String("a = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))

	_, err = srv.client.DeleteItem(ctx, &DeleteItemInput{
		TableName: table, Key: key, ConditionExpression: aws.String("a = :nope"),
	})
	c.EqualError(err, undefinedValueMsg("ConditionExpression", ":nope"))

	_, err = srv.client.Scan(ctx, &ScanInput{TableName: table, FilterExpression: aws.String("a = :nope")})
	c.EqualError(err, undefinedValueMsg("FilterExpression", ":nope"))

	_, err = srv.client.Query(ctx, &QueryInput{TableName: table, KeyConditionExpression: aws.String("id = :nope")})
	c.EqualError(err, undefinedValueMsg("KeyConditionExpression", ":nope"))
}

func TestValidateExpressionAttributesSkipsUnusedWhenUndefined(t *testing.T) {
	c := require.New(t)

	c.NoError(validateExpressionAttributes(nil, []string{":v"}, "b = :nope"))
	c.Error(validateExpressionAttributes(nil, []string{":v"}, "a = b"))
}
