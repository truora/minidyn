package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An update that creates the item has no old item: ALL_OLD returns no attributes (#214).
func TestServerUpdateItemAllOldOnMissingItem(t *testing.T) {
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

	update := func(name string) *UpdateItemOutput {
		out, uerr := srv.client.UpdateItem(ctx, &UpdateItemInput{
			TableName:                 aws.String("pokemons"),
			Key:                       map[string]*AttributeValue{"id": {S: aws.String("025")}},
			ReturnValues:              ddbtypes.ReturnValueAllOld,
			UpdateExpression:          aws.String("SET #n = :n"),
			ExpressionAttributeNames:  map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]*AttributeValue{":n": {S: aws.String(name)}},
		})
		c.NoError(uerr)

		return out
	}

	c.Empty(update("Pichu").Attributes)

	c.Equal(map[string]*AttributeValue{
		"id":   {S: aws.String("025")},
		"name": {S: aws.String("Pichu")},
	}, update("Pikachu").Attributes)
}

// An invalid ReturnValues gets the text PutItem and DeleteItem use, and DynamoDB reports it
// before the key and the expressions.
func TestServerUpdateItemInvalidReturnValues(t *testing.T) {
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

	key := func(id string) map[string]*AttributeValue {
		return map[string]*AttributeValue{"id": {S: aws.String(id)}}
	}
	vals := map[string]*AttributeValue{":v": {S: aws.String("x")}}

	tests := map[string]*UpdateItemInput{
		"valid key":       {Key: key("025"), UpdateExpression: aws.String("SET n = :v"), ExpressionAttributeValues: vals},
		"empty key":       {Key: key(""), UpdateExpression: aws.String("SET n = :v"), ExpressionAttributeValues: vals},
		"undefined value": {Key: key("025"), UpdateExpression: aws.String("SET n = :v")},
		"undeclared name": {Key: key("025"), UpdateExpression: aws.String("SET #n = :v"), ExpressionAttributeValues: vals},
		"syntax error":    {Key: key("025"), UpdateExpression: aws.String("SET")},
		"invalid RVOCCF": {
			Key: key(""), UpdateExpression: aws.String("SET n = :v"), ExpressionAttributeValues: vals,
			ReturnValuesOnConditionCheckFailure: "BOGUS",
		},
	}

	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			in.TableName, in.ReturnValues = aws.String("pokemons"), "BOGUS"

			_, uerr := srv.client.UpdateItem(ctx, in)

			require.ErrorContains(t, uerr, "ValidationException: Return values set to invalid value")
			require.NotContains(t, uerr.Error(), "validation error detected")
		})
	}
}

// An unknown table is reported before an invalid ReturnValues.
func TestServerUpdateItemUnknownTableBeforeInvalidReturnValues(t *testing.T) {
	srv := NewServer()

	_, err := srv.client.UpdateItem(context.Background(), &UpdateItemInput{
		TableName:        aws.String("no-such-table"),
		Key:              map[string]*AttributeValue{"id": {S: aws.String("025")}},
		UpdateExpression: aws.String("SET n = :v"),
		ReturnValues:     "BOGUS",
	})

	require.ErrorContains(t, err, "ResourceNotFoundException")
}

// A list index is a literal: DynamoDB Local rejects a placeholder between the brackets with a
// syntax error that names the token, before it checks the placeholders, so an undefined value, an
// unused value and an unused name do not change the answer (#164, #267).
func TestServerUpdateItemPlaceholderListIndex(t *testing.T) {
	cli := listIndexServerClient(t)

	str := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
	num := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: v} }

	tests := map[string]*dynamodb.UpdateItemInput{
		"placeholder": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":i": num("1"), ":v": str("w")},
		},
		"negative value": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":i": num("-1"), ":v": str("w")},
		},
		"undefined value": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":v": str("w")},
		},
		"unused value": {
			UpdateExpression: aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":i": num("1"), ":v": str("w"), ":z": str("unused"),
			},
		},
		"unused name": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeNames:  map[string]string{"#z": "z"},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":i": num("1"), ":v": str("w")},
		},
		"condition with an unused value": {
			UpdateExpression:    aws.String("SET n = :v"),
			ConditionExpression: aws.String("l[:i] = :v"),
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":i": num("1"), ":v": str("w"), ":z": str("unused"),
			},
		},
	}

	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			in.TableName = aws.String("lists")
			in.Key = map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "a"}}

			_, err := cli.UpdateItem(context.Background(), in)
			require.ErrorContains(t, err, "ValidationException: Invalid ")
			require.ErrorContains(t, err, `Expression: Syntax error; token: ":i", near: "[:i]"`)
		})
	}
}
