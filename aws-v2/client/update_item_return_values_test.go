package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// An update that creates the item has no old item: ALL_OLD returns no attributes (#214).
func TestUpdateItemAllOldOnMissingItem(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	update := func(name string) *dynamodb.UpdateItemOutput {
		out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]dynamodbtypes.AttributeValue{
				"id": &dynamodbtypes.AttributeValueMemberS{Value: "025"},
			},
			ReturnValues:             dynamodbtypes.ReturnValueAllOld,
			UpdateExpression:         aws.String("SET #n = :n"),
			ExpressionAttributeNames: map[string]string{"#n": "name"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":n": &dynamodbtypes.AttributeValueMemberS{Value: name},
			},
		})
		c.NoError(err)

		return out
	}

	c.Empty(update("Pichu").Attributes)

	c.Equal(map[string]dynamodbtypes.AttributeValue{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: "025"},
		"name": &dynamodbtypes.AttributeValueMemberS{Value: "Pichu"},
	}, update("Pikachu").Attributes)
}

// An invalid ReturnValues gets the text PutItem and DeleteItem use, and DynamoDB reports it
// before the key and the expressions.
func TestUpdateItemInvalidReturnValues(t *testing.T) {
	client := setupClient(tableName)
	require.NoError(t, ensurePokemonTable(client))

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	tests := map[string]*dynamodb.UpdateItemInput{
		"valid key": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("x")},
		},
		"empty key": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("")}, UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("x")},
		},
		"undefined value": {Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET n = :v")},
		"undeclared name": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET #n = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("x")},
		},
		"syntax error": {Key: map[string]dynamodbtypes.AttributeValue{"id": str("025")}, UpdateExpression: aws.String("SET")},
		"invalid RVOCCF": {
			Key: map[string]dynamodbtypes.AttributeValue{"id": str("")}, UpdateExpression: aws.String("SET n = :v"),
			ExpressionAttributeValues:           map[string]dynamodbtypes.AttributeValue{":v": str("x")},
			ReturnValuesOnConditionCheckFailure: "BOGUS",
		},
	}

	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			in.TableName, in.ReturnValues = aws.String(tableName), "BOGUS"

			_, err := client.UpdateItem(context.Background(), in)

			require.ErrorContains(t, err, "ValidationException: Return values set to invalid value")
			require.NotContains(t, err.Error(), "validation error detected")
		})
	}
}

// An unknown table is reported before an invalid ReturnValues.
func TestUpdateItemUnknownTableBeforeInvalidReturnValues(t *testing.T) {
	client := setupClient(tableName)

	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String("no-such-table"),
		Key:              map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "025"}},
		UpdateExpression: aws.String("SET n = :v"),
		ReturnValues:     "BOGUS",
	})

	require.ErrorContains(t, err, "ResourceNotFoundException")
}

// A list index is a literal: DynamoDB Local rejects a placeholder between the brackets with a
// syntax error that names the token, before it checks the placeholders, so an undefined value, an
// unused value and an unused name do not change the answer (#164, #267).
func TestUpdateItemPlaceholderListIndex(t *testing.T) {
	client := listIndexClient(t)

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	num := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

	tests := map[string]*dynamodb.UpdateItemInput{
		"placeholder": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":i": num("1"), ":v": str("w")},
		},
		"negative value": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":i": num("-1"), ":v": str("w")},
		},
		"undefined value": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": str("w")},
		},
		"unused value": {
			UpdateExpression: aws.String("SET l[:i] = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":i": num("1"), ":v": str("w"), ":z": str("unused"),
			},
		},
		"unused name": {
			UpdateExpression:          aws.String("SET l[:i] = :v"),
			ExpressionAttributeNames:  map[string]string{"#z": "z"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":i": num("1"), ":v": str("w")},
		},
		"condition with an unused value": {
			UpdateExpression:    aws.String("SET n = :v"),
			ConditionExpression: aws.String("l[:i] = :v"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":i": num("1"), ":v": str("w"), ":z": str("unused"),
			},
		},
	}

	for name, in := range tests {
		t.Run(name, func(t *testing.T) {
			in.TableName, in.Key = aws.String(rangeTableName), listIndexKey()

			_, err := client.UpdateItem(context.Background(), in)
			require.ErrorContains(t, err, "ValidationException: Invalid ")
			require.ErrorContains(t, err, `Expression: Syntax error; token: ":i", near: "[:i]"`)
		})
	}
}

// A literal list index above 2147483647, and one with a leading zero, are rejected before the
// placeholders are checked, as DynamoDB Local does (#164): an unused value, an unused name and
// an undefined value do not change the answer, and the boundary index is accepted.
func TestUpdateItemLiteralListIndexRules(t *testing.T) {
	cli := listIndexClient(t)

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	const (
		rangeText = "ValidationException: Invalid UpdateExpression: List index is not within the allowable range; index: [2147483648]"
		zeroText  = `ValidationException: Invalid UpdateExpression: Syntax error; token: "0", near: "007"`
	)

	tests := map[string]struct {
		expr   string
		names  map[string]string
		values map[string]dynamodbtypes.AttributeValue
		want   string
	}{
		"range": {"SET l[2147483648] = :v", nil, map[string]dynamodbtypes.AttributeValue{":v": str("w")}, rangeText},
		"range with an unused value": {
			"SET l[2147483648] = :v", nil, map[string]dynamodbtypes.AttributeValue{":v": str("w"), ":z": str("unused")}, rangeText,
		},
		"range with an unused name": {
			"SET l[2147483648] = :v", map[string]string{"#z": "z"}, map[string]dynamodbtypes.AttributeValue{":v": str("w")}, rangeText,
		},
		"range with an undefined value": {"SET l[2147483648] = :v", nil, nil, rangeText},
		"huge range": {
			"SET l[99999999999999999999] = :v", nil,
			map[string]dynamodbtypes.AttributeValue{":v": str("w")},
			"ValidationException: Invalid UpdateExpression: List index is not within the allowable range; index: [99999999999999999999]",
		},
		"leading zero": {"SET l[007] = :v", nil, map[string]dynamodbtypes.AttributeValue{":v": str("w")}, zeroText},
		"leading zero with an unused value": {
			"SET l[007] = :v", nil, map[string]dynamodbtypes.AttributeValue{":v": str("w"), ":z": str("unused")}, zeroText,
		},
		"leading zero with an unused name": {
			"SET l[007] = :v", map[string]string{"#z": "z"}, map[string]dynamodbtypes.AttributeValue{":v": str("w")}, zeroText,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
				TableName: aws.String(rangeTableName), Key: listIndexKey(), UpdateExpression: aws.String(tt.expr),
				ExpressionAttributeNames: tt.names, ExpressionAttributeValues: tt.values,
			})
			require.ErrorContains(t, err, tt.want)
		})
	}

	_, err := cli.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String(rangeTableName), Key: listIndexKey(), UpdateExpression: aws.String("REMOVE l[2147483647]"),
	})
	require.NoError(t, err)
}
