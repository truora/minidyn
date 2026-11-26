package client

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// #250: the interpreter settings are shared with every table, so a setting changed after
// CreateTable reaches existing tables, and registering native expressions does not race.

func TestActivateDebugReachesExistingAndLaterTables(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	require.NoError(t, AddTable(ctx, c, "before", "id", ""))
	require.False(t, c.tables["before"].LangInterpreter.Debug)

	c.ActivateDebug()

	require.NoError(t, AddTable(ctx, c, "after", "id", ""))
	require.True(t, c.tables["before"].LangInterpreter.Debug, "table created before ActivateDebug")
	require.True(t, c.tables["after"].LangInterpreter.Debug, "table created after ActivateDebug")
}

func TestNativeRegistrationAfterCreateTableTakesEffect(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	require.NoError(t, AddTable(ctx, c, "main", "id", ""))
	c.ActivateNativeInterpreter()

	native := c.GetNativeInterpreter()
	native.AddMatcher("main", interpreter.ExpressionTypeConditional, "always_false", func(_, _ map[string]*types.Item) bool {
		return false
	})
	native.AddUpdater("main", "SET x = :v", func(item, attrs map[string]*types.Item) {
		item["x"] = attrs[":v"]
	})

	_, err := c.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String("main"),
		Item:                lockTestItem("1"),
		ConditionExpression: aws.String("always_false"),
	})
	require.Error(t, err)

	_, err = c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("main"), Item: lockTestItem("1")})
	require.NoError(t, err)

	_, err = c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String("main"),
		Key:                       lockTestKey("1"),
		UpdateExpression:          aws.String("SET x = :v"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "native"}},
	})
	require.NoError(t, err)

	out, err := c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("main"), Key: lockTestKey("1")})
	require.NoError(t, err)
	require.Equal(t, &dynamodbtypes.AttributeValueMemberS{Value: "native"}, out.Item["x"])
}

func TestNativeRegistrationRacesOperations(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	require.NoError(t, AddTable(ctx, c, "main", "id", ""))
	c.ActivateNativeInterpreter()

	native := c.GetNativeInterpreter()
	native.AddMatcher("main", interpreter.ExpressionTypeConditional, "ok", func(_, _ map[string]*types.Item) bool { return true })
	native.AddMatcher("main", interpreter.ExpressionTypeFilter, "ok", func(_, _ map[string]*types.Item) bool { return true })
	native.AddMatcher("main", interpreter.ExpressionTypeKey, "id = :id", func(_, _ map[string]*types.Item) bool { return true })
	native.AddUpdater("main", "SET x = :v", func(item, attrs map[string]*types.Item) { item["x"] = attrs[":v"] })

	const rounds = 100

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range rounds {
			expr := fmt.Sprintf("expr-%d", i)

			native.AddMatcher("main", interpreter.ExpressionTypeFilter, expr, func(_, _ map[string]*types.Item) bool { return true })
			native.AddMatcher("main", interpreter.ExpressionTypeConditional, expr, func(_, _ map[string]*types.Item) bool { return true })
			native.AddMatcher("main", interpreter.ExpressionTypeKey, expr, func(_, _ map[string]*types.Item) bool { return true })
			native.AddUpdater("main", expr, func(_, _ map[string]*types.Item) {})
		}
	})

	wg.Go(func() {
		for i := range rounds {
			id := fmt.Sprintf("p-%d", i)

			_, err := c.PutItem(ctx, &dynamodb.PutItemInput{
				TableName:           aws.String("main"),
				Item:                lockTestItem(id),
				ConditionExpression: aws.String("ok"),
			})
			assert.NoError(t, err)

			_, err = c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:                 aws.String("main"),
				Key:                       lockTestKey(id),
				UpdateExpression:          aws.String("SET x = :v"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "u"}},
			})
			assert.NoError(t, err)

			_, err = c.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String("main"),
				KeyConditionExpression:    aws.String("id = :id"),
				FilterExpression:          aws.String("ok"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":id": &dynamodbtypes.AttributeValueMemberS{Value: id}},
			})
			assert.NoError(t, err)
		}
	})

	wg.Wait()
}
