package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// #250: the interpreter settings are shared with every table, so a setting changed after
// CreateTable reaches existing tables, and registering native expressions does not race.
// The server has no public interpreter knobs, so these tests drive the Client's fields.

func TestSettingsChangedAfterCreateTableReachTable(t *testing.T) {
	c := NewClient()

	lockTestTable(t, c, "main")

	c.mu.Lock()
	c.langInterpreter.Debug = true
	c.mu.Unlock()

	require.True(t, c.tables["main"].LangInterpreter.Debug)
}

func TestNativeRegistrationAfterCreateTableTakesEffect(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	lockTestTable(t, c, "main")

	c.mu.Lock()
	c.useNativeInterpreter = true
	c.tables["main"].UseNativeInterpreter = true
	c.mu.Unlock()

	c.nativeInterpreter.AddMatcher("main", interpreter.ExpressionTypeConditional, "always_false", func(_, _ map[string]*types.Item) bool {
		return false
	})

	_, err := c.PutItem(ctx, &PutItemInput{
		TableName:           aws.String("main"),
		Item:                lockTestItem("1"),
		ConditionExpression: aws.String("always_false"),
	})
	require.Error(t, err)
}

func TestNativeRegistrationRacesOperations(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	lockTestTable(t, c, "main")

	c.mu.Lock()
	c.tables["main"].UseNativeInterpreter = true
	c.mu.Unlock()

	native := c.nativeInterpreter
	native.AddMatcher("main", interpreter.ExpressionTypeConditional, "ok", func(_, _ map[string]*types.Item) bool { return true })
	native.AddMatcher("main", interpreter.ExpressionTypeKey, "id = :id", func(_, _ map[string]*types.Item) bool { return true })
	native.AddUpdater("main", "SET x = :v", func(_, _ map[string]*types.Item) {})

	const rounds = 100

	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range rounds {
			expr := fmt.Sprintf("expr-%d", i)

			native.AddMatcher("main", interpreter.ExpressionTypeFilter, expr, func(_, _ map[string]*types.Item) bool { return true })
			native.AddMatcher("main", interpreter.ExpressionTypeConditional, expr, func(_, _ map[string]*types.Item) bool { return true })
			native.AddUpdater("main", expr, func(_, _ map[string]*types.Item) {})
		}
	})

	wg.Go(func() {
		for i := range rounds {
			id := fmt.Sprintf("p-%d", i)

			_, err := c.PutItem(ctx, &PutItemInput{
				TableName:           aws.String("main"),
				Item:                lockTestItem(id),
				ConditionExpression: aws.String("ok"),
			})
			require.NoError(t, err)

			_, err = c.Query(ctx, &QueryInput{
				TableName:                 aws.String("main"),
				KeyConditionExpression:    aws.String("id = :id"),
				ExpressionAttributeValues: map[string]*AttributeValue{":id": {S: aws.String(id)}},
			})
			require.NoError(t, err)

			_, err = c.UpdateItem(ctx, &UpdateItemInput{
				TableName:                 aws.String("main"),
				Key:                       map[string]*AttributeValue{"id": {S: aws.String(id)}},
				UpdateExpression:          aws.String("SET x = :v"),
				ExpressionAttributeValues: map[string]*AttributeValue{":v": {S: aws.String("v")}},
			})
			require.NoError(t, err)
		}
	})

	wg.Wait()
}
