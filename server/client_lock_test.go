package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// D7: a public operation takes the client lock once, so none of these may race or deadlock.

func lockTestTable(t *testing.T, c *Client, name string) {
	t.Helper()

	require.NoError(t, createLockTestTable(c, name))
}

func createLockTestTable(c *Client, name string) error {
	_, err := c.CreateTable(context.Background(), &CreateTableInput{
		TableName:   aws.String(name),
		KeySchema:   []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
	})

	return err
}

func lockTestItem(id string) map[string]*AttributeValue {
	return map[string]*AttributeValue{"id": {S: aws.String(id)}, "type": {S: aws.String("t")}}
}

func lockTestKey(id string) map[string]*AttributeValue {
	return map[string]*AttributeValue{"id": {S: aws.String(id)}}
}

func TestClientTableOperationsRaceItemOperations(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	lockTestTable(t, c, "main")
	lockTestTable(t, c, "other")

	const rounds = 60

	var wg sync.WaitGroup

	run := func(fn func(i int)) {
		wg.Go(func() {
			for i := range rounds {
				fn(i)
			}
		})
	}

	run(func(i int) {
		name := aws.String("main")
		idx := fmt.Sprintf("gsi-%d", i)

		_, _ = c.UpdateTable(ctx, &UpdateTableInput{
			TableName: name,
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("type"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
			GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{
				IndexName:  aws.String(idx),
				KeySchema:  []ddbtypes.KeySchemaElement{{AttributeName: aws.String("type"), KeyType: ddbtypes.KeyTypeHash}},
				Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
			}}},
		})
		_, _ = c.UpdateTable(ctx, &UpdateTableInput{
			TableName: name,
			GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{{
				Delete: &ddbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(idx)},
			}},
		})
	})
	run(func(i int) {
		name := aws.String(fmt.Sprintf("tmp-%d", i))

		_, _ = c.CreateTable(ctx, &CreateTableInput{
			TableName:            name,
			KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
			AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
			BillingMode:          ddbtypes.BillingModePayPerRequest,
		})
		_, _ = c.DescribeTable(ctx, &DescribeTableInput{TableName: name})
		_, _ = c.DeleteTable(ctx, &DeleteTableInput{TableName: name})
	})
	run(func(int) { _, _ = c.DescribeTable(ctx, &DescribeTableInput{TableName: aws.String("main")}) })
	run(func(i int) {
		id := fmt.Sprintf("p-%d", i)

		_, _ = c.PutItem(ctx, &PutItemInput{TableName: aws.String("main"), Item: lockTestItem(id)})
		_, _ = c.GetItem(ctx, &GetItemInput{TableName: aws.String("main"), Key: lockTestKey(id)})
		_, _ = c.Scan(ctx, &ScanInput{TableName: aws.String("main")})
	})
	run(func(i int) {
		id := fmt.Sprintf("b-%d", i)

		_, _ = c.BatchWriteItem(ctx, &BatchWriteItemInput{RequestItems: map[string][]WriteRequest{
			"main":  {{PutRequest: &PutRequest{Item: lockTestItem(id)}}},
			"other": {{PutRequest: &PutRequest{Item: lockTestItem(id)}}},
		}})
		_, _ = c.BatchGetItem(ctx, &BatchGetItemInput{RequestItems: map[string]KeysAndAttributes{
			"main":  {Keys: []map[string]*AttributeValue{lockTestKey(id)}},
			"other": {Keys: []map[string]*AttributeValue{lockTestKey(id)}},
		}})
		_, _ = c.TransactGetItems(ctx, &TransactGetItemsInput{TransactItems: []TransactGetItem{
			{Get: &Get{TableName: aws.String("main"), Key: lockTestKey(id)}},
			{Get: &Get{TableName: aws.String("other"), Key: lockTestKey(id)}},
		}})
		_, _ = c.TransactWriteItems(ctx, &TransactWriteItemsInput{TransactItems: []TransactWriteItem{
			{Put: &Put{TableName: aws.String("main"), Item: lockTestItem("t-" + id)}},
		}})
	})
	run(func(int) {
		c.setFailureCondition(nil)
		c.setIndexActivationDelay(0)
		_ = c.ClearTable("other")
	})

	run(func(i int) {
		id := fmt.Sprintf("p-%d", i)

		_, _ = c.UpdateItem(ctx, &UpdateItemInput{
			TableName:                 aws.String("main"),
			Key:                       lockTestKey(id),
			UpdateExpression:          aws.String("SET #t = :v"),
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]*AttributeValue{":v": {S: aws.String("u")}},
		})
		_, _ = c.DeleteItem(ctx, &DeleteItemInput{TableName: aws.String("main"), Key: lockTestKey(id)})
		_, _ = c.Query(ctx, &QueryInput{
			TableName:                 aws.String("main"),
			KeyConditionExpression:    aws.String("id = :id"),
			ExpressionAttributeValues: map[string]*AttributeValue{":id": {S: aws.String(id)}},
		})
		_, _ = c.ListTables(ctx, &ListTablesInput{})
	})
	run(func(i int) {
		// The matcher must not call the client: it runs with the client lock held.
		c.setTableFailureCondition("ghost", "", nil)
		c.setUnprocessedMatcher("other", func(n int, _ map[string]*AttributeValue) bool { return (n+i)%2 == 0 })
		c.clearUnprocessedMatchers()
		c.ClearAllTables()

		if i%10 == 0 {
			c.Reset()

			_ = createLockTestTable(c, "main")
			_ = createLockTestTable(c, "other")
		}
	})

	wg.Wait()
}

// The probe runs inside the unprocessed-item matcher, which the batch calls with the client
// lock held. TryLock succeeds only if the batch released the lock in the middle of the call.
func probeLockReleased(c *Client) (match func(int, map[string]*AttributeValue) bool, released, calls *atomic.Int32) {
	released, calls = new(atomic.Int32), new(atomic.Int32)

	match = func(int, map[string]*AttributeValue) bool {
		calls.Add(1)

		if c.mu.TryLock() {
			c.mu.Unlock()
			released.Add(1)
		}

		return false
	}

	return match, released, calls
}

// The matcher runs inside the batch, so the lock is held there. Validating and applying under
// a single acquisition is by construction (the public method takes the lock once at the top).
func TestBatchWriteItemHoldsLockWhileMatcherRuns(t *testing.T) {
	c := NewClient()

	lockTestTable(t, c, "t")

	match, released, calls := probeLockReleased(c)
	c.setUnprocessedMatcher("t", match)

	_, err := c.BatchWriteItem(context.Background(), &BatchWriteItemInput{RequestItems: map[string][]WriteRequest{
		"t": {
			{PutRequest: &PutRequest{Item: lockTestItem("1")}},
			{PutRequest: &PutRequest{Item: lockTestItem("2")}},
		},
	}})
	require.NoError(t, err)

	require.Positive(t, calls.Load(), "the matcher never ran")
	require.Zero(t, released.Load(), "the lock was free while the batch was running")
}

func TestBatchGetItemHoldsLockAcrossTables(t *testing.T) {
	c := NewClient()

	lockTestTable(t, c, "a")
	lockTestTable(t, c, "b")

	match, released, calls := probeLockReleased(c)
	c.setUnprocessedMatcher("a", match)
	c.setUnprocessedMatcher("b", match)

	_, err := c.BatchGetItem(context.Background(), &BatchGetItemInput{RequestItems: map[string]KeysAndAttributes{
		"a": {Keys: []map[string]*AttributeValue{lockTestKey("1")}},
		"b": {Keys: []map[string]*AttributeValue{lockTestKey("1")}},
	}})
	require.NoError(t, err)

	require.EqualValues(t, 2, calls.Load(), "the matcher should run once per table")
	require.Zero(t, released.Load(), "the lock was free while the batch was running")
}

// A batch or transaction that holds the lock must not call a public method that locks again.
// A self-deadlock is deterministic, so go test's own -timeout reports it with a goroutine dump.
func TestBatchesAndTransactionsDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	lockTestTable(t, c, "a")
	lockTestTable(t, c, "b")

	for _, mode := range []ddbtypes.ReturnConsumedCapacity{"", ddbtypes.ReturnConsumedCapacityTotal, ddbtypes.ReturnConsumedCapacityIndexes} {
		_, err := c.BatchWriteItem(ctx, &BatchWriteItemInput{
			ReturnConsumedCapacity: mode,
			RequestItems: map[string][]WriteRequest{
				"a": {{PutRequest: &PutRequest{Item: lockTestItem("1")}}, {DeleteRequest: &DeleteRequest{Key: lockTestKey("9")}}},
				"b": {{PutRequest: &PutRequest{Item: lockTestItem("1")}}},
			},
		})
		require.NoError(t, err)

		_, err = c.BatchGetItem(ctx, &BatchGetItemInput{
			ReturnConsumedCapacity: mode,
			RequestItems: map[string]KeysAndAttributes{
				"a": {Keys: []map[string]*AttributeValue{lockTestKey("1")}},
				"b": {Keys: []map[string]*AttributeValue{lockTestKey("1")}},
			},
		})
		require.NoError(t, err)

		_, err = c.TransactGetItems(ctx, &TransactGetItemsInput{
			ReturnConsumedCapacity: mode,
			TransactItems: []TransactGetItem{
				{Get: &Get{TableName: aws.String("a"), Key: lockTestKey("1")}},
				{Get: &Get{TableName: aws.String("b"), Key: lockTestKey("1")}},
			},
		})
		require.NoError(t, err)

		_, err = c.TransactWriteItems(ctx, &TransactWriteItemsInput{
			ReturnConsumedCapacity: mode,
			TransactItems: []TransactWriteItem{
				{Put: &Put{TableName: aws.String("a"), Item: lockTestItem("2")}},
				{Delete: &Delete{TableName: aws.String("b"), Key: lockTestKey("1")}},
			},
		})
		require.NoError(t, err)
	}
}
