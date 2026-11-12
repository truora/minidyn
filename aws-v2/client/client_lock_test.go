package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// D7: a public operation takes the client lock once, so none of these may race or deadlock.

type lockItem = map[string]dynamodbtypes.AttributeValue

func lockTestItem(id string) lockItem {
	return lockItem{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: id},
		"type": &dynamodbtypes.AttributeValueMemberS{Value: "t"},
	}
}

func lockTestKey(id string) lockItem {
	return lockItem{"id": &dynamodbtypes.AttributeValueMemberS{Value: id}}
}

func TestClientTableOperationsRaceItemOperations(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	require.NoError(t, AddTable(ctx, c, "main", "id", ""))
	require.NoError(t, AddTable(ctx, c, "other", "id", ""))

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

		_, _ = c.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName: name,
			AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
				{AttributeName: aws.String("type"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			},
			GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
				IndexName:  aws.String(idx),
				KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeHash}},
				Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
			}}},
		})
		_, _ = c.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName: name,
			GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{
				Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(idx)},
			}},
		})
	})
	run(func(i int) {
		name := fmt.Sprintf("tmp-%d", i)

		_ = AddTable(ctx, c, name, "id", "")
		_, _ = c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		_, _ = c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
	})
	run(func(int) { _, _ = c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("main")}) })
	run(func(i int) {
		id := fmt.Sprintf("p-%d", i)

		_, _ = c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("main"), Item: lockTestItem(id)})
		_, _ = c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("main"), Key: lockTestKey(id)})
		_, _ = c.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("main")})
	})
	run(func(i int) {
		id := fmt.Sprintf("b-%d", i)

		_, _ = c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
			"main":  {{PutRequest: &dynamodbtypes.PutRequest{Item: lockTestItem(id)}}},
			"other": {{PutRequest: &dynamodbtypes.PutRequest{Item: lockTestItem(id)}}},
		}})
		_, _ = c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
			"main":  {Keys: []lockItem{lockTestKey(id)}},
			"other": {Keys: []lockItem{lockTestKey(id)}},
		}})
		_, _ = c.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{
			{Get: &dynamodbtypes.Get{TableName: aws.String("main"), Key: lockTestKey(id)}},
			{Get: &dynamodbtypes.Get{TableName: aws.String("other"), Key: lockTestKey(id)}},
		}})
		_, _ = c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
			{Put: &dynamodbtypes.Put{TableName: aws.String("main"), Item: lockTestItem("t-" + id)}},
		}})
	})
	run(func(int) {
		ActiveForceFailure(c)
		DeactiveForceFailure(c)
		SetItemCollectionMetrics(c, nil)
		SetIndexActivationDelay(c, 0)
		_ = c.GetNativeInterpreter()
		c.SetInterpreter(c.GetNativeInterpreter())
		_ = ClearTable(c, "other")
	})

	run(func(i int) {
		id := fmt.Sprintf("p-%d", i)

		_, _ = c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String("main"),
			Key:                       lockTestKey(id),
			UpdateExpression:          aws.String("SET #t = :v"),
			ExpressionAttributeNames:  map[string]string{"#t": "type"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "u"}},
		})
		_, _ = c.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("main"), Key: lockTestKey(id)})
		_, _ = c.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String("main"),
			KeyConditionExpression:    aws.String("id = :id"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":id": &dynamodbtypes.AttributeValueMemberS{Value: id}},
		})
		_, _ = c.ListTables(ctx, &dynamodb.ListTablesInput{})
	})
	run(func(i int) {
		c.ActivateDebug()
		c.ActivateNativeInterpreter()
		EmulateFailureForTable(c, "ghost", FailureConditionInternalServerError)
		EmulateFailureForTable(c, "ghost", FailureConditionNone)

		// The predicate must not call the client: it runs with the client lock held.
		EmulateUnprocessedItems(c, "other", func(n int, _ lockItem) bool { return (n+i)%2 == 0 })
		ClearUnprocessedItems(c)
	})

	wg.Wait()
}

// The probe runs inside the unprocessed-item matcher, which the batch calls with the client
// lock held. TryLock succeeds only if the batch released the lock in the middle of the call.
func probeLockReleased(c *Client) (match func(int, lockItem) bool, released, calls *atomic.Int32) {
	released, calls = new(atomic.Int32), new(atomic.Int32)

	match = func(int, lockItem) bool {
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
	ctx := context.Background()
	c := NewClient()

	require.NoError(t, AddTable(ctx, c, "t", "id", ""))

	match, released, calls := probeLockReleased(c)
	EmulateUnprocessedItems(c, "t", match)

	_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
		"t": {
			{PutRequest: &dynamodbtypes.PutRequest{Item: lockTestItem("1")}},
			{PutRequest: &dynamodbtypes.PutRequest{Item: lockTestItem("2")}},
		},
	}})
	require.NoError(t, err)

	require.Positive(t, calls.Load(), "the matcher never ran")
	require.Zero(t, released.Load(), "the lock was free while the batch was running")
}

func TestBatchGetItemHoldsLockAcrossTables(t *testing.T) {
	ctx := context.Background()
	c := NewClient()

	require.NoError(t, AddTable(ctx, c, "a", "id", ""))
	require.NoError(t, AddTable(ctx, c, "b", "id", ""))

	match, released, calls := probeLockReleased(c)
	EmulateUnprocessedItems(c, "a", match)
	EmulateUnprocessedItems(c, "b", match)

	_, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
		"a": {Keys: []lockItem{lockTestKey("1")}},
		"b": {Keys: []lockItem{lockTestKey("1")}},
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

	require.NoError(t, AddTable(ctx, c, "a", "id", ""))
	require.NoError(t, AddTable(ctx, c, "b", "id", ""))

	for _, mode := range []dynamodbtypes.ReturnConsumedCapacity{"", dynamodbtypes.ReturnConsumedCapacityTotal, dynamodbtypes.ReturnConsumedCapacityIndexes} {
		_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			ReturnConsumedCapacity: mode,
			RequestItems: map[string][]dynamodbtypes.WriteRequest{
				"a": {
					{PutRequest: &dynamodbtypes.PutRequest{Item: lockTestItem("1")}},
					{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: lockTestKey("9")}},
				},
				"b": {{PutRequest: &dynamodbtypes.PutRequest{Item: lockTestItem("1")}}},
			},
		})
		require.NoError(t, err)

		_, err = c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			ReturnConsumedCapacity: mode,
			RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				"a": {Keys: []lockItem{lockTestKey("1")}},
				"b": {Keys: []lockItem{lockTestKey("1")}},
			},
		})
		require.NoError(t, err)

		_, err = c.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			ReturnConsumedCapacity: mode,
			TransactItems: []dynamodbtypes.TransactGetItem{
				{Get: &dynamodbtypes.Get{TableName: aws.String("a"), Key: lockTestKey("1")}},
				{Get: &dynamodbtypes.Get{TableName: aws.String("b"), Key: lockTestKey("1")}},
			},
		})
		require.NoError(t, err)

		_, err = c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			ReturnConsumedCapacity: mode,
			TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{TableName: aws.String("a"), Item: lockTestItem("2")}},
				{Delete: &dynamodbtypes.Delete{TableName: aws.String("b"), Key: lockTestKey("1")}},
			},
		})
		require.NoError(t, err)
	}
}
