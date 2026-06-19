package client

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// FailureCondition describe the failure condtion to emulate
type FailureCondition string

const (
	// FailureConditionNone emulates the system is working
	FailureConditionNone FailureCondition = "none"
	// FailureConditionInternalServerError emulates dynamodb having internal issues
	FailureConditionInternalServerError FailureCondition = "internal_server"
	// FailureConditionDeprecated returns the old error
	FailureConditionDeprecated FailureCondition = "deprecated"
)

var (
	// emulatedInternalServeError represents the error for dynamodb internal server error
	emulatedInternalServeError = types.InternalServerError{Message: aws.String("emulated error")}
	// ErrForcedFailure when the error is forced
	//
	// Deprecated: use EmulateFailure instead
	ErrForcedFailure = errors.New("forced failure response")

	emulatingErrors = map[FailureCondition]error{
		FailureConditionNone:                nil,
		FailureConditionInternalServerError: &emulatedInternalServeError,
		FailureConditionDeprecated:          ErrForcedFailure,
	}
)

// EmulateFailure forces the fake client to fail. Every API hard-fails the call with
// the emulated error, including BatchWriteItem and BatchGetItem (the whole batch
// errors; it does not return partial results). Use FailureConditionNone to clear. To
// leave individual batch sub-requests unprocessed instead of failing the whole call,
// use EmulateUnprocessedItems.
func EmulateFailure(client FakeClient, condition FailureCondition) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("EmulateFailure: invalid client type")
	}

	fakeClient.setFailureCondition(condition)
}

// EmulateFailureForTable scopes failure emulation to a single table, or to a
// specific index of that table when indexName is provided. Operations targeting
// other tables (or, for an index-scoped failure, other access paths on the same
// table) keep working. A batch (BatchWriteItem/BatchGetItem) that touches the scoped
// table hard-fails the whole call. Passing FailureConditionNone clears the failure for
// that exact table/index scope. The global EmulateFailure still overrides everything.
func EmulateFailureForTable(client FakeClient, tableName string, condition FailureCondition, indexName ...string) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("EmulateFailureForTable: invalid client type")
	}

	index := ""
	if len(indexName) > 0 {
		index = indexName[0]
	}

	fakeClient.setTableFailureCondition(tableName, index, condition)
}

// EmulateUnprocessedItems makes BatchWriteItem and BatchGetItem leave selected
// sub-requests of tableName unprocessed (returned in UnprocessedItems/UnprocessedKeys)
// instead of executing them, while the rest of the batch is applied normally. The
// match predicate receives the zero-based index of the sub-request within that table's
// request slice and its raw payload: a PutRequest's full item, or a DeleteRequest/get
// key map. It is sticky until cleared with EmulateUnprocessedItems(client, tableName,
// nil) or ClearUnprocessedItems. Single-item operations are unaffected. A global or
// table-scoped EmulateFailure overrides this and hard-fails the whole batch.
func EmulateUnprocessedItems(client FakeClient, tableName string, match func(n int, raw map[string]types.AttributeValue) bool) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("EmulateUnprocessedItems: invalid client type")
	}

	fakeClient.setUnprocessedMatcher(tableName, match)
}

// ClearUnprocessedItems removes every batch partial-failure predicate set with
// EmulateUnprocessedItems.
func ClearUnprocessedItems(client FakeClient) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("ClearUnprocessedItems: invalid client type")
	}

	fakeClient.clearUnprocessedMatchers()
}

// ActiveForceFailure active force operation to fail
func ActiveForceFailure(client FakeClient) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("ActiveForceFailure: invalid client type")
	}

	fakeClient.setFailureCondition(FailureConditionDeprecated)
}

// DeactiveForceFailure deactive force operation to fail
func DeactiveForceFailure(client FakeClient) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("DeactiveForceFailure: invalid client type")
	}

	fakeClient.setFailureCondition(FailureConditionNone)
}

// AddTable add a new table
func AddTable(ctx context.Context, client FakeClient, tableName, partitionKey, rangeKey string) error {
	input := generateAddTableInput(tableName, partitionKey, rangeKey)

	_, err := client.CreateTable(ctx, input)

	return err
}

// AddIndex add a new index to the table table
func AddIndex(ctx context.Context, client FakeClient, tableName, indexName, partitionKey, rangeKey string) error {
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String(partitionKey),
			KeyType:       types.KeyTypeHash,
		},
	}

	attributes := []types.AttributeDefinition{
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: types.ScalarAttributeTypeS,
		},
	}

	if rangeKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(rangeKey),
			KeyType:       types.KeyTypeRange,
		})

		attributes = append(attributes, types.AttributeDefinition{
			AttributeName: aws.String(rangeKey),
			AttributeType: types.ScalarAttributeTypeS,
		})
	}

	input := &dynamodb.UpdateTableInput{
		AttributeDefinitions: attributes,
		TableName:            aws.String(tableName),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(indexName),
					KeySchema: keySchema,
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
			},
		},
	}

	_, err := client.UpdateTable(ctx, input)

	return err
}

// SetIndexActivationDelay configures how long newly created GSIs report CREATING before ACTIVE.
func SetIndexActivationDelay(client FakeClient, delay time.Duration) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("SetIndexActivationDelay: invalid client type")
	}

	fakeClient.setIndexActivationDelay(delay)
}

// ClearTable removes all data from a specific table
func ClearTable(client FakeClient, tableName string) error {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("ClearTable: invalid client type")
	}

	table, err := fakeClient.getTable(tableName)
	if err != nil {
		return err
	}

	fakeClient.mu.Lock()
	defer fakeClient.mu.Unlock()

	table.Clear()

	for _, index := range table.Indexes {
		index.Clear()
	}

	return nil
}

func generateAddTableInput(tableName, hashKey, rangeKey string) *dynamodb.CreateTableInput {
	var cunit int64 = 10

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName: aws.String(tableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &cunit,
			WriteCapacityUnits: &cunit,
		},
	}

	if rangeKey != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			types.AttributeDefinition{
				AttributeName: aws.String(rangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		)

		input.KeySchema = append(input.KeySchema,
			types.KeySchemaElement{
				AttributeName: aws.String(rangeKey),
				KeyType:       types.KeyTypeRange,
			},
		)
	}

	return input
}

func copyItem(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	copy := map[string]types.AttributeValue{}
	maps.Copy(copy, item)

	return copy
}
