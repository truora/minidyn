package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/interpreter"
	mtypes "github.com/truora/minidyn/types"
)

const (
	batchRequestsLimit                              = 25
	batchGetItemRequestsLimit                       = 100
	unusedExpressionAttributeNamesMsg               = "Value provided in ExpressionAttributeNames unused in expressions"
	unusedExpressionAttributeValuesMsg              = "Value provided in ExpressionAttributeValues unused in expressions"
	expressionAttributeValuesOnlyWithExpressionsMsg = "ExpressionAttributeValues can only be specified when using expressions"
	invalidExpressionAttributeName                  = "ExpressionAttributeNames contains invalid key"
	invalidExpressionAttributeValue                 = "ExpressionAttributeValues contains invalid key"
)

var (
	// ErrInvalidTableName when the provided table name is invalid
	ErrInvalidTableName = errors.New("invalid table name")
	// ErrResourceNotFoundException when the requested resource is not found
	ErrResourceNotFoundException   = errors.New("requested resource not found")
	expressionAttributeNamesRegex  = regexp.MustCompile("^#[A-Za-z0-9_]+$")
	expressionAttributeValuesRegex = regexp.MustCompile("^:[A-Za-z0-9_]+$")
)

// FakeClient mocks the Dynamodb client
type FakeClient interface {
	CreateTable(ctx context.Context, input *dynamodb.CreateTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	UpdateTable(ctx context.Context, input *dynamodb.UpdateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	DescribeTable(ctx context.Context, input *dynamodb.DescribeTableInput, ops ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	GetItem(ctx context.Context, input *dynamodb.GetItemInput, opt ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, input *dynamodb.QueryInput, opt ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, input *dynamodb.ScanInput, opt ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	BatchGetItem(ctx context.Context, input *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	TransactGetItems(ctx context.Context, input *dynamodb.TransactGetItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

// Client define a mock struct to be used
type Client struct {
	tables                map[string]*core.Table
	mu                    sync.Mutex
	itemCollectionMetrics map[string][]types.ItemCollectionMetrics
	langInterpreter       *interpreter.Language
	nativeInterpreter     *interpreter.Native
	useNativeInterpreter  bool
	forceFailureErr       error
	tableFailureErrs      map[string]error
	unprocessedMatchers   map[string]func(int, map[string]types.AttributeValue) bool
	indexActivationDelay  time.Duration
}

// NewClient initializes dynamodb client with a mock
func NewClient() *Client {
	fake := Client{
		tables:              map[string]*core.Table{},
		mu:                  sync.Mutex{},
		nativeInterpreter:   interpreter.NewNativeInterpreter(),
		langInterpreter:     &interpreter.Language{},
		tableFailureErrs:    map[string]error{},
		unprocessedMatchers: map[string]func(int, map[string]types.AttributeValue) bool{},
	}

	return &fake
}

// ActivateDebug it activates the debug mode
func (fd *Client) ActivateDebug() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.langInterpreter.Debug = true
}

// ActivateNativeInterpreter it activates the debug mode
func (fd *Client) ActivateNativeInterpreter() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.useNativeInterpreter = true

	for _, table := range fd.tables {
		table.UseNativeInterpreter = true
	}
}

func (fd *Client) setFailureCondition(condition FailureCondition) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.forceFailureErr = emulatingErrors[condition]
}

// failureKey builds the lookup key for a table-scoped failure. An empty index
// means the failure applies to the whole table.
func failureKey(table, index string) string {
	return table + "\x00" + index
}

// setTableFailureCondition scopes a failure to a table, or to a specific index of
// that table when index is not empty. FailureConditionNone clears that exact scope.
func (fd *Client) setTableFailureCondition(table, index string, condition FailureCondition) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	key := failureKey(table, index)

	err := emulatingErrors[condition]
	if err == nil {
		delete(fd.tableFailureErrs, key)

		return
	}

	fd.tableFailureErrs[key] = err
}

// failureErrFor resolves the emulated error for an operation targeting the given
// table and index. A global failure overrides everything; otherwise an
// index-scoped failure (when index is set) wins over a table-wide one. Callers
// must hold fd.mu.
func (fd *Client) failureErrFor(table, index string) error {
	if fd.forceFailureErr != nil {
		return fd.forceFailureErr
	}

	if index != "" {
		if err := fd.tableFailureErrs[failureKey(table, index)]; err != nil {
			return err
		}
	}

	return fd.tableFailureErrs[failureKey(table, "")]
}

// setUnprocessedMatcher installs a predicate that marks matching sub-requests of a
// batch operation on tableName as unprocessed. A nil match clears the predicate for
// that table.
func (fd *Client) setUnprocessedMatcher(tableName string, match func(int, map[string]types.AttributeValue) bool) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if match == nil {
		delete(fd.unprocessedMatchers, tableName)

		return
	}

	fd.unprocessedMatchers[tableName] = match
}

// clearUnprocessedMatchers removes every batch partial-failure predicate.
func (fd *Client) clearUnprocessedMatchers() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.unprocessedMatchers = map[string]func(int, map[string]types.AttributeValue) bool{}
}

// batchEmulation is a lock-free snapshot of the failure emulation that applies to a
// batch operation: a hard failure error (global or table-scoped) that fails the whole
// call, and the per-table partial-failure predicates.
type batchEmulation struct {
	failErr  error
	matchers map[string]func(int, map[string]types.AttributeValue) bool
}

// unprocessed reports whether sub-request n (raw payload) of tableName should be
// returned as unprocessed instead of being executed.
func (e batchEmulation) unprocessed(tableName string, n int, raw map[string]types.AttributeValue) bool {
	match, ok := e.matchers[tableName]

	return ok && match(n, raw)
}

// batchEmulationFor snapshots the emulation state relevant to a batch touching the
// given tables. A global failure overrides everything; otherwise any table-scoped
// failure hard-fails the whole batch. Takes fd.mu briefly so the loop can run lock-free.
func (fd *Client) batchEmulationFor(tables ...string) batchEmulation {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	snap := batchEmulation{}

	if fd.forceFailureErr != nil {
		snap.failErr = fd.forceFailureErr

		return snap
	}

	for _, table := range tables {
		if err := fd.failureErrFor(table, ""); err != nil {
			snap.failErr = err

			return snap
		}
	}

	for _, table := range tables {
		if match, ok := fd.unprocessedMatchers[table]; ok {
			if snap.matchers == nil {
				snap.matchers = map[string]func(int, map[string]types.AttributeValue) bool{}
			}

			snap.matchers[table] = match
		}
	}

	return snap
}

func (fd *Client) setIndexActivationDelay(delay time.Duration) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	fd.indexActivationDelay = delay

	for _, table := range fd.tables {
		table.IndexActivationDelay = delay
	}
}

// SetInterpreter assigns a native interpreter
func (fd *Client) SetInterpreter(i interpreter.Interpreter) {
	native, ok := i.(*interpreter.Native)
	if !ok {
		panic("invalid interpreter type")
	}

	fd.nativeInterpreter = native

	for _, table := range fd.tables {
		table.NativeInterpreter = *native
	}
}

// GetNativeInterpreter returns native interpreter
func (fd *Client) GetNativeInterpreter() *interpreter.Native {
	return fd.nativeInterpreter
}

// CreateTable creates a new table
func (fd *Client) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if _, ok := fd.tables[tableName]; ok {
		return nil, &types.ResourceInUseException{Message: aws.String("Cannot create preexisting table")}
	}

	newTable := core.NewTable(tableName)
	newTable.SetAttributeDefinition(mapDynamoToTypesAttributeDefinitionSlice(input.AttributeDefinitions))
	newTable.BillingMode = aws.String(string(input.BillingMode))
	newTable.NativeInterpreter = *fd.nativeInterpreter
	newTable.UseNativeInterpreter = fd.useNativeInterpreter
	newTable.LangInterpreter = *fd.langInterpreter
	newTable.IndexActivationDelay = fd.indexActivationDelay

	if err := newTable.CreatePrimaryIndex(mapDynamoToTypesCreateTableInput(input)); err != nil {
		return nil, mapKnownError(err)
	}

	if err := newTable.AddGlobalIndexes(mapDynamoToTypesGlobalSecondaryIndexes(input.GlobalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	if err := newTable.AddLocalIndexes(mapDynamoToTypesLocalSecondaryIndexes(input.LocalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	fd.tables[tableName] = newTable

	return &dynamodb.CreateTableOutput{
		TableDescription: mapTypesToDynamoTableDescription(newTable.Description(tableName)),
	}, nil
}

// DeleteTable deletes a table
func (fd *Client) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, mapKnownError(err)
	}

	desc := mapTypesToDynamoTableDescription(table.Description(tableName))

	delete(fd.tables, tableName)

	return &dynamodb.DeleteTableOutput{
		TableDescription: desc,
	}, nil
}

// UpdateTable update a table
func (fd *Client) UpdateTable(ctx context.Context, input *dynamodb.UpdateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, ok := fd.tables[tableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String("Cannot do operations on a non-existent table")}
	}

	if input.AttributeDefinitions != nil {
		table.SetAttributeDefinition(mapDynamoToTypesAttributeDefinitionSlice(input.AttributeDefinitions))
	}

	for _, change := range input.GlobalSecondaryIndexUpdates {
		if err := table.ApplyIndexChange(mapDynamoTotypesGlobalSecondaryIndexUpdate(change)); err != nil {
			return &dynamodb.UpdateTableOutput{
				TableDescription: mapTypesToDynamoTableDescription(table.Description(tableName)),
			}, mapKnownError(err)
		}
	}

	return &dynamodb.UpdateTableOutput{
		TableDescription: mapTypesToDynamoTableDescription(table.Description(tableName)),
	}, nil
}

// DescribeTable returns information about the table
func (fd *Client) DescribeTable(ctx context.Context, input *dynamodb.DescribeTableInput, ops ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, mapKnownError(err)
	}

	output := &dynamodb.DescribeTableOutput{
		Table: mapTypesToDynamoTableDescription(table.Description(tableName)),
	}

	return output, nil
}

// PutItem mock response for dynamodb
func (fd *Client) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	putInput := mapDynamoToTypesPutItemInput(input)

	newSize := capacity.Size(putInput.Item)

	oldSize := 0
	if key, kerr := table.KeySchema.GetKey(table.AttributesDef, putInput.Item); kerr == nil {
		oldSize = capacity.Size(table.Data[key])
	}

	item, err := table.Put(putInput)
	if err != nil {
		return &dynamodb.PutItemOutput{Attributes: mapTypesToDynamoMapItem(item)}, mapKnownError(err)
	}

	return &dynamodb.PutItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
		ConsumedCapacity: toSDKConsumed(capacity.ForWrite(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), max(oldSize, newSize),
		)),
	}, nil
}

// DeleteItem mock response for dynamodb
func (fd *Client) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		items, _, serr := table.SearchData(core.QueryInput{
			Index:                     core.PrimaryIndexName,
			ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
			Aliases:                   input.ExpressionAttributeNames,
			Limit:                     aws.ToInt64(aws.Int64(1)),
			ConditionExpression:       input.ConditionExpression,
		})
		if serr != nil {
			return nil, mapKnownError(serr)
		}

		if len(items) == 0 {
			return &dynamodb.DeleteItemOutput{}, &types.ConditionalCheckFailedException{Message: aws.String(core.ErrConditionalRequestFailed.Error())}
		}
	}

	deleteInput := mapDynamoToTypesDeleteItemInput(input)

	item, err := table.Delete(deleteInput)
	if err != nil {
		return nil, mapKnownError(err)
	}

	sizeSource := item
	if len(sizeSource) == 0 {
		sizeSource = deleteInput.Key
	}

	consumed := toSDKConsumed(capacity.ForWrite(
		consumedMode(string(input.ReturnConsumedCapacity)),
		aws.ToString(input.TableName), capacity.Size(sizeSource),
	))

	if string(input.ReturnValues) == "ALL_OLD" {
		return &dynamodb.DeleteItemOutput{
			Attributes:       mapTypesToDynamoMapItem(item),
			ConsumedCapacity: consumed,
		}, nil
	}

	return &dynamodb.DeleteItemOutput{ConsumedCapacity: consumed}, nil
}

// UpdateItem mock response for dynamodb
func (fd *Client) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.UpdateExpression), aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	updateInput := mapDynamoToTypesUpdateItemInput(input)

	updateKey, keyErr := table.KeySchema.GetKey(table.AttributesDef, updateInput.Key)

	beforeSize := 0
	if keyErr == nil {
		beforeSize = capacity.Size(table.Data[updateKey])
	}

	item, err := table.Update(updateInput)
	if err != nil {
		if errors.Is(err, interpreter.ErrSyntaxError) {
			return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
		}

		return nil, mapKnownError(err)
	}

	afterSize := beforeSize
	if keyErr == nil {
		afterSize = capacity.Size(table.Data[updateKey])
	}

	output := &dynamodb.UpdateItemOutput{
		ConsumedCapacity: toSDKConsumed(capacity.ForWrite(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), max(beforeSize, afterSize),
		)),
	}

	if item != nil {
		output.Attributes = mapTypesToDynamoMapItem(item)
	}

	return output, nil
}

// GetItem mock response for dynamodb
func (fd *Client) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opt ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, nil, aws.ToString(input.ProjectionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	keyMap := mapDynamoToTypesMapItem(input.Key)
	if vErr := mtypes.ValidateItemMap(keyMap); vErr != nil {
		return nil, mapKnownError(mtypes.NewError("ValidationException", vErr.Error(), nil))
	}

	if keyErr := table.ValidatePrimaryKeyMap(keyMap); keyErr != nil {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: keyErr.Error()}
	}

	key, err := table.KeySchema.GetKey(table.AttributesDef, keyMap)
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
	}

	stored := table.Data[key]

	item, err := getItemAttributesForOutput(table, stored, aws.ToString(input.ProjectionExpression), input.ExpressionAttributeNames)
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
	}

	output := &dynamodb.GetItemOutput{
		Item: copyItem(item),
	}

	sizeSource := stored
	if len(sizeSource) == 0 {
		sizeSource = keyMap
	}

	output.ConsumedCapacity = toSDKConsumed(capacity.ForRead(
		consumedMode(string(input.ReturnConsumedCapacity)),
		aws.ToString(input.TableName), "", "",
		capacity.Size(sizeSource), aws.ToBool(input.ConsistentRead),
	))

	return output, nil
}

// Query mock response for dynamodb
func (fd *Client) Query(ctx context.Context, input *dynamodb.QueryInput, opt ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), aws.ToString(input.IndexName)); ferr != nil {
		return nil, ferr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.KeyConditionExpression), aws.ToString(input.FilterExpression), aws.ToString(input.ProjectionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	indexName := aws.ToString(input.IndexName)

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	items, lastKey, err := table.SearchData(mapDynamoToTypesQueryInput(input, indexName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	count := len(items)
	if count > math.MaxInt32 {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: "Result count exceeds maximum allowed value"}
	}

	output := &dynamodb.QueryOutput{
		Items:            mapTypesToDynamoSliceMapItem(items),
		Count:            int32(count),
		LastEvaluatedKey: mapTypesToDynamoMapItem(lastKey),
	}

	output.ConsumedCapacity = toSDKConsumed(capacity.ForRead(
		consumedMode(string(input.ReturnConsumedCapacity)),
		aws.ToString(input.TableName), indexName, table.IndexKind(indexName),
		capacity.SumSize(items), aws.ToBool(input.ConsistentRead),
	))

	return output, nil
}

// Scan mock scan operation
func (fd *Client) Scan(ctx context.Context, input *dynamodb.ScanInput, opt ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), aws.ToString(input.IndexName)); ferr != nil {
		return nil, ferr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ProjectionExpression), aws.ToString(input.FilterExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	indexName := aws.ToString(input.IndexName)

	items, lastKey, err := table.SearchData(core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ExclusiveStartKey:         mapDynamoToTypesMapItem(input.ExclusiveStartKey),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ProjectionExpression:      aws.ToString(input.ProjectionExpression),
		ScanIndexForward:          true,
		Scan:                      true,
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	count := len(items)
	if count > math.MaxInt32 {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: "Result count exceeds maximum allowed value"}
	}

	output := &dynamodb.ScanOutput{
		Items:            mapTypesToDynamoSliceMapItem(items),
		Count:            int32(count),
		LastEvaluatedKey: mapTypesToDynamoMapItem(lastKey),
	}

	output.ConsumedCapacity = toSDKConsumed(capacity.ForRead(
		consumedMode(string(input.ReturnConsumedCapacity)),
		aws.ToString(input.TableName), indexName, table.IndexKind(indexName),
		capacity.SumSize(items), aws.ToBool(input.ConsistentRead),
	))

	return output, nil
}

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func (fd *Client) setItemCollectionMetrics(itemCollectionMetrics map[string][]types.ItemCollectionMetrics) {
	fd.itemCollectionMetrics = itemCollectionMetrics
}

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func SetItemCollectionMetrics(client FakeClient, itemCollectionMetrics map[string][]types.ItemCollectionMetrics) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("SetItemCollectionMetrics: invalid client type")
	}

	fakeClient.setItemCollectionMetrics(itemCollectionMetrics)
}

// BatchWriteItem mock response for dynamodb. An emulated failure (global EmulateFailure
// or table-scoped EmulateFailureForTable touching any table in the batch) hard-fails the
// whole call. UnprocessedItems is produced only by EmulateUnprocessedItems, whose
// predicate selects individual sub-requests to leave unprocessed while the rest are
// applied.
func (fd *Client) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if err := validateBatchWriteItemInput(input); err != nil {
		return &dynamodb.BatchWriteItemOutput{}, err
	}

	emulation := fd.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	unprocessed := map[string][]types.WriteRequest{}
	unitsByTable := map[string]float64{}

	for table, reqs := range input.RequestItems {
		for i, req := range reqs {
			if emulation.unprocessed(table, i, batchWriteRequestKey(req)) {
				unprocessed[table] = append(unprocessed[table], req)

				continue
			}

			if err := executeBatchWriteRequest(ctx, fd, aws.String(table), req); err != nil {
				return &dynamodb.BatchWriteItemOutput{}, err
			}

			unitsByTable[table] += capacity.WriteUnits(capacity.Size(batchWriteRequestItem(req)))
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems:      unprocessed,
		ItemCollectionMetrics: fd.itemCollectionMetrics,
		ConsumedCapacity:      sdkConsumedSlice(consumedMode(string(input.ReturnConsumedCapacity)), unitsByTable, false),
	}, nil
}

// batchWriteRequestItem returns the item used to size a write request: the put item, or
// the delete key when sizing a delete.
func batchWriteRequestItem(req types.WriteRequest) map[string]*mtypes.Item {
	if req.PutRequest != nil {
		return mapDynamoToTypesMapItem(req.PutRequest.Item)
	}

	if req.DeleteRequest != nil {
		return mapDynamoToTypesMapItem(req.DeleteRequest.Key)
	}

	return nil
}

func tableNames[V any](requestItems map[string]V) []string {
	names := make([]string, 0, len(requestItems))
	for name := range requestItems {
		names = append(names, name)
	}

	return names
}

func batchWriteRequestKey(req types.WriteRequest) map[string]types.AttributeValue {
	if req.PutRequest != nil {
		return req.PutRequest.Item
	}

	if req.DeleteRequest != nil {
		return req.DeleteRequest.Key
	}

	return nil
}

// BatchGetItem mock response for dynamodb. An emulated failure (global EmulateFailure
// or table-scoped EmulateFailureForTable touching any table in the batch) hard-fails the
// whole call. UnprocessedKeys is produced only by EmulateUnprocessedItems, whose
// predicate selects individual keys to leave unprocessed while the rest are fetched.
func (fd *Client) BatchGetItem(ctx context.Context, input *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	emulation := fd.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	responses := make(map[string][]map[string]types.AttributeValue, len(input.RequestItems))
	unprocessed := make(map[string]types.KeysAndAttributes, len(input.RequestItems))
	unitsByTable := map[string]float64{}

	for tableName, reqs := range input.RequestItems {
		unprocessedKeys := make([]map[string]types.AttributeValue, 0, len(reqs.Keys))
		responses[tableName] = make([]map[string]types.AttributeValue, 0, len(reqs.Keys))

		consistent := aws.ToBool(reqs.ConsistentRead)

		for i, req := range reqs.Keys {
			if emulation.unprocessed(tableName, i, req) {
				unprocessedKeys = append(unprocessedKeys, req)

				continue
			}

			getInput := &dynamodb.GetItemInput{
				TableName:                aws.String(tableName),
				Key:                      req,
				ConsistentRead:           reqs.ConsistentRead,
				AttributesToGet:          reqs.AttributesToGet,
				ExpressionAttributeNames: reqs.ExpressionAttributeNames,
				ProjectionExpression:     reqs.ProjectionExpression,
			}

			out, err := fd.GetItem(ctx, getInput)
			if err != nil {
				return nil, err
			}

			unitsByTable[tableName] += capacity.ReadUnits(capacity.Size(mapDynamoToTypesMapItem(out.Item)), consistent)

			if len(out.Item) > 0 {
				responses[tableName] = append(responses[tableName], out.Item)
			}
		}

		if len(unprocessedKeys) > 0 {
			unprocessed[tableName] = reqs

			tableUnprocessedKeys := unprocessed[tableName]
			tableUnprocessedKeys.Keys = unprocessedKeys

			unprocessed[tableName] = tableUnprocessedKeys
		}
	}

	return &dynamodb.BatchGetItemOutput{
		Responses:        responses,
		UnprocessedKeys:  unprocessed,
		ConsumedCapacity: sdkConsumedSlice(consumedMode(string(input.ReturnConsumedCapacity)), unitsByTable, true),
	}, nil
}

func validateWriteRequest(req types.WriteRequest) error {
	if req.DeleteRequest != nil && req.PutRequest != nil {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}
	}

	if req.DeleteRequest == nil && req.PutRequest == nil {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"}
	}

	return nil
}

func validateBatchWriteItemInput(input *dynamodb.BatchWriteItemInput) error {
	count := 0

	for _, reqs := range input.RequestItems {
		for _, req := range reqs {
			err := validateWriteRequest(req)
			if err != nil {
				return err
			}

			count++
		}
	}

	if count > batchRequestsLimit {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: "Too many items requested for the BatchWriteItem call"}
	}

	return nil
}

func executeBatchWriteRequest(ctx context.Context, fd *Client, table *string, req types.WriteRequest) error {
	if req.PutRequest != nil {
		_, err := fd.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      req.PutRequest.Item,
			TableName: table,
		})

		return err
	}

	if req.DeleteRequest != nil {
		_, err := fd.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			Key:       req.DeleteRequest.Key,
			TableName: table,
		})

		return err
	}

	return nil
}

func (fd *Client) prepareTransact(items []types.TransactWriteItem) (map[string]core.TableSnapshot, error) {
	snapshots := map[string]core.TableSnapshot{}
	seenKeys := make(map[string]struct{}, len(items))

	for _, item := range items {
		var tableName string
		var rawKeyMap map[string]types.AttributeValue

		switch {
		case item.Put != nil:
			tableName, rawKeyMap = aws.ToString(item.Put.TableName), item.Put.Item
		case item.Update != nil:
			tableName, rawKeyMap = aws.ToString(item.Update.TableName), item.Update.Key
		case item.Delete != nil:
			tableName, rawKeyMap = aws.ToString(item.Delete.TableName), item.Delete.Key
		case item.ConditionCheck != nil:
			tableName, rawKeyMap = aws.ToString(item.ConditionCheck.TableName), item.ConditionCheck.Key
		}

		if tableName == "" {
			continue
		}

		if ferr := fd.failureErrFor(tableName, ""); ferr != nil {
			return nil, ferr
		}

		if _, alreadySnapped := snapshots[tableName]; !alreadySnapped {
			table, err := fd.getTable(tableName)
			if err != nil {
				return nil, mapKnownError(err)
			}

			snapshots[tableName] = table.Snapshot()
		}

		table := fd.tables[tableName]
		internalKeyMap := mapDynamoToTypesMapItem(rawKeyMap)

		if key, err := table.KeySchema.GetKey(table.AttributesDef, internalKeyMap); err == nil {
			id := tableName + "|" + key

			if _, exists := seenKeys[id]; exists {
				return nil, &smithy.GenericAPIError{
					Code:    "ValidationException",
					Message: "Transaction request cannot include multiple operations on one item",
				}
			}

			seenKeys[id] = struct{}{}
		}
	}

	return snapshots, nil
}

// TransactWriteItems mock response for dynamodb
func (fd *Client) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	snapshots, err := fd.prepareTransact(input.TransactItems)
	if err != nil {
		return nil, err
	}

	var execErr error

	defer func() {
		if execErr != nil {
			for name, snap := range snapshots {
				fd.tables[name].Restore(snap)
			}
		}
	}()

	n := len(input.TransactItems)

	for i, item := range input.TransactItems {
		execErr = fd.runTransactItem(i, n, item)
		if execErr != nil {
			return nil, execErr
		}
	}

	mode := consumedMode(string(input.ReturnConsumedCapacity))
	unitsByTable := map[string]float64{}

	for _, item := range input.TransactItems {
		tableName, size := fd.transactWriteSize(item)
		if tableName == "" {
			continue
		}

		unitsByTable[tableName] += 2 * capacity.WriteUnits(size)
	}

	return &dynamodb.TransactWriteItemsOutput{
		ConsumedCapacity: sdkConsumedSlice(mode, unitsByTable, false),
	}, nil
}

// transactWriteSize returns the table name and byte size used to bill a transact write item.
// Update sizes the stored item after the update; Delete/ConditionCheck size the key.
func (fd *Client) transactWriteSize(item types.TransactWriteItem) (string, int) {
	switch {
	case item.Put != nil:
		return aws.ToString(item.Put.TableName), capacity.Size(mapDynamoToTypesMapItem(item.Put.Item))
	case item.Update != nil:
		return aws.ToString(item.Update.TableName), fd.itemSizeByKey(aws.ToString(item.Update.TableName), item.Update.Key)
	case item.Delete != nil:
		return aws.ToString(item.Delete.TableName), capacity.Size(mapDynamoToTypesMapItem(item.Delete.Key))
	case item.ConditionCheck != nil:
		return aws.ToString(item.ConditionCheck.TableName), capacity.Size(mapDynamoToTypesMapItem(item.ConditionCheck.Key))
	default:
		return "", 0
	}
}

// itemSizeByKey returns the byte size of the stored item for a key, falling back to the
// key's own size when the table or item is not found.
func (fd *Client) itemSizeByKey(tableName string, key map[string]types.AttributeValue) int {
	keyMap := mapDynamoToTypesMapItem(key)

	table, err := fd.getTable(tableName)
	if err != nil {
		return capacity.Size(keyMap)
	}

	k, err := table.KeySchema.GetKey(table.AttributesDef, keyMap)
	if err != nil {
		return capacity.Size(keyMap)
	}

	if stored := table.Data[k]; len(stored) > 0 {
		return capacity.Size(stored)
	}

	return capacity.Size(keyMap)
}

func validateTransactGetItemsInput(fd *Client, input *dynamodb.TransactGetItemsInput) error {
	if input == nil {
		return nil
	}

	count := len(input.TransactItems)
	if count == 0 {
		return &smithy.GenericAPIError{
			Code:    "ValidationException",
			Message: "1 validation error detected: Value '{}' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
		}
	}

	if count > batchGetItemRequestsLimit {
		return &smithy.GenericAPIError{
			Code:    "ValidationException",
			Message: "Too many items requested for the TransactGetItems call",
		}
	}

	seenKeys := make(map[string]struct{}, count)

	for _, item := range input.TransactItems {
		if item.Get == nil {
			return &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "transaction item must include Get",
			}
		}

		get := item.Get

		tableName := aws.ToString(get.TableName)
		if tableName == "" {
			return &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "TableName must not be null",
			}
		}

		table, err := fd.getTable(tableName)
		if err != nil {
			return mapKnownError(err)
		}

		internalKeyMap := mapDynamoToTypesMapItem(get.Key)
		if vErr := mtypes.ValidateItemMap(internalKeyMap); vErr != nil {
			return mapKnownError(mtypes.NewError("ValidationException", vErr.Error(), nil))
		}

		if keyErr := table.ValidatePrimaryKeyMap(internalKeyMap); keyErr != nil {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: keyErr.Error()}
		}

		key, err := table.KeySchema.GetKey(table.AttributesDef, internalKeyMap)
		if err != nil {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
		}

		id := tableName + "|" + key

		if _, exists := seenKeys[id]; exists {
			return &smithy.GenericAPIError{
				Code:    "ValidationException",
				Message: "Transaction request cannot include multiple operations on one item",
			}
		}

		seenKeys[id] = struct{}{}
	}

	return nil
}

// TransactGetItems mock response for dynamodb.
func (fd *Client) TransactGetItems(ctx context.Context, input *dynamodb.TransactGetItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	if err := validateTransactGetItemsInput(fd, input); err != nil {
		return nil, err
	}

	responses := make([]types.ItemResponse, 0, len(input.TransactItems))
	mode := consumedMode(string(input.ReturnConsumedCapacity))
	unitsByTable := map[string]float64{}

	for _, item := range input.TransactItems {
		get := item.Get

		out, err := fd.GetItem(ctx, &dynamodb.GetItemInput{
			TableName:                get.TableName,
			Key:                      get.Key,
			ExpressionAttributeNames: get.ExpressionAttributeNames,
			ProjectionExpression:     get.ProjectionExpression,
		})
		if err != nil {
			return nil, err
		}

		responses = append(responses, types.ItemResponse{Item: out.Item})

		unitsByTable[aws.ToString(get.TableName)] += 2 * capacity.ReadUnits(capacity.Size(mapDynamoToTypesMapItem(out.Item)), true)
	}

	return &dynamodb.TransactGetItemsOutput{
		Responses:        responses,
		ConsumedCapacity: sdkConsumedSlice(mode, unitsByTable, true),
	}, nil
}

func (fd *Client) runTransactItem(i, n int, item types.TransactWriteItem) error {
	switch {
	case item.Put != nil:
		return fd.runTransactPut(i, n, item.Put)
	case item.Update != nil:
		return fd.runTransactUpdate(i, n, item.Update)
	case item.Delete != nil:
		return fd.runTransactDelete(i, n, item.Delete)
	case item.ConditionCheck != nil:
		return fd.runTransactConditionCheck(i, n, item.ConditionCheck)
	default:
		return &smithy.GenericAPIError{Code: "ValidationException", Message: "transaction item must include one of Put, Update, Delete, or ConditionCheck"}
	}
}

func (fd *Client) runTransactPut(i, n int, put *types.Put) error {
	if vErr := validateExpressionAttributes(put.ExpressionAttributeNames, put.ExpressionAttributeValues, aws.ToString(put.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := fd.getTable(aws.ToString(put.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	if _, opErr := table.Put(mapDynamoToTypesTransactPut(put)); opErr != nil {
		return newTransactionCancelledError(i, n, opErr)
	}

	return nil
}

func (fd *Client) runTransactUpdate(i, n int, update *types.Update) error {
	if vErr := validateExpressionAttributes(update.ExpressionAttributeNames, update.ExpressionAttributeValues, aws.ToString(update.UpdateExpression), aws.ToString(update.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := fd.getTable(aws.ToString(update.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	_, opErr := table.Update(mapDynamoToTypesTransactUpdate(update))
	if opErr != nil {
		if errors.Is(opErr, interpreter.ErrSyntaxError) {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: opErr.Error()}
		}

		return newTransactionCancelledError(i, n, opErr)
	}

	return nil
}

func (fd *Client) runTransactDelete(i, n int, del *types.Delete) error {
	if vErr := validateExpressionAttributes(del.ExpressionAttributeNames, del.ExpressionAttributeValues, aws.ToString(del.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := fd.getTable(aws.ToString(del.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	if _, opErr := table.Delete(mapDynamoToTypesTransactDelete(del)); opErr != nil {
		return newTransactionCancelledError(i, n, opErr)
	}

	return nil
}

func (fd *Client) runTransactConditionCheck(i, n int, check *types.ConditionCheck) error {
	if vErr := validateExpressionAttributes(check.ExpressionAttributeNames, check.ExpressionAttributeValues, aws.ToString(check.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := fd.getTable(aws.ToString(check.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	keyMap := mapDynamoToTypesMapItem(check.Key)
	if vErr := mtypes.ValidateItemMap(keyMap); vErr != nil {
		return mapKnownError(mtypes.NewError("ValidationException", vErr.Error(), nil))
	}

	if keyErr := table.ValidatePrimaryKeyMap(keyMap); keyErr != nil {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: keyErr.Error()}
	}

	key, kErr := table.KeySchema.GetKey(table.AttributesDef, keyMap)
	if kErr != nil {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: kErr.Error()}
	}

	stored := table.Data[key]
	if stored == nil {
		stored = map[string]*mtypes.Item{}
	}

	matched, mErr := table.InterpreterMatch(interpreter.MatchInput{
		TableName:      table.Name,
		Expression:     aws.ToString(check.ConditionExpression),
		ExpressionType: interpreter.ExpressionTypeConditional,
		Item:           stored,
		Aliases:        check.ExpressionAttributeNames,
		Attributes:     mapDynamoToTypesMapItem(check.ExpressionAttributeValues),
	})
	if mErr != nil {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: mErr.Error()}
	}

	if !matched {
		checkErr := &mtypes.ConditionalCheckFailedException{
			MessageText: core.ErrConditionalRequestFailed.Error(),
		}

		if check.ReturnValuesOnConditionCheckFailure == types.ReturnValuesOnConditionCheckFailureAllOld {
			checkErr.Item = stored
		}

		return newTransactionCancelledError(i, n, checkErr)
	}

	return nil
}

func newTransactionCancelledError(i, n int, opErr error) error {
	var ccf *mtypes.ConditionalCheckFailedException
	if !errors.As(opErr, &ccf) {
		return mapKnownError(opErr)
	}

	reasons := make([]types.CancellationReason, n)
	for j := range reasons {
		reasons[j] = types.CancellationReason{Code: aws.String("None")}
	}

	reasons[i] = types.CancellationReason{
		Code:    aws.String("ConditionalCheckFailed"),
		Message: aws.String(core.ErrConditionalRequestFailed.Error()),
	}

	if ccf.Item != nil {
		reasons[i].Item = mapTypesToDynamoMapItem(ccf.Item)
	}

	return &types.TransactionCanceledException{
		Message:             aws.String("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]"),
		CancellationReasons: reasons,
	}
}

func (fd *Client) getTable(tableName string) (*core.Table, error) {
	table, ok := fd.tables[tableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String("Cannot do operations on a non-existent table")}
	}

	return table, nil
}

func getItemAttributesForOutput(table *core.Table, stored map[string]*mtypes.Item, projectionExpr string, aliases map[string]string) (map[string]types.AttributeValue, error) {
	if projectionExpr == "" {
		return mapTypesToDynamoMapItem(stored), nil
	}

	projected, err := table.LangInterpreter.Project(interpreter.ProjectInput{
		Expression: projectionExpr,
		Item:       stored,
		Aliases:    aliases,
	})
	if err != nil {
		return nil, err
	}

	return mapTypesToDynamoMapItem(projected), nil
}

func validateExpressionAttributes(exprNames map[string]string, exprValues map[string]types.AttributeValue, genericExpressions ...string) error {
	genericExpression := strings.Join(genericExpressions, " ")
	genericExpression = strings.TrimSpace(genericExpression)

	if genericExpression == "" && len(exprNames) == 0 && len(exprValues) == 0 {
		return nil
	}

	flattenNames := getKeysFromExpressionNames(exprNames)
	flattenValues := getKeysFromExpressionValues(exprValues)

	missingNames := getMissingSubstrs(genericExpression, flattenNames)
	missingValues := getMissingSubstrs(genericExpression, flattenValues)

	if len(missingNames) > 0 {
		sort.Strings(missingNames)

		return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: keys: {%s}", unusedExpressionAttributeNamesMsg, strings.Join(missingNames, ", "))}
	}

	err := validateSyntaxExpression(expressionAttributeNamesRegex, flattenNames, invalidExpressionAttributeName)
	if err != nil {
		return err
	}

	if len(missingValues) > 0 {
		if genericExpression == "" {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: expressionAttributeValuesOnlyWithExpressionsMsg}
		}

		sort.Strings(missingValues)

		return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: keys: {%s}", unusedExpressionAttributeValuesMsg, strings.Join(missingValues, ", "))}
	}

	err = validateSyntaxExpression(expressionAttributeValuesRegex, flattenValues, invalidExpressionAttributeValue)
	if err != nil {
		return err
	}

	return nil
}

func validateSyntaxExpression(regex *regexp.Regexp, expressions []string, errorMsg string) error {
	for _, exprName := range expressions {
		ok := regex.MatchString(exprName)
		if !ok {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: Syntax error; key: %s", errorMsg, exprName)}
		}
	}

	return nil
}

func getKeysFromExpressionNames(m map[string]string) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func getKeysFromExpressionValues(m map[string]types.AttributeValue) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func getMissingSubstrs(s string, substrs []string) []string {
	missingSubstrs := make([]string, 0, len(substrs))

	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			continue
		}

		missingSubstrs = append(missingSubstrs, substr)
	}

	return missingSubstrs
}
