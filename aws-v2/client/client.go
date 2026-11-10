package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
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
	ListTables(ctx context.Context, input *dynamodb.ListTablesInput, opts ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
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
	if err := newTable.SetAttributeDefinition(mapDynamoToTypesAttributeDefinitionSlice(input.AttributeDefinitions)); err != nil {
		return nil, mapKnownError(err)
	}

	newTable.BillingMode = aws.String(string(input.BillingMode))
	newTable.NativeInterpreter = *fd.nativeInterpreter
	newTable.UseNativeInterpreter = fd.useNativeInterpreter
	newTable.LangInterpreter = *fd.langInterpreter
	newTable.IndexActivationDelay = fd.indexActivationDelay

	if err := newTable.CreatePrimaryIndex(mapDynamoToTypesCreateTableInput(input)); err != nil {
		return nil, mapKnownError(err)
	}

	if err := newTable.AddIndexes(
		mapDynamoToTypesGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		mapDynamoToTypesLocalSecondaryIndexes(input.LocalSecondaryIndexes),
	); err != nil {
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

	changes := make([]*mtypes.GlobalSecondaryIndexUpdate, 0, len(input.GlobalSecondaryIndexUpdates))
	for _, change := range input.GlobalSecondaryIndexUpdates {
		changes = append(changes, mapDynamoTotypesGlobalSecondaryIndexUpdate(change))
	}

	desc, err := table.UpdateTable(tableName, mapDynamoToTypesAttributeDefinitionSlice(input.AttributeDefinitions), changes)

	return &dynamodb.UpdateTableOutput{
		TableDescription: mapTypesToDynamoTableDescription(desc),
	}, mapKnownError(err)
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

// ListTables returns the table names in byte order, one page at a time
func (fd *Client) ListTables(ctx context.Context, input *dynamodb.ListTablesInput, opts ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	names, last, err := core.ListTableNames(fd.tables, aws.ToString(input.ExclusiveStartTableName), input.Limit)
	if err != nil {
		return nil, mapKnownError(err)
	}

	return &dynamodb.ListTablesOutput{TableNames: names, LastEvaluatedTableName: last}, nil
}

// PutItem mock response for dynamodb
func (fd *Client) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := core.ValidateReturnValuesOnConditionCheckFailure(aws.String(string(input.ReturnValuesOnConditionCheckFailure))); err != nil {
		return nil, mapKnownError(err)
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

	var before map[string]*mtypes.Item
	if mode != capacity.ModeNone {
		before = table.StoredItem(putInput.Item)
	}

	item, err := table.Put(putInput)
	if err != nil {
		return &dynamodb.PutItemOutput{Attributes: mapTypesToDynamoMapItem(item)}, mapKnownError(err)
	}

	return &dynamodb.PutItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
		ConsumedCapacity: toSDKConsumed(capacity.ForItemWrite(
			mode, table, aws.ToString(input.TableName), putInput.Item, before, putInput.Item,
		)),
	}, nil
}

// DeleteItem mock response for dynamodb
func (fd *Client) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := core.ValidateReturnValuesOnConditionCheckFailure(aws.String(string(input.ReturnValuesOnConditionCheckFailure))); err != nil {
		return nil, mapKnownError(err)
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	deleteInput := mapDynamoToTypesDeleteItemInput(input)

	// Delete only returns the old item for ALL_OLD, so measure it before deleting.
	var stored map[string]*mtypes.Item
	if mode != capacity.ModeNone {
		stored = table.StoredItem(deleteInput.Key)
	}

	item, err := table.Delete(deleteInput)
	if err != nil {
		return nil, mapKnownError(err)
	}

	consumed := toSDKConsumed(capacity.ForItemWrite(
		mode, table, aws.ToString(input.TableName), deleteInput.Key, stored, nil,
	))

	if item == nil {
		return &dynamodb.DeleteItemOutput{ConsumedCapacity: consumed}, nil
	}

	return &dynamodb.DeleteItemOutput{
		Attributes:       mapTypesToDynamoMapItem(item),
		ConsumedCapacity: consumed,
	}, nil
}

// UpdateItem mock response for dynamodb
func (fd *Client) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := fd.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	// DynamoDB reports the Key ahead of the rest of the request. An unknown table is reported
	// where it always was, after the expressions.
	if table, tableErr := fd.getTable(aws.ToString(input.TableName)); tableErr == nil {
		if err := table.ValidatePrimaryKeyMap(mapDynamoToTypesMapItem(input.Key)); err != nil {
			return nil, mapKnownError(mtypes.NewError("ValidationException", err.Error(), nil))
		}
	}

	if err := core.ValidateReturnValuesOnConditionCheckFailure(aws.String(string(input.ReturnValuesOnConditionCheckFailure))); err != nil {
		return nil, mapKnownError(err)
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

	var before map[string]*mtypes.Item
	if mode != capacity.ModeNone {
		before = table.StoredItem(updateInput.Key)
	}

	item, err := table.Update(updateInput)
	if err != nil {
		return nil, mapKnownError(err)
	}

	var after map[string]*mtypes.Item
	if mode != capacity.ModeNone {
		after = table.StoredItem(updateInput.Key)
	}

	output := &dynamodb.UpdateItemOutput{
		ConsumedCapacity: toSDKConsumed(capacity.ForItemWrite(
			mode, table, aws.ToString(input.TableName), updateInput.Key, before, after,
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

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

	if mode != capacity.ModeNone {
		sizeSource := stored
		if len(sizeSource) == 0 {
			sizeSource = keyMap
		}

		output.ConsumedCapacity = toSDKConsumed(capacity.ForRead(
			mode,
			aws.ToString(input.TableName), "", "",
			capacity.Size(sizeSource), aws.ToBool(input.ConsistentRead),
		))
	}

	return output, nil
}

// Query mock response for dynamodb
func (fd *Client) Query(ctx context.Context, input *dynamodb.QueryInput, opt ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

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

	res, err := table.SearchData(mapDynamoToTypesQueryInput(input, indexName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	// ScannedCount is never smaller than Count, so checking it covers both conversions.
	count := len(res.Items)
	if res.ScannedCount > math.MaxInt32 {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: "Result count exceeds maximum allowed value"}
	}

	output := &dynamodb.QueryOutput{
		Count:            int32(count),
		ScannedCount:     int32(res.ScannedCount),
		LastEvaluatedKey: mapTypesToDynamoMapItem(res.LastEvaluatedKey),
	}

	if input.Select != types.SelectCount {
		output.Items = mapTypesToDynamoSliceMapItem(res.Items)
	}

	if mode != capacity.ModeNone {
		output.ConsumedCapacity = toSDKConsumed(capacity.ForRead(
			mode,
			aws.ToString(input.TableName), indexName, table.IndexKind(indexName),
			capacity.SumSize(res.Items), aws.ToBool(input.ConsistentRead),
		))
	}

	return output, nil
}

// Scan mock scan operation
func (fd *Client) Scan(ctx context.Context, input *dynamodb.ScanInput, opt ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

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

	res, err := table.SearchData(core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ExclusiveStartKey:         mapDynamoToTypesMapItem(input.ExclusiveStartKey),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ProjectionExpression:      aws.ToString(input.ProjectionExpression),
		Select:                    string(input.Select),
		ScanIndexForward:          true,
		Scan:                      true,
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	// ScannedCount is never smaller than Count, so checking it covers both conversions.
	count := len(res.Items)
	if res.ScannedCount > math.MaxInt32 {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: "Result count exceeds maximum allowed value"}
	}

	output := &dynamodb.ScanOutput{
		Count:            int32(count),
		ScannedCount:     int32(res.ScannedCount),
		LastEvaluatedKey: mapTypesToDynamoMapItem(res.LastEvaluatedKey),
	}

	if input.Select != types.SelectCount {
		output.Items = mapTypesToDynamoSliceMapItem(res.Items)
	}

	if mode != capacity.ModeNone {
		output.ConsumedCapacity = toSDKConsumed(capacity.ForRead(
			mode,
			aws.ToString(input.TableName), indexName, table.IndexKind(indexName),
			capacity.SumSize(res.Items), aws.ToBool(input.ConsistentRead),
		))
	}

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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	emulation := fd.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	if err := fd.validateBatchWriteRequests(input.RequestItems); err != nil {
		return &dynamodb.BatchWriteItemOutput{}, err
	}

	unprocessed := map[string][]types.WriteRequest{}
	acc := capacity.NewWriteAccumulator()

	for table, reqs := range input.RequestItems {
		for i, req := range reqs {
			if emulation.unprocessed(table, i, batchWriteRequestKey(req)) {
				unprocessed[table] = append(unprocessed[table], req)

				continue
			}

			// Sizes are measured before the request is applied: a delete bills the item it
			// removes and a put the larger of the item it replaces and the item it writes.
			var target capacity.WriteTarget

			if mode != capacity.ModeNone {
				target = batchWriteTarget(table, req)
				fd.snapshotLocked(&target)
			}

			if err := executeBatchWriteRequest(ctx, fd, aws.String(table), req); err != nil {
				return &dynamodb.BatchWriteItemOutput{}, err
			}

			if mode != capacity.ModeNone {
				size, indexes := fd.billLocked(&target)
				acc.Add(table, size, indexes, 1)
			}
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems:      unprocessed,
		ItemCollectionMetrics: fd.itemCollectionMetrics,
		ConsumedCapacity:      sdkConsumedList(acc.Consumed(mode)),
	}, nil
}

// validateBatchWriteRequests rejects the whole batch when any request is invalid, so no request
// is applied before one fails (D8). The checks and their order live in core.ValidateBatchWrite.
func (fd *Client) validateBatchWriteRequests(requestItems map[string][]types.WriteRequest) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	requests := make(map[string][]core.WriteRequest, len(requestItems))

	for name, reqs := range requestItems {
		requests[name] = nil

		for _, req := range reqs {
			var r core.WriteRequest

			switch {
			case req.PutRequest != nil:
				r.Put = mapDynamoToTypesMapItem(req.PutRequest.Item)
			case req.DeleteRequest != nil:
				r.Delete = mapDynamoToTypesMapItem(req.DeleteRequest.Key)
			}

			requests[name] = append(requests[name], r)
		}
	}

	lookup := func(name string) (*core.Table, bool) {
		table, ok := fd.tables[name]

		return table, ok
	}

	if err := core.ValidateBatchWrite(lookup, requests); err != nil {
		return mapKnownError(err)
	}

	return nil
}

// batchWriteTarget describes a put or delete sub-request for billing.
func batchWriteTarget(table string, req types.WriteRequest) capacity.WriteTarget {
	if req.PutRequest != nil {
		return capacity.WriteTarget{Table: table, Kind: capacity.WritePut, Key: mapDynamoToTypesMapItem(req.PutRequest.Item)}
	}

	if req.DeleteRequest != nil {
		return capacity.WriteTarget{Table: table, Kind: capacity.WriteDelete, Key: mapDynamoToTypesMapItem(req.DeleteRequest.Key)}
	}

	return capacity.WriteTarget{Table: table, Kind: capacity.WriteCheck}
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
	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

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

			if mode != capacity.ModeNone {
				unitsByTable[tableName] += capacity.ReadUnits(capacity.Size(mapDynamoToTypesMapItem(out.Item)), consistent)
			}

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
		ConsumedCapacity: sdkConsumedSlice(mode, unitsByTable),
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	if err := validateTransactReturnValuesOnConditionCheckFailure(input.TransactItems); err != nil {
		return nil, err
	}

	snapshots, err := fd.prepareTransact(input.TransactItems)
	if err != nil {
		return nil, err
	}

	// Sizes are measured before the writes: a delete bills the item it removes and an update
	// the larger of the item before and after.
	var targets []capacity.WriteTarget
	if mode != capacity.ModeNone {
		targets = transactWriteTargets(input.TransactItems)

		for i := range targets {
			targets[i].Snapshot(fd.tableOrNil)
		}
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

	acc := capacity.NewWriteAccumulator()

	for i := range targets {
		size, indexes := targets[i].Bill(fd.tableOrNil)
		acc.AddTransact(targets[i].Table, size, indexes)
	}

	return &dynamodb.TransactWriteItemsOutput{
		ConsumedCapacity: sdkConsumedList(acc.Consumed(mode)),
	}, nil
}

// transactWriteTargets describes every write of the transaction for billing. A ConditionCheck is
// billed by its key.
func transactWriteTargets(items []types.TransactWriteItem) []capacity.WriteTarget {
	targets := make([]capacity.WriteTarget, 0, len(items))

	for _, item := range items {
		switch {
		case item.Put != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.Put.TableName), Kind: capacity.WritePut, Key: mapDynamoToTypesMapItem(item.Put.Item),
			})
		case item.Update != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.Update.TableName), Kind: capacity.WriteUpdate, Key: mapDynamoToTypesMapItem(item.Update.Key),
			})
		case item.Delete != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.Delete.TableName), Kind: capacity.WriteDelete, Key: mapDynamoToTypesMapItem(item.Delete.Key),
			})
		case item.ConditionCheck != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.ConditionCheck.TableName), Kind: capacity.WriteCheck, Key: mapDynamoToTypesMapItem(item.ConditionCheck.Key),
			})
		}
	}

	return targets
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	responses := make([]types.ItemResponse, 0, len(input.TransactItems))
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

		if mode != capacity.ModeNone {
			unitsByTable[aws.ToString(get.TableName)] += capacity.TransactReadUnits(capacity.Size(mapDynamoToTypesMapItem(out.Item)))
		}
	}

	return &dynamodb.TransactGetItemsOutput{
		Responses:        responses,
		ConsumedCapacity: sdkConsumedSlice(mode, unitsByTable),
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
		return newTransactionCancelledError(i, n, opErr)
	}

	return nil
}

// validateTransactReturnValuesOnConditionCheckFailure rejects the whole transaction, before any
// write, when an item has a ReturnValuesOnConditionCheckFailure outside ALL_OLD and NONE.
// DynamoDB Local accepts the value; minidyn follows the API reference (see e2e/README.md).
func validateTransactReturnValuesOnConditionCheckFailure(items []types.TransactWriteItem) error {
	for i, item := range items {
		var shape string

		var rv types.ReturnValuesOnConditionCheckFailure

		switch {
		case item.Put != nil:
			shape, rv = "put", item.Put.ReturnValuesOnConditionCheckFailure
		case item.Update != nil:
			shape, rv = "update", item.Update.ReturnValuesOnConditionCheckFailure
		case item.Delete != nil:
			shape, rv = "delete", item.Delete.ReturnValuesOnConditionCheckFailure
		case item.ConditionCheck != nil:
			shape, rv = "conditionCheck", item.ConditionCheck.ReturnValuesOnConditionCheckFailure
		default:
			continue
		}

		if err := core.ValidateTransactItemReturnValuesOnConditionCheckFailure(i, shape, aws.String(string(rv))); err != nil {
			return mapKnownError(err)
		}
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
		mode := string(check.ReturnValuesOnConditionCheckFailure)

		checkErr := core.ConditionalCheckFailed(&mode, stored)

		return newTransactionCancelledError(i, n, checkErr)
	}

	return nil
}

func newTransactionCancelledError(i, n int, opErr error) error {
	var ccf *mtypes.ConditionalCheckFailedException
	if !errors.As(opErr, &ccf) {
		return mapKnownError(opErr)
	}

	codes := core.ConditionalCheckFailedCancellationCodes(i, n)

	reasons := make([]types.CancellationReason, n)
	for j, code := range codes {
		reasons[j] = types.CancellationReason{Code: aws.String(code)}
	}

	reasons[i].Message = aws.String(core.ConditionalCheckFailedReasonMessage)

	if ccf.Item != nil {
		reasons[i].Item = mapTypesToDynamoMapItem(ccf.Item)
	}

	return &types.TransactionCanceledException{
		Message:             aws.String(core.TransactionCanceledMessage(codes)),
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

	// An undefined placeholder is reported by the core, with its expression label, ahead of
	// an unused value, as DynamoDB does.
	if len(missingValues) > 0 && !hasUndefinedValue(genericExpression, flattenValues) {
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
			return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: Syntax error; key: %q", errorMsg, exprName)}
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

// hasUndefinedValue reports whether the expressions use a value placeholder that is not among
// the declared ExpressionAttributeValues keys.
func hasUndefinedValue(expression string, declared []string) bool {
	return interpreter.HasUndeclaredExpressionAttributeValues(expression, func(name string) bool {
		return slices.Contains(declared, name)
	})
}
