package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/truora/minidyn/capacity"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// Client implements a DynamoDB-like engine backed by core.Table.
type Client struct {
	tables               map[string]*core.Table
	mu                   sync.Mutex
	langInterpreter      *interpreter.Language
	nativeInterpreter    *interpreter.Native
	useNativeInterpreter bool
	forceFailureErr      error
	tableFailureErrs     map[string]error
	unprocessedMatchers  map[string]func(int, map[string]*AttributeValue) bool
	indexActivationDelay time.Duration
}

// NewClient creates a new in-memory DynamoDB-compatible client used by the HTTP server.
func NewClient() *Client {
	return &Client{
		tables:              map[string]*core.Table{},
		mu:                  sync.Mutex{},
		nativeInterpreter:   interpreter.NewNativeInterpreter(),
		langInterpreter:     &interpreter.Language{},
		tableFailureErrs:    map[string]error{},
		unprocessedMatchers: map[string]func(int, map[string]*AttributeValue) bool{},
	}
}

func (c *Client) setFailureCondition(err error) {
	c.forceFailureErr = err
}

// failureKey builds the lookup key for a table-scoped failure. An empty index
// means the failure applies to the whole table.
func failureKey(table, index string) string {
	return table + "\x00" + index
}

// setTableFailureCondition scopes a failure to a table, or to a specific index of
// that table when index is not empty. A nil err clears that exact scope.
func (c *Client) setTableFailureCondition(table, index string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := failureKey(table, index)

	if err == nil {
		delete(c.tableFailureErrs, key)

		return
	}

	c.tableFailureErrs[key] = err
}

// failureErrFor resolves the emulated error for an operation targeting the given
// table and index. A global failure overrides everything; otherwise an
// index-scoped failure (when index is set) wins over a table-wide one. Callers
// must hold c.mu.
func (c *Client) failureErrFor(table, index string) error {
	if c.forceFailureErr != nil {
		return c.forceFailureErr
	}

	if index != "" {
		if err := c.tableFailureErrs[failureKey(table, index)]; err != nil {
			return err
		}
	}

	return c.tableFailureErrs[failureKey(table, "")]
}

// setUnprocessedMatcher installs a predicate that marks matching sub-requests of a
// batch operation on tableName as unprocessed. A nil match clears the predicate for
// that table.
func (c *Client) setUnprocessedMatcher(tableName string, match func(int, map[string]*AttributeValue) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if match == nil {
		delete(c.unprocessedMatchers, tableName)

		return
	}

	c.unprocessedMatchers[tableName] = match
}

// clearUnprocessedMatchers removes every batch partial-failure predicate.
func (c *Client) clearUnprocessedMatchers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.unprocessedMatchers = map[string]func(int, map[string]*AttributeValue) bool{}
}

// batchEmulation is a lock-free snapshot of the failure emulation that applies to a
// batch operation: a hard failure error (global or table-scoped) that fails the whole
// call, and the per-table partial-failure predicates.
type batchEmulation struct {
	failErr  error
	matchers map[string]func(int, map[string]*AttributeValue) bool
}

// unprocessed reports whether sub-request n (raw payload) of tableName should be
// returned as unprocessed instead of being executed.
func (e batchEmulation) unprocessed(tableName string, n int, raw map[string]*AttributeValue) bool {
	match, ok := e.matchers[tableName]

	return ok && match(n, raw)
}

// batchEmulationFor snapshots the emulation state relevant to a batch touching the
// given tables. A global failure overrides everything; otherwise any table-scoped
// failure hard-fails the whole batch. Takes c.mu briefly so the loop can run lock-free.
func (c *Client) batchEmulationFor(tables ...string) batchEmulation {
	c.mu.Lock()
	defer c.mu.Unlock()

	snap := batchEmulation{}

	if c.forceFailureErr != nil {
		snap.failErr = c.forceFailureErr

		return snap
	}

	for _, table := range tables {
		if err := c.failureErrFor(table, ""); err != nil {
			snap.failErr = err

			return snap
		}
	}

	for _, table := range tables {
		if match, ok := c.unprocessedMatchers[table]; ok {
			if snap.matchers == nil {
				snap.matchers = map[string]func(int, map[string]*AttributeValue) bool{}
			}

			snap.matchers[table] = match
		}
	}

	return snap
}

func (c *Client) setIndexActivationDelay(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.indexActivationDelay = delay

	for _, table := range c.tables {
		table.IndexActivationDelay = delay
	}
}

// Table helpers
func (c *Client) getTable(tableName string) (*core.Table, error) {
	table, ok := c.tables[tableName]
	if !ok {
		return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("Cannot do operations on a non-existent table")}
	}

	return table, nil
}

// CreateTable creates a new table in the in-memory engine.
func (c *Client) CreateTable(ctx context.Context, input *CreateTableInput) (*CreateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if _, ok := c.tables[tableName]; ok {
		return nil, &ddbtypes.ResourceInUseException{Message: aws.String("Cannot create preexisting table")}
	}

	table := core.NewTable(tableName)
	table.SetAttributeDefinition(mapAttributeDefinitions(input.AttributeDefinitions))
	table.BillingMode = toStringPtr(string(input.BillingMode))
	table.NativeInterpreter = *c.nativeInterpreter
	table.UseNativeInterpreter = c.useNativeInterpreter
	table.LangInterpreter = *c.langInterpreter
	table.IndexActivationDelay = c.indexActivationDelay

	if err := table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema:             mapKeySchema(input.KeySchema),
		ProvisionedThroughput: mapProvisionedThroughput(input.ProvisionedThroughput),
	}); err != nil {
		return nil, mapKnownError(err)
	}

	if err := table.AddGlobalIndexes(mapGSI(input.GlobalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	if err := table.AddLocalIndexes(mapLSI(input.LocalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	c.tables[tableName] = table

	return &CreateTableOutput{TableDescription: mapTableDescriptionToDDB(table.Description(tableName))}, nil
}

// UpdateTable applies metadata changes, including GSI updates.
func (c *Client) UpdateTable(ctx context.Context, input *UpdateTableInput) (*UpdateTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, ok := c.tables[tableName]
	if !ok {
		return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("Cannot do operations on a non-existent table")}
	}

	if input.AttributeDefinitions != nil {
		table.SetAttributeDefinition(mapAttributeDefinitions(input.AttributeDefinitions))
	}

	for _, change := range mapGSIUpdate(input.GlobalSecondaryIndexUpdates) {
		if err := table.ApplyIndexChange(change); err != nil {
			return &UpdateTableOutput{TableDescription: mapTableDescriptionToDDB(table.Description(tableName))}, mapKnownError(err)
		}
	}

	return &UpdateTableOutput{TableDescription: mapTableDescriptionToDDB(table.Description(tableName))}, nil
}

// DeleteTable removes a table and its data.
func (c *Client) DeleteTable(ctx context.Context, input *DeleteTableInput) (*DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := c.getTable(tableName)
	if err != nil {
		return nil, err
	}

	desc := mapTableDescriptionToDDB(table.Description(tableName))
	delete(c.tables, tableName)

	return &DeleteTableOutput{TableDescription: desc}, nil
}

// DescribeTable returns table metadata.
func (c *Client) DescribeTable(ctx context.Context, input *DescribeTableInput) (*DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := c.getTable(tableName)
	if err != nil {
		return nil, err
	}

	return &DescribeTableOutput{Table: mapTableDescriptionToDDB(table.Description(tableName))}, nil
}

// ClearTable removes all data from a specific table, including its indexes.
func (c *Client) ClearTable(tableName string) error {
	table, err := c.getTable(tableName)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	table.Clear()

	for _, index := range table.Indexes {
		index.Clear()
	}

	return nil
}

// ClearAllTables removes all data from every table and its indexes.
func (c *Client) ClearAllTables() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, table := range c.tables {
		table.Clear()

		for _, index := range table.Indexes {
			index.Clear()
		}
	}
}

// Reset removes all tables and their indexes from the in-memory client.
func (c *Client) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name := range c.tables {
		delete(c.tables, name)
	}
}

// PutItem inserts or replaces an item.
func (c *Client) PutItem(ctx context.Context, input *PutItemInput) (*PutItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.ConditionExpression),
	); err != nil {
		return nil, err
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	putItem := mapAttributeValueMapToTypes(input.Item)

	newSize := capacity.Size(putItem)

	oldSize := 0
	if key, kerr := table.KeySchema.GetKey(table.AttributesDef, putItem); kerr == nil {
		oldSize = capacity.Size(table.Data[key])
	}

	item, err := table.Put(&types.PutItemInput{
		TableName:                   input.TableName,
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:    input.ExpressionAttributeNames,
		ExpressionAttributeValues:   mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Item:                        putItem,
		ReturnConsumedCapacity:      toStringPtr(string(input.ReturnConsumedCapacity)),
		ReturnItemCollectionMetrics: toStringPtr(string(input.ReturnItemCollectionMetrics)),
		ReturnValues:                toStringPtr(string(input.ReturnValues)),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	return &PutItemOutput{
		Attributes: mapTypesMapToAttributeValue(item),
		ConsumedCapacity: toWireConsumed(capacity.ForWrite(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), max(oldSize, newSize),
		)),
	}, nil
}

// DeleteItem removes an item and optionally returns old values.
func (c *Client) DeleteItem(ctx context.Context, input *DeleteItemInput) (*DeleteItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.ConditionExpression),
	); err != nil {
		return nil, err
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	deleteKey := mapAttributeValueMapToTypes(input.Key)

	item, err := table.Delete(&types.DeleteItemInput{
		TableName:                 input.TableName,
		ConditionExpression:       input.ConditionExpression,
		ConditionalOperator:       toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:  toStringPtrMap(input.ExpressionAttributeNames),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Key:                       deleteKey,
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	sizeSource := item
	if len(sizeSource) == 0 {
		sizeSource = deleteKey
	}

	consumed := toWireConsumed(capacity.ForWrite(
		consumedMode(string(input.ReturnConsumedCapacity)),
		aws.ToString(input.TableName), capacity.Size(sizeSource),
	))

	if input.ReturnValues == ddbtypes.ReturnValueAllOld {
		return &DeleteItemOutput{Attributes: mapTypesMapToAttributeValue(item), ConsumedCapacity: consumed}, nil
	}

	return &DeleteItemOutput{ConsumedCapacity: consumed}, nil
}

// UpdateItem modifies attributes of an item using an update expression.
func (c *Client) UpdateItem(ctx context.Context, input *UpdateItemInput) (*UpdateItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.UpdateExpression),
		aws.ToString(input.ConditionExpression),
	); err != nil {
		return nil, err
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	updateKeyMap := mapAttributeValueMapToTypes(input.Key)

	updateKey, keyErr := table.KeySchema.GetKey(table.AttributesDef, updateKeyMap)

	beforeSize := 0
	if keyErr == nil {
		beforeSize = capacity.Size(table.Data[updateKey])
	}

	item, err := table.Update(&types.UpdateItemInput{
		TableName:                           input.TableName,
		ConditionExpression:                 input.ConditionExpression,
		ConditionalOperator:                 toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:            input.ExpressionAttributeNames,
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Key:                                 updateKeyMap,
		UpdateExpression:                    aws.ToString(input.UpdateExpression),
		ReturnValues:                        toStringPtr(string(input.ReturnValues)),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(input.ReturnValuesOnConditionCheckFailure)),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	afterSize := beforeSize
	if keyErr == nil {
		afterSize = capacity.Size(table.Data[updateKey])
	}

	return &UpdateItemOutput{
		Attributes: mapTypesMapToAttributeValue(item),
		ConsumedCapacity: toWireConsumed(capacity.ForWrite(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), max(beforeSize, afterSize),
		)),
	}, nil
}

// GetItem returns a single item by key.
func (c *Client) GetItem(ctx context.Context, input *GetItemInput) (*GetItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := validateExpressionAttributes(input.ExpressionAttributeNames, nil, aws.ToString(input.ProjectionExpression)); err != nil {
		return nil, err
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	keyMap := mapAttributeValueMapToTypes(input.Key)
	if vErr := types.ValidateItemMap(keyMap); vErr != nil {
		return nil, mapKnownError(types.NewError("ValidationException", vErr.Error(), nil))
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

	sizeSource := stored
	if len(sizeSource) == 0 {
		sizeSource = keyMap
	}

	return &GetItemOutput{
		Item: item,
		ConsumedCapacity: toWireConsumed(capacity.ForRead(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), "", "",
			capacity.Size(sizeSource), aws.ToBool(input.ConsistentRead),
		)),
	}, nil
}

// Query searches items by key condition and optional filter.
func (c *Client) Query(ctx context.Context, input *QueryInput) (*QueryOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ferr := c.failureErrFor(aws.ToString(input.TableName), aws.ToString(input.IndexName)); ferr != nil {
		return nil, ferr
	}

	if err := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.KeyConditionExpression),
		aws.ToString(input.FilterExpression),
		aws.ToString(input.ProjectionExpression),
	); err != nil {
		return nil, err
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	items, last, err := table.SearchData(core.QueryInput{
		Index:                     aws.ToString(input.IndexName),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapAttributeValueMapToTypes(input.ExclusiveStartKey),
		KeyConditionExpression:    aws.ToString(input.KeyConditionExpression),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ProjectionExpression:      aws.ToString(input.ProjectionExpression),
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ScanIndexForward:          aws.ToBool(input.ScanIndexForward),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	count := int32(len(items))
	indexName := aws.ToString(input.IndexName)

	return &QueryOutput{
		Items:            mapTypesSliceToAttributeValue(items),
		Count:            count,
		LastEvaluatedKey: mapTypesMapToAttributeValue(last),
		ConsumedCapacity: toWireConsumed(capacity.ForRead(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), indexName, table.IndexKind(indexName),
			capacity.SumSize(items), aws.ToBool(input.ConsistentRead),
		)),
	}, nil
}

// Scan iterates items (optionally filtered) and returns a page of results.
func (c *Client) Scan(ctx context.Context, input *ScanInput) (*ScanOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ferr := c.failureErrFor(aws.ToString(input.TableName), aws.ToString(input.IndexName)); ferr != nil {
		return nil, ferr
	}

	if err := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.ProjectionExpression),
		aws.ToString(input.FilterExpression),
	); err != nil {
		return nil, err
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	items, last, err := table.SearchData(core.QueryInput{
		Index:                     aws.ToString(input.IndexName),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapAttributeValueMapToTypes(input.ExclusiveStartKey),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ProjectionExpression:      aws.ToString(input.ProjectionExpression),
		Limit:                     int64(aws.ToInt32(input.Limit)),
		Scan:                      true,
		ScanIndexForward:          true,
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	count := int32(len(items))
	indexName := aws.ToString(input.IndexName)

	return &ScanOutput{
		Items:            mapTypesSliceToAttributeValue(items),
		Count:            count,
		LastEvaluatedKey: mapTypesMapToAttributeValue(last),
		ConsumedCapacity: toWireConsumed(capacity.ForRead(
			consumedMode(string(input.ReturnConsumedCapacity)),
			aws.ToString(input.TableName), indexName, table.IndexKind(indexName),
			capacity.SumSize(items), aws.ToBool(input.ConsistentRead),
		)),
	}, nil
}

const (
	batchWriteItemRequestsLimit = 25
	batchGetItemRequestsLimit   = 100
)

// DynamoDB returns this message when a WriteRequest has both Put and Delete, or neither.
var errBatchWriteRequestShape = &smithy.GenericAPIError{
	Code:    "ValidationException",
	Message: "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes",
}

func validateWriteRequest(req WriteRequest) error {
	hasPut := req.PutRequest != nil

	hasDel := req.DeleteRequest != nil
	if hasPut == hasDel {
		return errBatchWriteRequestShape
	}

	return nil
}

func validateBatchWriteItemInput(input *BatchWriteItemInput) error {
	if input == nil {
		return nil
	}

	count := 0

	for _, reqs := range input.RequestItems {
		for _, req := range reqs {
			if err := validateWriteRequest(req); err != nil {
				return err
			}

			count++
		}
	}

	if count > batchWriteItemRequestsLimit {
		return &smithy.GenericAPIError{
			Code:    "ValidationException",
			Message: "Too many items requested for the BatchWriteItem call",
		}
	}

	return nil
}

func batchWriteRequestKey(req WriteRequest) map[string]*AttributeValue {
	if req.PutRequest != nil {
		return req.PutRequest.Item
	}

	if req.DeleteRequest != nil {
		return req.DeleteRequest.Key
	}

	return nil
}

// BatchWriteItem runs put and delete sub-requests in order. An emulated failure
// (global EmulateFailure or table-scoped EmulateFailureForTable touching any table in
// the batch) hard-fails the whole call, mirroring DynamoDB returning a 500 for the
// request. UnprocessedItems is produced only by EmulateUnprocessedItems, whose
// predicate selects individual sub-requests to leave unprocessed while the rest are
// applied.
func (c *Client) BatchWriteItem(ctx context.Context, input *BatchWriteItemInput) (*BatchWriteItemOutput, error) {
	if err := validateBatchWriteItemInput(input); err != nil {
		return nil, err
	}

	emulation := c.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	unprocessed := map[string][]WriteRequest{}
	unitsByTable := map[string]float64{}

	for tableName, reqs := range input.RequestItems {
		for i, req := range reqs {
			if emulation.unprocessed(tableName, i, batchWriteRequestKey(req)) {
				unprocessed[tableName] = append(unprocessed[tableName], req)

				continue
			}

			var err error
			if req.PutRequest != nil {
				_, err = c.PutItem(ctx, &PutItemInput{
					TableName: aws.String(tableName),
					Item:      req.PutRequest.Item,
				})
			}

			if req.DeleteRequest != nil {
				_, err = c.DeleteItem(ctx, &DeleteItemInput{
					TableName: aws.String(tableName),
					Key:       req.DeleteRequest.Key,
				})
			}

			if err != nil {
				return nil, err
			}

			unitsByTable[tableName] += capacity.WriteUnits(capacity.Size(batchWriteRequestItem(req)))
		}
	}

	return &BatchWriteItemOutput{
		UnprocessedItems: unprocessed,
		ConsumedCapacity: wireConsumedSlice(consumedMode(string(input.ReturnConsumedCapacity)), unitsByTable, false),
	}, nil
}

// batchWriteRequestItem returns the internal item used to size a write request: the put
// item, or the delete key when sizing a delete.
func batchWriteRequestItem(req WriteRequest) map[string]*types.Item {
	if req.PutRequest != nil {
		return mapAttributeValueMapToTypes(req.PutRequest.Item)
	}

	if req.DeleteRequest != nil {
		return mapAttributeValueMapToTypes(req.DeleteRequest.Key)
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

func validateBatchGetItemInput(input *BatchGetItemInput) error {
	if input == nil {
		return nil
	}

	count := 0

	for _, reqs := range input.RequestItems {
		count += len(reqs.Keys)
	}

	if count == 0 {
		return &smithy.GenericAPIError{
			Code:    "ValidationException",
			Message: "1 validation error detected: Value '{}' at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
		}
	}

	if count > batchGetItemRequestsLimit {
		return &smithy.GenericAPIError{
			Code:    "ValidationException",
			Message: "Too many items requested for the BatchGetItem call",
		}
	}

	return nil
}

// BatchGetItem retrieves multiple items across one or more tables. Validation errors
// (invalid key schema, malformed AttributeValues, or missing tables) fail the whole
// batch. An emulated failure (global EmulateFailure or table-scoped
// EmulateFailureForTable touching any table in the batch) hard-fails the whole call.
// UnprocessedKeys is produced only by EmulateUnprocessedItems, whose predicate selects
// individual keys to leave unprocessed while the rest are fetched.
func (c *Client) BatchGetItem(ctx context.Context, input *BatchGetItemInput) (*BatchGetItemOutput, error) {
	if err := validateBatchGetItemInput(input); err != nil {
		return nil, err
	}

	emulation := c.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	responses := map[string][]map[string]*AttributeValue{}
	unprocessed := map[string]KeysAndAttributes{}
	unitsByTable := map[string]float64{}

	for tableName, reqs := range input.RequestItems {
		tableResponses, unprocessedKeys, units, err := c.batchGetItemForTable(ctx, tableName, reqs, emulation)
		if err != nil {
			return nil, err
		}

		responses[tableName] = tableResponses
		unitsByTable[tableName] += units

		if len(unprocessedKeys) > 0 {
			tableUnprocessed := reqs
			tableUnprocessed.Keys = unprocessedKeys
			unprocessed[tableName] = tableUnprocessed
		}
	}

	return &BatchGetItemOutput{
		Responses:        responses,
		UnprocessedKeys:  unprocessed,
		ConsumedCapacity: wireConsumedSlice(consumedMode(string(input.ReturnConsumedCapacity)), unitsByTable, true),
	}, nil
}

func (c *Client) batchGetItemForTable(ctx context.Context, tableName string, reqs KeysAndAttributes, emulation batchEmulation) ([]map[string]*AttributeValue, []map[string]*AttributeValue, float64, error) {
	if err := validateExpressionAttributes(reqs.ExpressionAttributeNames, nil, aws.ToString(reqs.ProjectionExpression)); err != nil {
		return nil, nil, 0, err
	}

	responses := make([]map[string]*AttributeValue, 0, len(reqs.Keys))
	unprocessedKeys := make([]map[string]*AttributeValue, 0, len(reqs.Keys))

	consistent := aws.ToBool(reqs.ConsistentRead)
	units := 0.0

	for i, key := range reqs.Keys {
		if emulation.unprocessed(tableName, i, key) {
			unprocessedKeys = append(unprocessedKeys, key)

			continue
		}

		item, err := c.GetItem(ctx, &GetItemInput{
			TableName:                aws.String(tableName),
			Key:                      key,
			ConsistentRead:           reqs.ConsistentRead,
			ExpressionAttributeNames: reqs.ExpressionAttributeNames,
			ProjectionExpression:     reqs.ProjectionExpression,
		})
		if err != nil {
			return nil, nil, 0, err
		}

		units += capacity.ReadUnits(capacity.Size(mapAttributeValueMapToTypes(item.Item)), consistent)

		if len(item.Item) > 0 {
			responses = append(responses, item.Item)
		}
	}

	return responses, unprocessedKeys, units, nil
}

func (c *Client) prepareTransact(items []TransactWriteItem) (map[string]core.TableSnapshot, error) {
	snapshots := map[string]core.TableSnapshot{}
	seenKeys := make(map[string]struct{}, len(items))

	for _, item := range items {
		var tableName string
		var rawKeyMap map[string]*AttributeValue

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

		if ferr := c.failureErrFor(tableName, ""); ferr != nil {
			return nil, ferr
		}

		if _, alreadySnapped := snapshots[tableName]; !alreadySnapped {
			table, err := c.getTable(tableName)
			if err != nil {
				return nil, mapKnownError(err)
			}

			snapshots[tableName] = table.Snapshot()
		}

		table := c.tables[tableName]
		internalKeyMap := mapAttributeValueMapToTypes(rawKeyMap)

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

// TransactWriteItems executes a set of Put, Update, Delete, and ConditionCheck operations atomically.
// If any operation fails the entire transaction is  rolled back via table snapshots captured before execution begins.
func (c *Client) TransactWriteItems(ctx context.Context, input *TransactWriteItemsInput) (*TransactWriteItemsOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
	}

	snapshots, err := c.prepareTransact(input.TransactItems)
	if err != nil {
		return nil, err
	}

	var execErr error

	defer func() {
		if execErr != nil {
			for name, snap := range snapshots {
				c.tables[name].Restore(snap)
			}
		}
	}()

	n := len(input.TransactItems)

	for i, item := range input.TransactItems {
		execErr = c.runTransactItem(i, n, item)
		if execErr != nil {
			return nil, execErr
		}
	}

	unitsByTable := map[string]float64{}

	for _, item := range input.TransactItems {
		tableName, size := c.transactWriteSize(item)
		if tableName == "" {
			continue
		}

		unitsByTable[tableName] += 2 * capacity.WriteUnits(size)
	}

	return &TransactWriteItemsOutput{
		ConsumedCapacity: wireConsumedSlice(consumedMode(string(input.ReturnConsumedCapacity)), unitsByTable, false),
	}, nil
}

// transactWriteSize returns the table name and byte size used to bill a transact write item.
// Update sizes the stored item after the update; Delete/ConditionCheck size the key.
func (c *Client) transactWriteSize(item TransactWriteItem) (string, int) {
	switch {
	case item.Put != nil:
		return aws.ToString(item.Put.TableName), capacity.Size(mapAttributeValueMapToTypes(item.Put.Item))
	case item.Update != nil:
		return aws.ToString(item.Update.TableName), c.itemSizeByKey(aws.ToString(item.Update.TableName), item.Update.Key)
	case item.Delete != nil:
		return aws.ToString(item.Delete.TableName), capacity.Size(mapAttributeValueMapToTypes(item.Delete.Key))
	case item.ConditionCheck != nil:
		return aws.ToString(item.ConditionCheck.TableName), capacity.Size(mapAttributeValueMapToTypes(item.ConditionCheck.Key))
	default:
		return "", 0
	}
}

// itemSizeByKey returns the byte size of the stored item for a key, falling back to the
// key's own size when the table or item is not found.
func (c *Client) itemSizeByKey(tableName string, key map[string]*AttributeValue) int {
	keyMap := mapAttributeValueMapToTypes(key)

	table, err := c.getTable(tableName)
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

func validateTransactGetItemsInput(c *Client, input *TransactGetItemsInput) error {
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

		table, err := c.getTable(tableName)
		if err != nil {
			return mapKnownError(err)
		}

		internalKeyMap := mapAttributeValueMapToTypes(get.Key)
		if vErr := types.ValidateItemMap(internalKeyMap); vErr != nil {
			return mapKnownError(types.NewError("ValidationException", vErr.Error(), nil))
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

// TransactGetItems atomically retrieves multiple items from one or more tables.
func (c *Client) TransactGetItems(ctx context.Context, input *TransactGetItemsInput) (*TransactGetItemsOutput, error) {
	if err := validateTransactGetItemsInput(c, input); err != nil {
		return nil, err
	}

	responses := make([]ItemResponse, 0, len(input.TransactItems))
	unitsByTable := map[string]float64{}

	for _, item := range input.TransactItems {
		get := item.Get

		out, err := c.GetItem(ctx, &GetItemInput{
			TableName:                get.TableName,
			Key:                      get.Key,
			ExpressionAttributeNames: get.ExpressionAttributeNames,
			ProjectionExpression:     get.ProjectionExpression,
		})
		if err != nil {
			return nil, err
		}

		responses = append(responses, ItemResponse{Item: out.Item})

		unitsByTable[aws.ToString(get.TableName)] += 2 * capacity.ReadUnits(capacity.Size(mapAttributeValueMapToTypes(out.Item)), true)
	}

	return &TransactGetItemsOutput{
		Responses:        responses,
		ConsumedCapacity: wireConsumedSlice(consumedMode(string(input.ReturnConsumedCapacity)), unitsByTable, true),
	}, nil
}

func (c *Client) runTransactItem(i, n int, item TransactWriteItem) error {
	switch {
	case item.Put != nil:
		return c.runTransactPut(i, n, item.Put)
	case item.Update != nil:
		return c.runTransactUpdate(i, n, item.Update)
	case item.Delete != nil:
		return c.runTransactDelete(i, n, item.Delete)
	case item.ConditionCheck != nil:
		return c.runTransactConditionCheck(i, n, item.ConditionCheck)
	default:
		return &smithy.GenericAPIError{Code: "ValidationException", Message: "transaction item must include one of Put, Update, Delete, or ConditionCheck"}
	}
}

func (c *Client) runTransactPut(i, n int, put *Put) error {
	if vErr := validateExpressionAttributes(put.ExpressionAttributeNames, keysFromAttributeValueMap(put.ExpressionAttributeValues), aws.ToString(put.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := c.getTable(aws.ToString(put.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	if _, opErr := table.Put(&types.PutItemInput{
		TableName:                 put.TableName,
		ConditionExpression:       put.ConditionExpression,
		ExpressionAttributeNames:  put.ExpressionAttributeNames,
		ExpressionAttributeValues: mapAttributeValueMapToTypes(put.ExpressionAttributeValues),
		Item:                      mapAttributeValueMapToTypes(put.Item),
	}); opErr != nil {
		return newServerTransactionCancelledError(i, n, opErr)
	}

	return nil
}

func (c *Client) runTransactUpdate(i, n int, update *Update) error {
	if vErr := validateExpressionAttributes(update.ExpressionAttributeNames, keysFromAttributeValueMap(update.ExpressionAttributeValues), aws.ToString(update.UpdateExpression), aws.ToString(update.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := c.getTable(aws.ToString(update.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	_, opErr := table.Update(&types.UpdateItemInput{
		TableName:                           update.TableName,
		ConditionExpression:                 update.ConditionExpression,
		ExpressionAttributeNames:            update.ExpressionAttributeNames,
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(update.ExpressionAttributeValues),
		Key:                                 mapAttributeValueMapToTypes(update.Key),
		UpdateExpression:                    aws.ToString(update.UpdateExpression),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(update.ReturnValuesOnConditionCheckFailure)),
	})
	if opErr != nil {
		if errors.Is(opErr, interpreter.ErrSyntaxError) {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: opErr.Error()}
		}

		return newServerTransactionCancelledError(i, n, opErr)
	}

	return nil
}

func (c *Client) runTransactDelete(i, n int, del *Delete) error {
	if vErr := validateExpressionAttributes(del.ExpressionAttributeNames, keysFromAttributeValueMap(del.ExpressionAttributeValues), aws.ToString(del.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := c.getTable(aws.ToString(del.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	if _, opErr := table.Delete(&types.DeleteItemInput{
		TableName:                 del.TableName,
		ConditionExpression:       del.ConditionExpression,
		ExpressionAttributeNames:  toStringPtrMap(del.ExpressionAttributeNames),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(del.ExpressionAttributeValues),
		Key:                       mapAttributeValueMapToTypes(del.Key),
	}); opErr != nil {
		return newServerTransactionCancelledError(i, n, opErr)
	}

	return nil
}

func (c *Client) runTransactConditionCheck(i, n int, check *ConditionCheck) error {
	if vErr := validateExpressionAttributes(check.ExpressionAttributeNames, keysFromAttributeValueMap(check.ExpressionAttributeValues), aws.ToString(check.ConditionExpression)); vErr != nil {
		return vErr
	}

	table, tErr := c.getTable(aws.ToString(check.TableName))
	if tErr != nil {
		return mapKnownError(tErr)
	}

	keyMap := mapAttributeValueMapToTypes(check.Key)
	if vErr := types.ValidateItemMap(keyMap); vErr != nil {
		return mapKnownError(types.NewError("ValidationException", vErr.Error(), nil))
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
		stored = map[string]*types.Item{}
	}

	matched, mErr := table.InterpreterMatch(interpreter.MatchInput{
		TableName:      table.Name,
		Expression:     aws.ToString(check.ConditionExpression),
		ExpressionType: interpreter.ExpressionTypeConditional,
		Item:           stored,
		Aliases:        check.ExpressionAttributeNames,
		Attributes:     mapAttributeValueMapToTypes(check.ExpressionAttributeValues),
	})
	if mErr != nil {
		return &smithy.GenericAPIError{Code: "ValidationException", Message: mErr.Error()}
	}

	if !matched {
		checkErr := &types.ConditionalCheckFailedException{
			MessageText: core.ErrConditionalRequestFailed.Error(),
		}

		if check.ReturnValuesOnConditionCheckFailure == ddbtypes.ReturnValuesOnConditionCheckFailureAllOld {
			checkErr.Item = stored
		}

		return newServerTransactionCancelledError(i, n, checkErr)
	}

	return nil
}

func newServerTransactionCancelledError(i, n int, opErr error) error {
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(opErr, &ccf) {
		return mapKnownError(opErr)
	}

	reasons := make([]ddbtypes.CancellationReason, n)
	for j := range reasons {
		reasons[j] = ddbtypes.CancellationReason{Code: aws.String("None")}
	}

	reasons[i] = ddbtypes.CancellationReason{
		Code:    aws.String("ConditionalCheckFailed"),
		Message: aws.String(core.ErrConditionalRequestFailed.Error()),
	}

	if ccf.Item != nil {
		reasons[i].Item = mapTypesMapToDDBAttributeValue(ccf.Item)
	}

	return &ddbtypes.TransactionCanceledException{
		Message:             aws.String("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]"),
		CancellationReasons: reasons,
	}
}

// Utilities
func mapTypesSliceToAttributeValue(items []map[string]*types.Item) []map[string]*AttributeValue {
	if len(items) == 0 {
		return nil
	}

	out := make([]map[string]*AttributeValue, len(items))
	for i, it := range items {
		out[i] = mapTypesMapToAttributeValue(it)
	}

	return out
}

func mapKnownError(err error) error {
	if err == nil {
		return nil
	}

	intErr, ok := errors.AsType[types.Error](err)
	if !ok {
		return err
	}

	switch intErr.Code() {
	case "ConditionalCheckFailedException":
		checkErr := &ddbtypes.ConditionalCheckFailedException{
			Message: aws.String(intErr.Message()),
		}

		if conditionalErr, ok := errors.AsType[*types.ConditionalCheckFailedException](err); ok {
			checkErr.Item = mapTypesMapToDDBAttributeValue(conditionalErr.Item)
		}

		return checkErr
	case "ResourceNotFoundException":
		return &ddbtypes.ResourceNotFoundException{Message: aws.String(intErr.Message())}
	default:
		return &smithy.GenericAPIError{Code: intErr.Code(), Message: intErr.Message()}
	}
}

func toStringPtrMap(in map[string]string) map[string]*string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]*string, len(in))

	for k, v := range in {
		out[k] = aws.String(v)
	}

	return out
}
