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
	if err := table.SetAttributeDefinition(mapAttributeDefinitions(input.AttributeDefinitions)); err != nil {
		return nil, mapKnownError(err)
	}

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

	if err := table.AddIndexes(mapGSI(input.GlobalSecondaryIndexes), mapLSI(input.LocalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	c.tables[tableName] = table

	return &CreateTableOutput{TableDescription: describeTableJSON(table.Description(tableName))}, nil
}

// UpdateTable applies metadata changes, including GSI updates.
func (c *Client) UpdateTable(ctx context.Context, input *UpdateTableInput) (*UpdateTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, ok := c.tables[tableName]
	if !ok {
		return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("Cannot do operations on a non-existent table")}
	}

	desc, err := table.UpdateTable(tableName, mapAttributeDefinitions(input.AttributeDefinitions), mapGSIUpdate(input.GlobalSecondaryIndexUpdates))

	return &UpdateTableOutput{TableDescription: describeTableJSON(desc)}, mapKnownError(err)
}

// DeleteTable removes a table and its data.
func (c *Client) DeleteTable(ctx context.Context, input *DeleteTableInput) (*DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := c.getTable(tableName)
	if err != nil {
		return nil, err
	}

	desc := describeTableJSON(table.Description(tableName))
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

	return &DescribeTableOutput{Table: describeTableJSON(table.Description(tableName))}, nil
}

// ListTables returns the table names in byte order, one page at a time.
func (c *Client) ListTables(ctx context.Context, input *ListTablesInput) (*ListTablesOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	names, last, err := core.ListTableNames(c.tables, aws.ToString(input.ExclusiveStartTableName), input.Limit)
	if err != nil {
		return nil, mapKnownError(err)
	}

	return &ListTablesOutput{TableNames: names, LastEvaluatedTableName: last}, nil
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := core.ValidateReturnValuesOnConditionCheckFailure(toStringPtr(string(input.ReturnValuesOnConditionCheckFailure))); err != nil {
		return nil, mapKnownError(err)
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

	var before map[string]*types.Item
	if mode != capacity.ModeNone {
		before = table.StoredItem(putItem)
	}

	item, err := table.Put(&types.PutItemInput{
		TableName:                           input.TableName,
		ConditionExpression:                 input.ConditionExpression,
		ConditionalOperator:                 toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:            input.ExpressionAttributeNames,
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Item:                                putItem,
		ReturnConsumedCapacity:              toStringPtr(string(input.ReturnConsumedCapacity)),
		ReturnItemCollectionMetrics:         toStringPtr(string(input.ReturnItemCollectionMetrics)),
		ReturnValues:                        toStringPtr(string(input.ReturnValues)),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(input.ReturnValuesOnConditionCheckFailure)),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	return &PutItemOutput{
		Attributes: mapTypesMapToAttributeValue(item),
		ConsumedCapacity: toWireConsumed(capacity.ForItemWrite(
			mode, table, aws.ToString(input.TableName), putItem, before, putItem,
		)),
	}, nil
}

// DeleteItem removes an item and optionally returns old values.
func (c *Client) DeleteItem(ctx context.Context, input *DeleteItemInput) (*DeleteItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	if err := core.ValidateReturnValuesOnConditionCheckFailure(toStringPtr(string(input.ReturnValuesOnConditionCheckFailure))); err != nil {
		return nil, mapKnownError(err)
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

	// Delete only returns the old item for ALL_OLD, so measure it before deleting.
	var stored map[string]*types.Item
	if mode != capacity.ModeNone {
		stored = table.StoredItem(deleteKey)
	}

	item, err := table.Delete(&types.DeleteItemInput{
		TableName:                           input.TableName,
		ConditionExpression:                 input.ConditionExpression,
		ConditionalOperator:                 toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:            toStringPtrMap(input.ExpressionAttributeNames),
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Key:                                 deleteKey,
		ReturnValues:                        toStringPtr(string(input.ReturnValues)),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(input.ReturnValuesOnConditionCheckFailure)),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	consumed := toWireConsumed(capacity.ForItemWrite(
		mode, table, aws.ToString(input.TableName), deleteKey, stored, nil,
	))

	if item == nil {
		return &DeleteItemOutput{ConsumedCapacity: consumed}, nil
	}

	return &DeleteItemOutput{Attributes: mapTypesMapToAttributeValue(item), ConsumedCapacity: consumed}, nil
}

// UpdateItem modifies attributes of an item using an update expression.
func (c *Client) UpdateItem(ctx context.Context, input *UpdateItemInput) (*UpdateItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := c.failureErrFor(aws.ToString(input.TableName), ""); ferr != nil {
		return nil, ferr
	}

	// DynamoDB reports ReturnValues, then the Key, ahead of the rest of the request. An unknown
	// table is reported where it always was, after the expressions.
	if table, tableErr := c.getTable(aws.ToString(input.TableName)); tableErr == nil {
		if err := core.ValidateUpdateItemReturnValues(toStringPtr(string(input.ReturnValues))); err != nil {
			return nil, mapKnownError(err)
		}

		if err := table.ValidatePrimaryKeyMap(mapAttributeValueMapToTypes(input.Key)); err != nil {
			return nil, mapKnownError(types.NewError("ValidationException", err.Error(), nil))
		}
	}

	if err := core.ValidateReturnValuesOnConditionCheckFailure(toStringPtr(string(input.ReturnValuesOnConditionCheckFailure))); err != nil {
		return nil, mapKnownError(err)
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

	var before map[string]*types.Item
	if mode != capacity.ModeNone {
		before = table.StoredItem(updateKeyMap)
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

	var after map[string]*types.Item
	if mode != capacity.ModeNone {
		after = table.StoredItem(updateKeyMap)
	}

	return &UpdateItemOutput{
		Attributes: mapTypesMapToAttributeValue(item),
		ConsumedCapacity: toWireConsumed(capacity.ForItemWrite(
			mode, table, aws.ToString(input.TableName), updateKeyMap, before, after,
		)),
	}, nil
}

// GetItem returns a single item by key.
func (c *Client) GetItem(ctx context.Context, input *GetItemInput) (*GetItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

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

	read, err := table.GetItem(keyMap, aws.ToString(input.ProjectionExpression), input.ExpressionAttributeNames)
	if err != nil {
		return nil, mapKnownError(err)
	}

	item := mapTypesMapToAttributeValue(read.Projected)

	out := &GetItemOutput{Item: item}

	if mode != capacity.ModeNone {
		sizeSource := read.Stored
		if len(sizeSource) == 0 {
			sizeSource = keyMap
		}

		out.ConsumedCapacity = toWireConsumed(capacity.ForRead(
			mode,
			aws.ToString(input.TableName), "", "",
			capacity.Size(sizeSource), aws.ToBool(input.ConsistentRead),
		))
	}

	return out, nil
}

// Query searches items by key condition and optional filter.
func (c *Client) Query(ctx context.Context, input *QueryInput) (*QueryOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := c.failureErrFor(aws.ToString(input.TableName), aws.ToString(input.IndexName)); ferr != nil {
		return nil, ferr
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	// Stopgap until #243 moves the unused-placeholder pre-check into core: DynamoDB Local
	// reports a missing table and an unknown index before unused names and values.
	if ierr := table.ValidateSearchIndex(aws.ToString(input.IndexName)); ierr != nil {
		return nil, mapKnownError(ierr)
	}

	if verr := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.KeyConditionExpression),
		aws.ToString(input.FilterExpression),
		aws.ToString(input.ProjectionExpression),
	); verr != nil {
		return nil, verr
	}

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	res, err := table.SearchData(core.QueryInput{
		Index:                     aws.ToString(input.IndexName),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapAttributeValueMapToTypes(input.ExclusiveStartKey),
		KeyConditionExpression:    aws.ToString(input.KeyConditionExpression),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ProjectionExpression:      aws.ToString(input.ProjectionExpression),
		Select:                    string(input.Select),
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ScanIndexForward:          aws.ToBool(input.ScanIndexForward),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	out := &QueryOutput{
		Count:            int32(len(res.Items)),
		ScannedCount:     int32(res.ScannedCount),
		LastEvaluatedKey: mapTypesMapToAttributeValue(res.LastEvaluatedKey),
	}

	if input.Select != ddbtypes.SelectCount {
		out.Items = mapTypesSliceToAttributeValue(res.Items)
	}

	if mode != capacity.ModeNone {
		out.ConsumedCapacity = toWireConsumed(capacity.ForRead(
			mode,
			aws.ToString(input.TableName), aws.ToString(input.IndexName), table.IndexKind(aws.ToString(input.IndexName)),
			capacity.SumSize(res.Items), aws.ToBool(input.ConsistentRead),
		))
	}

	return out, nil
}

// Scan iterates items (optionally filtered) and returns a page of results.
func (c *Client) Scan(ctx context.Context, input *ScanInput) (*ScanOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if ferr := c.failureErrFor(aws.ToString(input.TableName), aws.ToString(input.IndexName)); ferr != nil {
		return nil, ferr
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	// Stopgap until #243 moves the unused-placeholder pre-check into core: DynamoDB Local
	// reports a missing table and an unknown index before unused names and values.
	if ierr := table.ValidateSearchIndex(aws.ToString(input.IndexName)); ierr != nil {
		return nil, mapKnownError(ierr)
	}

	if verr := validateExpressionAttributes(
		input.ExpressionAttributeNames,
		keysFromAttributeValueMap(input.ExpressionAttributeValues),
		aws.ToString(input.ProjectionExpression),
		aws.ToString(input.FilterExpression),
	); verr != nil {
		return nil, verr
	}

	res, err := table.SearchData(core.QueryInput{
		Index:                     aws.ToString(input.IndexName),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapAttributeValueMapToTypes(input.ExclusiveStartKey),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ProjectionExpression:      aws.ToString(input.ProjectionExpression),
		Select:                    string(input.Select),
		Limit:                     int64(aws.ToInt32(input.Limit)),
		Scan:                      true,
		ScanIndexForward:          true,
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	out := &ScanOutput{
		Count:            int32(len(res.Items)),
		ScannedCount:     int32(res.ScannedCount),
		LastEvaluatedKey: mapTypesMapToAttributeValue(res.LastEvaluatedKey),
	}

	if input.Select != ddbtypes.SelectCount {
		out.Items = mapTypesSliceToAttributeValue(res.Items)
	}

	if mode != capacity.ModeNone {
		out.ConsumedCapacity = toWireConsumed(capacity.ForRead(
			mode,
			aws.ToString(input.TableName), aws.ToString(input.IndexName), table.IndexKind(aws.ToString(input.IndexName)),
			capacity.SumSize(res.Items), aws.ToBool(input.ConsistentRead),
		))
	}

	return out, nil
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	emulation := c.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	if err := c.validateBatchWriteRequests(input.RequestItems); err != nil {
		return nil, err
	}

	unprocessed := map[string][]WriteRequest{}
	acc := capacity.NewWriteAccumulator()

	for tableName, reqs := range input.RequestItems {
		for i, req := range reqs {
			if emulation.unprocessed(tableName, i, batchWriteRequestKey(req)) {
				unprocessed[tableName] = append(unprocessed[tableName], req)

				continue
			}

			if err := c.applyBatchWriteRequest(ctx, tableName, req, mode, acc); err != nil {
				return nil, err
			}
		}
	}

	return &BatchWriteItemOutput{
		UnprocessedItems: unprocessed,
		ConsumedCapacity: wireConsumedList(acc.Consumed(mode)),
	}, nil
}

// validateBatchWriteRequests rejects the whole batch when any request is invalid, so no request
// is applied before one fails (D8). The checks and their order live in core.ValidateBatchWrite.
func (c *Client) validateBatchWriteRequests(requestItems map[string][]WriteRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	requests := make(map[string][]core.WriteRequest, len(requestItems))

	for name, reqs := range requestItems {
		requests[name] = nil

		for _, req := range reqs {
			var r core.WriteRequest

			switch {
			case req.PutRequest != nil:
				r.Put = mapAttributeValueMapToTypes(req.PutRequest.Item)
			case req.DeleteRequest != nil:
				r.Delete = mapAttributeValueMapToTypes(req.DeleteRequest.Key)
			}

			requests[name] = append(requests[name], r)
		}
	}

	lookup := func(name string) (*core.Table, bool) {
		table, ok := c.tables[name]

		return table, ok
	}

	if err := core.ValidateBatchWrite(lookup, requests); err != nil {
		return mapKnownError(err)
	}

	return nil
}

// applyBatchWriteRequest runs one put or delete sub-request and, when consumed capacity is
// reported, bills it into acc. Sizes are measured before the request is applied: a delete bills
// the item it removes and a put the larger of the item it replaces and the item it writes.
func (c *Client) applyBatchWriteRequest(ctx context.Context, tableName string, req WriteRequest, mode capacity.Mode, acc *capacity.WriteAccumulator) error {
	var target capacity.WriteTarget

	if mode != capacity.ModeNone {
		target = batchWriteTarget(tableName, req)
		c.snapshotLocked(&target)
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
		return err
	}

	if mode != capacity.ModeNone {
		size, indexes := c.billLocked(&target)
		acc.Add(tableName, size, indexes, 1)
	}

	return nil
}

// batchWriteTarget describes a put or delete sub-request for billing.
func batchWriteTarget(tableName string, req WriteRequest) capacity.WriteTarget {
	if req.PutRequest != nil {
		return capacity.WriteTarget{Table: tableName, Kind: capacity.WritePut, Key: mapAttributeValueMapToTypes(req.PutRequest.Item)}
	}

	if req.DeleteRequest != nil {
		return capacity.WriteTarget{Table: tableName, Kind: capacity.WriteDelete, Key: mapAttributeValueMapToTypes(req.DeleteRequest.Key)}
	}

	return capacity.WriteTarget{Table: tableName, Kind: capacity.WriteCheck}
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	emulation := c.batchEmulationFor(tableNames(input.RequestItems)...)
	if emulation.failErr != nil {
		return nil, emulation.failErr
	}

	responses := map[string][]map[string]*AttributeValue{}
	unprocessed := map[string]KeysAndAttributes{}
	unitsByTable := map[string]float64{}

	for tableName, reqs := range input.RequestItems {
		tableResponses, unprocessedKeys, units, err := c.batchGetItemForTable(ctx, tableName, reqs, emulation, mode)
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
		ConsumedCapacity: wireConsumedSlice(mode, unitsByTable),
	}, nil
}

func (c *Client) batchGetItemForTable(ctx context.Context, tableName string, reqs KeysAndAttributes, emulation batchEmulation, mode capacity.Mode) ([]map[string]*AttributeValue, []map[string]*AttributeValue, float64, error) {
	read, err := c.batchGetTable(tableName, reqs)
	if err != nil {
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

		item := mapTypesMapToAttributeValue(read[i].Projected)

		if mode != capacity.ModeNone {
			units += capacity.ReadUnits(capacity.Size(read[i].Projected), consistent)
		}

		if len(item) > 0 {
			responses = append(responses, item)
		}
	}

	return responses, unprocessedKeys, units, nil
}

// batchGetTable reads every key of one table of a BatchGetItem under one lock, checking all the
// keys, then the projection once. Keys the unprocessed emulation skips are checked too.
func (c *Client) batchGetTable(tableName string, reqs KeysAndAttributes) ([]core.ReadItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := validateExpressionAttributes(reqs.ExpressionAttributeNames, nil, aws.ToString(reqs.ProjectionExpression)); err != nil {
		return nil, err
	}

	table, err := c.getTable(tableName)
	if err != nil {
		return nil, err
	}

	keys := make([]map[string]*types.Item, len(reqs.Keys))
	for i, key := range reqs.Keys {
		keys[i] = mapAttributeValueMapToTypes(key)
	}

	read, err := table.BatchGetItem(keys, aws.ToString(reqs.ProjectionExpression), reqs.ExpressionAttributeNames)
	if err != nil {
		return nil, mapKnownError(err)
	}

	return read, nil
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
	}

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
	}

	if err := validateTransactReturnValuesOnConditionCheckFailure(input.TransactItems); err != nil {
		return nil, err
	}

	snapshots, err := c.prepareTransact(input.TransactItems)
	if err != nil {
		return nil, err
	}

	// Sizes are measured before the writes: a delete bills the item it removes and an update
	// the larger of the item before and after.
	var targets []capacity.WriteTarget
	if mode != capacity.ModeNone {
		targets = transactWriteTargets(input.TransactItems)

		for i := range targets {
			targets[i].Snapshot(c.tableOrNil)
		}
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

	acc := capacity.NewWriteAccumulator()

	for i := range targets {
		size, indexes := targets[i].Bill(c.tableOrNil)
		acc.AddTransact(targets[i].Table, size, indexes)
	}

	return &TransactWriteItemsOutput{
		ConsumedCapacity: wireConsumedList(acc.Consumed(mode)),
	}, nil
}

// validateTransactReturnValuesOnConditionCheckFailure rejects the whole transaction, before any
// write, when an item has a ReturnValuesOnConditionCheckFailure outside ALL_OLD and NONE.
// DynamoDB Local accepts the value; minidyn follows the API reference (see e2e/README.md).
func validateTransactReturnValuesOnConditionCheckFailure(items []TransactWriteItem) error {
	for i, item := range items {
		var shape string

		var rv ddbtypes.ReturnValuesOnConditionCheckFailure

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

		if err := core.ValidateTransactItemReturnValuesOnConditionCheckFailure(i, shape, toStringPtr(string(rv))); err != nil {
			return mapKnownError(err)
		}
	}

	return nil
}

// transactWriteTargets describes every write of the transaction for billing. A ConditionCheck is
// billed by its key.
func transactWriteTargets(items []TransactWriteItem) []capacity.WriteTarget {
	targets := make([]capacity.WriteTarget, 0, len(items))

	for _, item := range items {
		switch {
		case item.Put != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.Put.TableName), Kind: capacity.WritePut, Key: mapAttributeValueMapToTypes(item.Put.Item),
			})
		case item.Update != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.Update.TableName), Kind: capacity.WriteUpdate, Key: mapAttributeValueMapToTypes(item.Update.Key),
			})
		case item.Delete != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.Delete.TableName), Kind: capacity.WriteDelete, Key: mapAttributeValueMapToTypes(item.Delete.Key),
			})
		case item.ConditionCheck != nil:
			targets = append(targets, capacity.WriteTarget{
				Table: aws.ToString(item.ConditionCheck.TableName), Kind: capacity.WriteCheck, Key: mapAttributeValueMapToTypes(item.ConditionCheck.Key),
			})
		}
	}

	return targets
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

		key, err := table.CheckedKeyOf(mapAttributeValueMapToTypes(get.Key))
		if err != nil {
			return mapKnownError(err)
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

	mode, modeErr := capacity.ParseMode(string(input.ReturnConsumedCapacity))
	if modeErr != nil {
		return nil, mapKnownError(modeErr)
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

		if mode != capacity.ModeNone {
			unitsByTable[aws.ToString(get.TableName)] += capacity.TransactReadUnits(capacity.Size(mapAttributeValueMapToTypes(out.Item)))
		}
	}

	return &TransactGetItemsOutput{
		Responses:        responses,
		ConsumedCapacity: wireConsumedSlice(mode, unitsByTable),
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
		TableName:                           put.TableName,
		ConditionExpression:                 put.ConditionExpression,
		ExpressionAttributeNames:            put.ExpressionAttributeNames,
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(put.ExpressionAttributeValues),
		Item:                                mapAttributeValueMapToTypes(put.Item),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(put.ReturnValuesOnConditionCheckFailure)),
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
		TableName:                           del.TableName,
		ConditionExpression:                 del.ConditionExpression,
		ExpressionAttributeNames:            toStringPtrMap(del.ExpressionAttributeNames),
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(del.ExpressionAttributeValues),
		Key:                                 mapAttributeValueMapToTypes(del.Key),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(del.ReturnValuesOnConditionCheckFailure)),
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

	read, kErr := table.GetItem(mapAttributeValueMapToTypes(check.Key), "", nil)
	if kErr != nil {
		return mapKnownError(kErr)
	}

	stored := read.Stored

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
		mode := string(check.ReturnValuesOnConditionCheckFailure)

		checkErr := core.ConditionalCheckFailed(&mode, stored)

		return newServerTransactionCancelledError(i, n, checkErr)
	}

	return nil
}

func newServerTransactionCancelledError(i, n int, opErr error) error {
	var ccf *types.ConditionalCheckFailedException
	if !errors.As(opErr, &ccf) {
		return mapKnownError(opErr)
	}

	codes := core.ConditionalCheckFailedCancellationCodes(i, n)

	reasons := make([]ddbtypes.CancellationReason, n)
	for j, code := range codes {
		reasons[j] = ddbtypes.CancellationReason{Code: aws.String(code)}
	}

	reasons[i].Message = aws.String(core.ConditionalCheckFailedReasonMessage)

	if ccf.Item != nil {
		reasons[i].Item = mapTypesMapToDDBAttributeValue(ccf.Item)
	}

	return &ddbtypes.TransactionCanceledException{
		Message:             aws.String(core.TransactionCanceledMessage(codes)),
		CancellationReasons: reasons,
	}
}

// Utilities
func mapTypesSliceToAttributeValue(items []map[string]*types.Item) []map[string]*AttributeValue {
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
