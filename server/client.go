package server

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
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
}

// NewClient creates a new in-memory DynamoDB-compatible client used by the HTTP server.
func NewClient() *Client {
	return &Client{
		tables:            map[string]*core.Table{},
		mu:                sync.Mutex{},
		nativeInterpreter: interpreter.NewNativeInterpreter(),
		langInterpreter:   &interpreter.Language{},
	}
}

func (c *Client) setFailureCondition(err error) {
	c.forceFailureErr = err
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

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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

	item, err := table.Put(&types.PutItemInput{
		TableName:                   input.TableName,
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:    input.ExpressionAttributeNames,
		ExpressionAttributeValues:   mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Item:                        mapAttributeValueMapToTypes(input.Item),
		ReturnConsumedCapacity:      toStringPtr(string(input.ReturnConsumedCapacity)),
		ReturnItemCollectionMetrics: toStringPtr(string(input.ReturnItemCollectionMetrics)),
		ReturnValues:                toStringPtr(string(input.ReturnValues)),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	return &PutItemOutput{Attributes: mapTypesMapToAttributeValue(item)}, nil
}

// DeleteItem removes an item and optionally returns old values.
func (c *Client) DeleteItem(ctx context.Context, input *DeleteItemInput) (*DeleteItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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

	item, err := table.Delete(&types.DeleteItemInput{
		TableName:                 input.TableName,
		ConditionExpression:       input.ConditionExpression,
		ConditionalOperator:       toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:  toStringPtrMap(input.ExpressionAttributeNames),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Key:                       mapAttributeValueMapToTypes(input.Key),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	if input.ReturnValues == ddbtypes.ReturnValueAllOld {
		return &DeleteItemOutput{Attributes: mapTypesMapToAttributeValue(item)}, nil
	}

	return &DeleteItemOutput{}, nil
}

// UpdateItem modifies attributes of an item using an update expression.
func (c *Client) UpdateItem(ctx context.Context, input *UpdateItemInput) (*UpdateItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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

	item, err := table.Update(&types.UpdateItemInput{
		TableName:                           input.TableName,
		ConditionExpression:                 input.ConditionExpression,
		ConditionalOperator:                 toStringPtr(string(input.ConditionalOperator)),
		ExpressionAttributeNames:            input.ExpressionAttributeNames,
		ExpressionAttributeValues:           mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Key:                                 mapAttributeValueMapToTypes(input.Key),
		UpdateExpression:                    aws.ToString(input.UpdateExpression),
		ReturnValues:                        toStringPtr(string(input.ReturnValues)),
		ReturnValuesOnConditionCheckFailure: toStringPtr(string(input.ReturnValuesOnConditionCheckFailure)),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	return &UpdateItemOutput{Attributes: mapTypesMapToAttributeValue(item)}, nil
}

// GetItem returns a single item by key.
func (c *Client) GetItem(ctx context.Context, input *GetItemInput) (*GetItemOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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

	return &GetItemOutput{Item: item}, nil
}

// Query searches items by key condition and optional filter.
func (c *Client) Query(ctx context.Context, input *QueryInput) (*QueryOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ScanIndexForward:          aws.ToBool(input.ScanIndexForward),
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	count := int32(len(items))

	return &QueryOutput{
		Items:            mapTypesSliceToAttributeValue(items),
		Count:            count,
		LastEvaluatedKey: mapTypesMapToAttributeValue(last),
	}, nil
}

// Scan iterates items (optionally filtered) and returns a page of results.
func (c *Client) Scan(ctx context.Context, input *ScanInput) (*ScanOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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
		Limit:                     int64(aws.ToInt32(input.Limit)),
		Scan:                      true,
		ScanIndexForward:          true,
	})
	if err != nil {
		return nil, mapKnownError(err)
	}

	count := int32(len(items))

	return &ScanOutput{
		Items:            mapTypesSliceToAttributeValue(items),
		Count:            count,
		LastEvaluatedKey: mapTypesMapToAttributeValue(last),
	}, nil
}

const batchWriteItemRequestsLimit = 25

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

// BatchWriteItem runs put and delete sub-requests in order. Sub-request failures that look
// retriable (InternalServerError, ProvisionedThroughputExceededException) are appended to
// UnprocessedItems and processing continues, matching DynamoDB batch semantics. Any other
// error fails the whole batch.
//
// With EmulateFailure(FailureConditionInternalServerError), each sub-request fails that way,
// so those writes appear as unprocessed until you call EmulateFailure(FailureConditionNone).
//
//gocyclo:ignore
//gocognit:ignore
func (c *Client) BatchWriteItem(ctx context.Context, input *BatchWriteItemInput) (*BatchWriteItemOutput, error) {
	if err := validateBatchWriteItemInput(input); err != nil {
		return nil, err
	}

	unprocessed := map[string][]WriteRequest{}

	for tableName, reqs := range input.RequestItems {
		for _, req := range reqs {
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
				var oe smithy.APIError
				if errors.As(err, &oe) {
					var is500 *ddbtypes.InternalServerError
					var isThrottled *ddbtypes.ProvisionedThroughputExceededException

					if errors.As(err, &is500) || errors.As(err, &isThrottled) {
						unprocessed[tableName] = append(unprocessed[tableName], req)
						continue
					}
				}

				return nil, err
			}
		}
	}

	return &BatchWriteItemOutput{UnprocessedItems: unprocessed}, nil
}

// TransactWriteItems executes a set of Put, Update, Delete, and ConditionCheck operations atomically.
// If any operation fails the entire transaction is  rolled back via table snapshots captured before execution begins.
func (c *Client) TransactWriteItems(ctx context.Context, input *TransactWriteItemsInput) (_ *TransactWriteItemsOutput, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
	}

	snapshots := map[string]core.TableSnapshot{}

	defer func() {
		if err != nil {
			for name, snap := range snapshots {
				c.tables[name].Restore(snap)
			}
		}
	}()

	for _, item := range input.TransactItems {
		var tableName string

		switch {
		case item.Put != nil:
			tableName = aws.ToString(item.Put.TableName)
		case item.Update != nil:
			tableName = aws.ToString(item.Update.TableName)
		case item.Delete != nil:
			tableName = aws.ToString(item.Delete.TableName)
		case item.ConditionCheck != nil:
			tableName = aws.ToString(item.ConditionCheck.TableName)
		}

		if _, alreadySnapped := snapshots[tableName]; tableName == "" || alreadySnapped {
			continue
		}

		table, tErr := c.getTable(tableName)
		if tErr != nil {
			return nil, mapKnownError(tErr)
		}

		snapshots[tableName] = table.Snapshot()
	}

	n := len(input.TransactItems)

	for i, item := range input.TransactItems {
		switch {
		case item.Put != nil:
			if err = validateExpressionAttributes(item.Put.ExpressionAttributeNames, keysFromAttributeValueMap(item.Put.ExpressionAttributeValues), aws.ToString(item.Put.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := c.getTable(aws.ToString(item.Put.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			if _, opErr := table.Put(&types.PutItemInput{
				TableName:                 item.Put.TableName,
				ConditionExpression:       item.Put.ConditionExpression,
				ExpressionAttributeNames:  item.Put.ExpressionAttributeNames,
				ExpressionAttributeValues: mapAttributeValueMapToTypes(item.Put.ExpressionAttributeValues),
				Item:                      mapAttributeValueMapToTypes(item.Put.Item),
			}); opErr != nil {
				return nil, newServerTransactionCancelledError(i, n, opErr)
			}

		case item.Update != nil:
			if err = validateExpressionAttributes(item.Update.ExpressionAttributeNames, keysFromAttributeValueMap(item.Update.ExpressionAttributeValues), aws.ToString(item.Update.UpdateExpression), aws.ToString(item.Update.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := c.getTable(aws.ToString(item.Update.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			_, opErr := table.Update(&types.UpdateItemInput{
				TableName:                           item.Update.TableName,
				ConditionExpression:                 item.Update.ConditionExpression,
				ExpressionAttributeNames:            item.Update.ExpressionAttributeNames,
				ExpressionAttributeValues:           mapAttributeValueMapToTypes(item.Update.ExpressionAttributeValues),
				Key:                                 mapAttributeValueMapToTypes(item.Update.Key),
				UpdateExpression:                    aws.ToString(item.Update.UpdateExpression),
				ReturnValuesOnConditionCheckFailure: toStringPtr(string(item.Update.ReturnValuesOnConditionCheckFailure)),
			})
			if opErr != nil {
				if errors.Is(opErr, interpreter.ErrSyntaxError) {
					return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: opErr.Error()}
				}

				return nil, newServerTransactionCancelledError(i, n, opErr)
			}

		case item.Delete != nil:
			if err = validateExpressionAttributes(item.Delete.ExpressionAttributeNames, keysFromAttributeValueMap(item.Delete.ExpressionAttributeValues), aws.ToString(item.Delete.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := c.getTable(aws.ToString(item.Delete.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			if _, opErr := table.Delete(&types.DeleteItemInput{
				TableName:                 item.Delete.TableName,
				ConditionExpression:       item.Delete.ConditionExpression,
				ExpressionAttributeNames:  toStringPtrMap(item.Delete.ExpressionAttributeNames),
				ExpressionAttributeValues: mapAttributeValueMapToTypes(item.Delete.ExpressionAttributeValues),
				Key:                       mapAttributeValueMapToTypes(item.Delete.Key),
			}); opErr != nil {
				return nil, newServerTransactionCancelledError(i, n, opErr)
			}

		case item.ConditionCheck != nil:
			if err = validateExpressionAttributes(item.ConditionCheck.ExpressionAttributeNames, keysFromAttributeValueMap(item.ConditionCheck.ExpressionAttributeValues), aws.ToString(item.ConditionCheck.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := c.getTable(aws.ToString(item.ConditionCheck.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			keyMap := mapAttributeValueMapToTypes(item.ConditionCheck.Key)
			if vErr := types.ValidateItemMap(keyMap); vErr != nil {
				return nil, mapKnownError(types.NewError("ValidationException", vErr.Error(), nil))
			}

			if keyErr := table.ValidatePrimaryKeyMap(keyMap); keyErr != nil {
				return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: keyErr.Error()}
			}

			key, kErr := table.KeySchema.GetKey(table.AttributesDef, keyMap)
			if kErr != nil {
				return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: kErr.Error()}
			}

			stored := table.Data[key]
			if stored == nil {
				stored = map[string]*types.Item{}
			}

			matched, mErr := table.InterpreterMatch(interpreter.MatchInput{
				TableName:      table.Name,
				Expression:     aws.ToString(item.ConditionCheck.ConditionExpression),
				ExpressionType: interpreter.ExpressionTypeConditional,
				Item:           stored,
				Aliases:        item.ConditionCheck.ExpressionAttributeNames,
				Attributes:     mapAttributeValueMapToTypes(item.ConditionCheck.ExpressionAttributeValues),
			})
			if mErr != nil {
				return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: mErr.Error()}
			}

			if !matched {
				checkErr := &types.ConditionalCheckFailedException{
					MessageText: core.ErrConditionalRequestFailed.Error(),
				}

				if item.ConditionCheck.ReturnValuesOnConditionCheckFailure == ddbtypes.ReturnValuesOnConditionCheckFailureAllOld {
					checkErr.Item = stored
				}

				return nil, newServerTransactionCancelledError(i, n, checkErr)
			}

		default:
			return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: "transaction item must include one of Put, Update, Delete, or ConditionCheck"}
		}
	}

	return &TransactWriteItemsOutput{}, nil
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

	var intErr types.Error
	if !errors.As(err, &intErr) {
		return err
	}

	switch intErr.Code() {
	case "ConditionalCheckFailedException":
		checkErr := &ddbtypes.ConditionalCheckFailedException{
			Message: aws.String(intErr.Message()),
		}

		var conditionalErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalErr) {
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
