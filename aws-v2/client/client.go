package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/interpreter"
	mtypes "github.com/truora/minidyn/types"
)

const (
	batchRequestsLimit                 = 25
	unusedExpressionAttributeNamesMsg  = "Value provided in ExpressionAttributeNames unused in expressions"
	unusedExpressionAttributeValuesMsg = "Value provided in ExpressionAttributeValues unused in expressions"
	invalidExpressionAttributeName     = "ExpressionAttributeNames contains invalid key"
	invalidExpressionAttributeValue    = "ExpressionAttributeValues contains invalid key"
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
}

// NewClient initializes dynamodb client with a mock
func NewClient() *Client {
	fake := Client{
		tables:            map[string]*core.Table{},
		mu:                sync.Mutex{},
		nativeInterpreter: interpreter.NewNativeInterpreter(),
		langInterpreter:   &interpreter.Language{},
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

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	item, err := table.Put(mapDynamoToTypesPutItemInput(input))

	return &dynamodb.PutItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
	}, mapKnownError(err)
}

// DeleteItem mock response for dynamodb
func (fd *Client) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
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

	item, err := table.Delete(mapDynamoToTypesDeleteItemInput(input))
	if err != nil {
		return nil, mapKnownError(err)
	}

	if string(input.ReturnValues) == "ALL_OLD" {
		return &dynamodb.DeleteItemOutput{
			Attributes: mapTypesToDynamoMapItem(item),
		}, nil
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

// UpdateItem mock response for dynamodb
func (fd *Client) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.UpdateExpression), aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	item, err := table.Update(mapDynamoToTypesUpdateItemInput(input))
	if err != nil {
		if errors.Is(err, interpreter.ErrSyntaxError) {
			return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
		}

		return nil, mapKnownError(err)
	}

	output := &dynamodb.UpdateItemOutput{}

	if item != nil {
		output.Attributes = mapTypesToDynamoMapItem(item)
	}

	return output, nil
}

// GetItem mock response for dynamodb
func (fd *Client) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opt ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
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

	return output, nil
}

// Query mock response for dynamodb
func (fd *Client) Query(ctx context.Context, input *dynamodb.QueryInput, opt ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
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

	return output, nil
}

// Scan mock scan operation
func (fd *Client) Scan(ctx context.Context, input *dynamodb.ScanInput, opt ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
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

// BatchWriteItem mock response for dynamodb
func (fd *Client) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	if err := validateBatchWriteItemInput(input); err != nil {
		return &dynamodb.BatchWriteItemOutput{}, err
	}

	unprocessed := map[string][]types.WriteRequest{}

	for table, reqs := range input.RequestItems {
		for _, req := range reqs {
			err := executeBatchWriteRequest(ctx, fd, aws.String(table), req)

			err = handleBatchWriteRequestError(table, req, unprocessed, err)
			if err != nil {
				return &dynamodb.BatchWriteItemOutput{}, err
			}
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems:      unprocessed,
		ItemCollectionMetrics: fd.itemCollectionMetrics,
	}, nil
}

// BatchGetItem mock response for dynamodb
func (fd *Client) BatchGetItem(ctx context.Context, input *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	responses := make(map[string][]map[string]types.AttributeValue, len(input.RequestItems))
	unprocessed := make(map[string]types.KeysAndAttributes, len(input.RequestItems))

	for tableName, reqs := range input.RequestItems {
		unprocessedKeys := make([]map[string]types.AttributeValue, 0, len(reqs.Keys))
		responses[tableName] = make([]map[string]types.AttributeValue, 0, len(reqs.Keys))

		for _, req := range reqs.Keys {
			getInput := &dynamodb.GetItemInput{
				TableName:                aws.String(tableName),
				Key:                      req,
				ConsistentRead:           reqs.ConsistentRead,
				AttributesToGet:          reqs.AttributesToGet,
				ExpressionAttributeNames: reqs.ExpressionAttributeNames,
				ProjectionExpression:     reqs.ProjectionExpression,
			}

			item, err := executeGetRequest(ctx, fd, getInput)
			if err != nil {
				unprocessedKeys = append(unprocessedKeys, req)

				continue
			}

			responses[tableName] = append(responses[tableName], item)
		}

		if len(unprocessedKeys) > 0 {
			unprocessed[tableName] = reqs

			tableUnprocessedKeys := unprocessed[tableName]
			tableUnprocessedKeys.Keys = unprocessedKeys

			unprocessed[tableName] = tableUnprocessedKeys
		}
	}

	return &dynamodb.BatchGetItemOutput{
		Responses:       responses,
		UnprocessedKeys: unprocessed,
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

func executeGetRequest(ctx context.Context, fd *Client, getInput *dynamodb.GetItemInput) (map[string]types.AttributeValue, error) {
	response, err := fd.GetItem(ctx, getInput)
	if err != nil {
		return nil, err
	}

	if len(response.Item) == 0 {
		return nil, ErrResourceNotFoundException
	}

	return response.Item, nil
}

func handleBatchWriteRequestError(table string, req types.WriteRequest, unprocessed map[string][]types.WriteRequest, err error) error {
	if err == nil {
		return nil
	}

	var oe smithy.APIError
	if !errors.As(err, &oe) {
		return err
	}

	var errInternalServer *types.InternalServerError
	var errProvisionedThroughputExceededException *types.ProvisionedThroughputExceededException

	if !errors.As(err, &errInternalServer) && !errors.As(err, &errProvisionedThroughputExceededException) {
		return err
	}

	if _, ok := unprocessed[table]; !ok {
		unprocessed[table] = []types.WriteRequest{}
	}

	unprocessed[table] = append(unprocessed[table], req)

	return nil
}

// TransactWriteItems mock response for dynamodb
func (fd *Client) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (_ *dynamodb.TransactWriteItemsOutput, err error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	snapshots := map[string]core.TableSnapshot{}

	defer func() {
		if err != nil {
			for name, snap := range snapshots {
				fd.tables[name].Restore(snap)
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

		table, tErr := fd.getTable(tableName)
		if tErr != nil {
			return nil, mapKnownError(tErr)
		}

		snapshots[tableName] = table.Snapshot()
	}

	n := len(input.TransactItems)

	for i, item := range input.TransactItems {
		switch {
		case item.Put != nil:
			if err = validateExpressionAttributes(item.Put.ExpressionAttributeNames, item.Put.ExpressionAttributeValues, aws.ToString(item.Put.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := fd.getTable(aws.ToString(item.Put.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			if _, opErr := table.Put(mapDynamoToTypesTransactPut(item.Put)); opErr != nil {
				return nil, newTransactionCancelledError(i, n, opErr)
			}

		case item.Update != nil:
			if err = validateExpressionAttributes(item.Update.ExpressionAttributeNames, item.Update.ExpressionAttributeValues, aws.ToString(item.Update.UpdateExpression), aws.ToString(item.Update.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := fd.getTable(aws.ToString(item.Update.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			_, opErr := table.Update(mapDynamoToTypesTransactUpdate(item.Update))
			if opErr != nil {
				if errors.Is(opErr, interpreter.ErrSyntaxError) {
					return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: opErr.Error()}
				}

				return nil, newTransactionCancelledError(i, n, opErr)
			}

		case item.Delete != nil:
			if err = validateExpressionAttributes(item.Delete.ExpressionAttributeNames, item.Delete.ExpressionAttributeValues, aws.ToString(item.Delete.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := fd.getTable(aws.ToString(item.Delete.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			if _, opErr := table.Delete(mapDynamoToTypesTransactDelete(item.Delete)); opErr != nil {
				return nil, newTransactionCancelledError(i, n, opErr)
			}

		case item.ConditionCheck != nil:
			if err = validateExpressionAttributes(item.ConditionCheck.ExpressionAttributeNames, item.ConditionCheck.ExpressionAttributeValues, aws.ToString(item.ConditionCheck.ConditionExpression)); err != nil {
				return nil, err
			}

			table, tErr := fd.getTable(aws.ToString(item.ConditionCheck.TableName))
			if tErr != nil {
				return nil, mapKnownError(tErr)
			}

			keyMap := mapDynamoToTypesMapItem(item.ConditionCheck.Key)
			if vErr := mtypes.ValidateItemMap(keyMap); vErr != nil {
				return nil, mapKnownError(mtypes.NewError("ValidationException", vErr.Error(), nil))
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
				stored = map[string]*mtypes.Item{}
			}

			matchInput := interpreter.MatchInput{
				TableName:      table.Name,
				Expression:     aws.ToString(item.ConditionCheck.ConditionExpression),
				ExpressionType: interpreter.ExpressionTypeConditional,
				Item:           stored,
				Aliases:        item.ConditionCheck.ExpressionAttributeNames,
				Attributes:     mapDynamoToTypesMapItem(item.ConditionCheck.ExpressionAttributeValues),
			}

			matched, mErr := table.InterpreterMatch(matchInput)
			if mErr != nil {
				return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: mErr.Error()}
			}

			if !matched {
				checkErr := &mtypes.ConditionalCheckFailedException{
					MessageText: core.ErrConditionalRequestFailed.Error(),
				}

				if item.ConditionCheck.ReturnValuesOnConditionCheckFailure == types.ReturnValuesOnConditionCheckFailureAllOld {
					checkErr.Item = stored
				}

				return nil, newTransactionCancelledError(i, n, checkErr)
			}

		default:
			return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: "transaction item must include one of Put, Update, Delete, or ConditionCheck"}
		}
	}

	return &dynamodb.TransactWriteItemsOutput{}, nil
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
		return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: keys: {%s}", unusedExpressionAttributeNamesMsg, strings.Join(missingNames, ", "))}
	}

	err := validateSyntaxExpression(expressionAttributeNamesRegex, flattenNames, invalidExpressionAttributeName)
	if err != nil {
		return err
	}

	if len(missingValues) > 0 {
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
