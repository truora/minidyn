package minidyn

import (
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/truora/minidyn/interpreter"
)

const primaryIndexName = ""

var (
	// ErrMissingKeys when missing table keys
	ErrMissingKeys = awserr.New("ValidationException", "The number of conditions on the keys is invalid", nil)
	// ErrInvalidTableName when the provided table name is invalid
	ErrInvalidTableName = errors.New("invalid table name")
	// ErrResourceNotFoundException when the requested resource is not found
	ErrResourceNotFoundException = errors.New("requested resource not found")
	// ErrConditionalRequestFailed when the conditional write is not meet
	ErrConditionalRequestFailed = errors.New("the conditional request failed")

	// ReturnUnprocessedItemsInBatch bool to control when the BatchWriteItemWithContext should return unprocessed items
	ReturnUnprocessedItemsInBatch = false
)

// Client define a mock struct to be used
type Client struct {
	dynamodbiface.DynamoDBAPI
	tables                map[string]*table
	mu                    sync.Mutex
	itemCollectionMetrics map[string][]*dynamodb.ItemCollectionMetrics
	langInterpreter       *interpreter.Language
	nativeInterpreter     *interpreter.Native
	forceFailureErr       error
}

// NewClient initializes dynamodb client with a mock
func NewClient() *Client {
	fake := Client{
		tables:            map[string]*table{},
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
		table.nativeInterpreter = native
	}
}

// GetNativeInterpreter returns native interpreter
func (fd *Client) GetNativeInterpreter() *interpreter.Native {
	return fd.nativeInterpreter
}

// CreateTable creates a new table
func (fd *Client) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	tableName := aws.StringValue(input.TableName)
	if _, ok := fd.tables[tableName]; ok {
		return nil, awserr.New(dynamodb.ErrCodeResourceInUseException, "Cannot create preexisting table", nil)
	}

	newTable := newTable(tableName)
	newTable.setAttributeDefinition(input.AttributeDefinitions)
	newTable.billingMode = input.BillingMode
	newTable.nativeInterpreter = fd.nativeInterpreter
	newTable.langInterpreter = fd.langInterpreter

	if err := newTable.createPrimaryIndex(input); err != nil {
		return nil, err
	}

	if err := newTable.addGlobalIndexes(input.GlobalSecondaryIndexes); err != nil {
		return nil, err
	}

	if err := newTable.addLocalIndexes(input.LocalSecondaryIndexes); err != nil {
		return nil, err
	}

	fd.tables[tableName] = newTable

	return &dynamodb.CreateTableOutput{
		TableDescription: newTable.description(tableName),
	}, nil
}

// CreateTableWithContext creates a new table
func (fd *Client) CreateTableWithContext(ctx aws.Context, input *dynamodb.CreateTableInput, opt ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return fd.CreateTable(input)
}

// DeleteTable deletes a table
func (fd *Client) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	tableName := aws.StringValue(input.TableName)

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, err
	}

	desc := table.description(tableName)

	delete(fd.tables, tableName)

	return &dynamodb.DeleteTableOutput{
		TableDescription: desc,
	}, nil
}

// DeleteTableWithContext deletes a table
func (fd *Client) DeleteTableWithContext(ctx aws.Context, input *dynamodb.DeleteTableInput, opt ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return fd.DeleteTable(input)
}

// UpdateTable update a table
func (fd *Client) UpdateTable(input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	tableName := aws.StringValue(input.TableName)

	table, ok := fd.tables[tableName]
	if !ok {
		return nil, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "Cannot do operations on a non-existent table", nil)
	}

	if input.AttributeDefinitions != nil {
		table.setAttributeDefinition(input.AttributeDefinitions)
	}

	for _, change := range input.GlobalSecondaryIndexUpdates {
		if err := table.applyIndexChange(change); err != nil {
			return &dynamodb.UpdateTableOutput{
				TableDescription: table.description(tableName),
			}, err
		}
	}

	return &dynamodb.UpdateTableOutput{
		TableDescription: table.description(tableName),
	}, nil
}

// UpdateTableWithContext update a table
func (fd *Client) UpdateTableWithContext(ctx aws.Context, input *dynamodb.UpdateTableInput, opts ...request.Option) (*dynamodb.UpdateTableOutput, error) {
	return fd.UpdateTable(input)
}

// DescribeTable returns information about the table
func (fd *Client) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.StringValue(input.TableName)

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, err
	}

	output := &dynamodb.DescribeTableOutput{
		Table: table.description(tableName),
	}

	return output, nil
}

// DescribeTableWithContext uses DescribeTableDescribeTable to return information about the table
func (fd *Client) DescribeTableWithContext(ctx aws.Context, input *dynamodb.DescribeTableInput, ops ...request.Option) (*dynamodb.DescribeTableOutput, error) {
	return fd.DescribeTable(input)
}

// PutItem mock response for dynamodb
func (fd *Client) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	item, err := table.put(input)

	return &dynamodb.PutItemOutput{
		Attributes: item,
	}, err
}

// PutItemWithContext mock response for dynamodb
func (fd *Client) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	return fd.PutItem(input)
}

// DeleteItem mock response for dynamodb
func (fd *Client) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		items, _ := table.searchData(queryInput{
			Index:                     primaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Aliases:                   input.ExpressionAttributeNames,
			Limit:                     aws.Int64(1),
			ConditionExpression:       input.ConditionExpression,
		})
		if len(items) == 0 {
			return &dynamodb.DeleteItemOutput{}, awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, ErrConditionalRequestFailed.Error(), nil)
		}
	}

	item, err := table.delete(input)
	if err != nil {
		return nil, err
	}

	if aws.StringValue(input.ReturnValues) == "ALL_OLD" {
		return &dynamodb.DeleteItemOutput{
			Attributes: item,
		}, nil
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

// DeleteItemWithContext mock response for dynamodb
func (fd *Client) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return fd.DeleteItem(input)
}

// UpdateItem mock response for dynamodb
func (fd *Client) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	item, err := table.update(input)
	if err != nil {
		return nil, err
	}

	output := &dynamodb.UpdateItemOutput{
		Attributes: item,
	}

	return output, nil
}

// UpdateItemWithContext mock response for dynamodb
func (fd *Client) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return fd.UpdateItem(input)
}

// GetItem mock response for dynamodb
func (fd *Client) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	key, ok := table.keySchema.getKey(table.attributesDef, input.Key)
	if !ok {
		return nil, ErrMissingKeys
	}

	item := copyItem(table.data[key])

	output := &dynamodb.GetItemOutput{
		Item: item,
	}

	return output, nil
}

// GetItemWithContext mock response for dynamodb
func (fd *Client) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opt ...request.Option) (*dynamodb.GetItemOutput, error) {
	return fd.GetItem(input)
}

// Query mock response for dynamodb
func (fd *Client) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	indexName := aws.StringValue(input.IndexName)

	items, lastKey := table.searchData(queryInput{
		Index:                     indexName,
		ExpressionAttributeValues: input.ExpressionAttributeValues,
		Aliases:                   input.ExpressionAttributeNames,
		Limit:                     input.Limit,
		ExclusiveStartKey:         input.ExclusiveStartKey,
		KeyConditionExpression:    input.KeyConditionExpression,
		FilterExpression:          input.FilterExpression,
	})

	count := int64(len(items))

	output := &dynamodb.QueryOutput{
		Items:            items,
		Count:            &count,
		LastEvaluatedKey: lastKey,
	}

	return output, nil
}

// QueryWithContext mock response for dynamodb
func (fd *Client) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opt ...request.Option) (*dynamodb.QueryOutput, error) {
	return fd.Query(input)
}

// Scan mock scan operation
func (fd *Client) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	indexName := aws.StringValue(input.IndexName)

	items, lastKey := table.searchData(queryInput{
		Index:                     indexName,
		ExpressionAttributeValues: input.ExpressionAttributeValues,
		Aliases:                   input.ExpressionAttributeNames,
		Limit:                     input.Limit,
		ExclusiveStartKey:         input.ExclusiveStartKey,
		FilterExpression:          input.FilterExpression,
		Scan:                      true,
	})

	count := int64(len(items))

	output := &dynamodb.ScanOutput{
		Items:            items,
		Count:            &count,
		LastEvaluatedKey: lastKey,
	}

	return output, nil
}

// ScanWithContext mock scan operation
func (fd *Client) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opt ...request.Option) (*dynamodb.ScanOutput, error) {
	return fd.Scan(input)
}

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func (fd *Client) setItemCollectionMetrics(itemCollectionMetrics map[string][]*dynamodb.ItemCollectionMetrics) {
	fd.itemCollectionMetrics = itemCollectionMetrics
}

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func SetItemCollectionMetrics(client dynamodbiface.DynamoDBAPI, itemCollectionMetrics map[string][]*dynamodb.ItemCollectionMetrics) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("SetItemCollectionMetrics: invalid client type")
	}

	fakeClient.setItemCollectionMetrics(itemCollectionMetrics)
}

// BatchWriteItemWithContext mock response for dynamodb
func (fd *Client) BatchWriteItemWithContext(ctx aws.Context, input *dynamodb.BatchWriteItemInput, opts ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}
	//TODO: Implement batch write

	if ReturnUnprocessedItemsInBatch {
		return &dynamodb.BatchWriteItemOutput{
			UnprocessedItems:      input.RequestItems,
			ItemCollectionMetrics: fd.itemCollectionMetrics}, nil
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems:      nil,
		ItemCollectionMetrics: fd.itemCollectionMetrics,
	}, nil
}

// TransactWriteItems mock response for dynamodb
func (fd *Client) TransactWriteItems(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	if fd.forceFailureErr != nil {
		return nil, ErrForcedFailure
	}

	//TODO: Implement transact write

	return &dynamodb.TransactWriteItemsOutput{}, nil
}

// TransactWriteItemsWithContext mock response for dynamodb
func (fd *Client) TransactWriteItemsWithContext(ctx aws.Context, input *dynamodb.TransactWriteItemsInput, opts ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return fd.TransactWriteItems(input)
}

func (fd *Client) getTable(tableName string) (*table, error) {
	table, ok := fd.tables[tableName]
	if !ok {
		return nil, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "Cannot do operations on a non-existent table", nil)
	}

	return table, nil
}
