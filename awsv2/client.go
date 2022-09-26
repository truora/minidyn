package awsv2

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/smithy-go"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/interpreter"
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
func (fd *Client) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if _, ok := fd.tables[tableName]; ok {
		return nil, &types.ResourceInUseException{Message: aws.String("Cannot create preexisting table")}
	}

	newTable := core.NewTable(tableName)
	newTable.SetAttributeDefinition(mapDynamoToTypesAttributeDefinitionSlice(input.AttributeDefinitions))
	newTable.BillingMode = string(input.BillingMode)
	newTable.NativeInterpreter = *fd.nativeInterpreter
	newTable.UseNativeInterpreter = fd.useNativeInterpreter
	newTable.LangInterpreter = *fd.langInterpreter

	if err := newTable.CreatePrimaryIndex(mapDynamoToTypesCreateTableInput(input)); err != nil {
		return nil, err
	}

	if err := newTable.AddGlobalIndexes(mapDynamoToTypesGlobalSecondaryIndexes(input.GlobalSecondaryIndexes)); err != nil {
		return nil, err
	}

	if err := newTable.AddLocalIndexes(mapDynamoToTypesLocalSecondaryIndexes(input.LocalSecondaryIndexes)); err != nil {
		return nil, err
	}

	fd.tables[tableName] = newTable

	return &dynamodb.CreateTableOutput{
		TableDescription: mapTypesToDynamoTableDescription(newTable.Description(tableName)),
	}, nil
}

// CreateTableWithContext creates a new table
func (fd *Client) CreateTableWithContext(ctx context.Context, input *dynamodb.CreateTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return fd.CreateTable(input)
}

// DeleteTable deletes a table
func (fd *Client) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, err
	}

	desc := mapTypesToDynamoTableDescription(table.Description(tableName))

	delete(fd.tables, tableName)

	return &dynamodb.DeleteTableOutput{
		TableDescription: desc,
	}, nil
}

// DeleteTableWithContext deletes a table
func (fd *Client) DeleteTableWithContext(ctx context.Context, input *dynamodb.DeleteTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	return fd.DeleteTable(input)
}

// UpdateTable update a table
func (fd *Client) UpdateTable(input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
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
			}, err
		}
	}

	return &dynamodb.UpdateTableOutput{
		TableDescription: mapTypesToDynamoTableDescription(table.Description(tableName)),
	}, nil
}

// UpdateTableWithContext update a table
func (fd *Client) UpdateTableWithContext(ctx context.Context, input *dynamodb.UpdateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	return fd.UpdateTable(input)
}

// DescribeTable returns information about the table
func (fd *Client) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, err
	}

	output := &dynamodb.DescribeTableOutput{
		Table: mapTypesToDynamoTableDescription(table.Description(tableName)),
	}

	return output, nil
}

// DescribeTableWithContext uses DescribeTableDescribeTable to return information about the table
func (fd *Client) DescribeTableWithContext(ctx context.Context, input *dynamodb.DescribeTableInput, ops ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return fd.DescribeTable(input)
}

// PutItem mock response for dynamodb
func (fd *Client) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	item, err := table.Put(mapDynamoToTypesPutItemInput(input))

	return &dynamodb.PutItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
	}, err
}

// PutItemWithContext mock response for dynamodb
func (fd *Client) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return fd.PutItem(input)
}

// DeleteItem mock response for dynamodb
func (fd *Client) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		items, _ := table.SearchData(core.QueryInput{
			Index:                     core.PrimaryIndexName,
			ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
			Aliases:                   input.ExpressionAttributeNames,
			Limit:                     aws.ToInt64(aws.Int64(1)),
			ConditionExpression:       input.ConditionExpression,
		})
		if len(items) == 0 {
			return &dynamodb.DeleteItemOutput{}, &types.ConditionalCheckFailedException{Message: aws.String(core.ErrConditionalRequestFailed.Error())}
		}
	}

	item, err := table.Delete(mapDynamoToTypesDeleteItemInput(input))
	if err != nil {
		return nil, err
	}

	if string(input.ReturnValues) == "ALL_OLD" {
		return &dynamodb.DeleteItemOutput{
			Attributes: mapTypesToDynamoMapItem(item),
		}, nil
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

// DeleteItemWithContext mock response for dynamodb
func (fd *Client) DeleteItemWithContext(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return fd.DeleteItem(input)
}

// UpdateItem mock response for dynamodb
func (fd *Client) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.UpdateExpression), aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	item, err := table.Update(mapDynamoToTypesUpdateItemInput(input))
	if err != nil {
		if errors.Is(err, interpreter.ErrSyntaxError) {
			return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
		}

		return nil, err
	}

	output := &dynamodb.UpdateItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
	}

	return output, nil
}

// UpdateItemWithContext mock response for dynamodb
func (fd *Client) UpdateItemWithContext(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return fd.UpdateItem(input)
}

// GetItem mock response for dynamodb
func (fd *Client) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, nil, aws.ToString(input.ProjectionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	key, err := table.KeySchema.GetKey(table.AttributesDef, mapDynamoToTypesMapItem(input.Key))
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
	}

	item := copyItem(table.Data[key])

	output := &dynamodb.GetItemOutput{
		Item: item,
	}

	return output, nil
}

// GetItemWithContext mock response for dynamodb
func (fd *Client) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, opt ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return fd.GetItem(input)
}

// Query mock response for dynamodb
func (fd *Client) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.KeyConditionExpression), aws.ToString(input.FilterExpression), aws.ToString(input.ProjectionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	indexName := aws.ToString(input.IndexName)

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	items, lastKey := table.SearchData(core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ExclusiveStartKey:         mapDynamoToTypesMapItem(input.ExclusiveStartKey),
		KeyConditionExpression:    aws.ToString(input.KeyConditionExpression),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ScanIndexForward:          aws.ToBool(input.ScanIndexForward),
	})

	count := int64(len(items))

	output := &dynamodb.QueryOutput{
		Items:            mapTypesToDynamoSliceMapItem(items),
		Count:            int32(count),
		LastEvaluatedKey: mapTypesToDynamoMapItem(lastKey),
	}

	return output, nil
}

// QueryWithContext mock response for dynamodb
func (fd *Client) QueryWithContext(ctx context.Context, input *dynamodb.QueryInput, opt ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return fd.Query(input)
}

// Scan mock scan operation
func (fd *Client) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ProjectionExpression), aws.ToString(input.FilterExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	indexName := aws.ToString(input.IndexName)

	items, lastKey := table.SearchData(core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ExclusiveStartKey:         mapDynamoToTypesMapItem(input.ExclusiveStartKey),
		FilterExpression:          aws.ToString(input.FilterExpression),
		ScanIndexForward:          true,
		Scan:                      true,
	})

	count := int64(len(items))

	output := &dynamodb.ScanOutput{
		Items:            mapTypesToDynamoSliceMapItem(items),
		Count:            int32(count),
		LastEvaluatedKey: mapTypesToDynamoMapItem(lastKey),
	}

	return output, nil
}

// ScanWithContext mock scan operation
func (fd *Client) ScanWithContext(ctx context.Context, input *dynamodb.ScanInput, opt ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return fd.Scan(input)
}

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func (fd *Client) setItemCollectionMetrics(itemCollectionMetrics map[string][]types.ItemCollectionMetrics) {
	fd.itemCollectionMetrics = itemCollectionMetrics
}

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func SetItemCollectionMetrics(client dynamodbiface.DynamoDBAPI, itemCollectionMetrics map[string][]types.ItemCollectionMetrics) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("SetItemCollectionMetrics: invalid client type")
	}

	fakeClient.setItemCollectionMetrics(itemCollectionMetrics)
}

// BatchWriteItemWithContext mock response for dynamodb
func (fd *Client) BatchWriteItemWithContext(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return fd.BatchWriteItem(input)
}

// BatchWriteItem mock response for dynamodb
func (fd *Client) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	if err := validateBatchWriteItemInput(input); err != nil {
		return &dynamodb.BatchWriteItemOutput{}, err
	}

	unprocessed := map[string][]types.WriteRequest{}

	for table, reqs := range input.RequestItems {
		for _, req := range reqs {
			err := executeBatchWriteRequest(fd, aws.String(table), req)

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

func executeBatchWriteRequest(fd *Client, table *string, req types.WriteRequest) error {
	if req.PutRequest != nil {
		_, err := fd.PutItem(&dynamodb.PutItemInput{
			Item:      req.PutRequest.Item,
			TableName: table,
		})

		return err
	}

	if req.DeleteRequest != nil {
		_, err := fd.DeleteItem(&dynamodb.DeleteItemInput{
			Key:       req.DeleteRequest.Key,
			TableName: table,
		})

		return err
	}

	return nil
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

	if !(errors.As(err, &errInternalServer) || errors.As(err, &errProvisionedThroughputExceededException)) {
		return err
	}

	if _, ok := unprocessed[table]; !ok {
		unprocessed[table] = []types.WriteRequest{}
	}

	unprocessed[table] = append(unprocessed[table], req)

	return nil
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
func (fd *Client) TransactWriteItemsWithContext(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return fd.TransactWriteItems(input)
}

func (fd *Client) getTable(tableName string) (*core.Table, error) {
	table, ok := fd.tables[tableName]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String("Cannot do operations on a non-existent table")}
	}

	return table, nil
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
