package client

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/interpreter"
	minidynTypes "github.com/truora/minidyn/types"
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
	dynamodbiface.DynamoDBAPI
	tables                map[string]*core.Table
	mu                    sync.Mutex
	itemCollectionMetrics map[string][]*dynamodb.ItemCollectionMetrics
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
	if err := input.Validate(); err != nil {
		return nil, err
	}

	tableName := aws.StringValue(input.TableName)
	if _, ok := fd.tables[tableName]; ok {
		return nil, awserr.New(dynamodb.ErrCodeResourceInUseException, "Cannot create preexisting table", nil)
	}

	newTable := core.NewTable(tableName)
	newTable.SetAttributeDefinition(mapAttributeDefinitionToTypes(input.AttributeDefinitions))
	newTable.BillingMode = input.BillingMode
	newTable.NativeInterpreter = *fd.nativeInterpreter
	newTable.UseNativeInterpreter = fd.useNativeInterpreter
	newTable.LangInterpreter = *fd.langInterpreter

	if err := newTable.CreatePrimaryIndex(mapCreateTableInputToTypes(input)); err != nil {
		return nil, err
	}

	if err := newTable.AddGlobalIndexes(mapGlobalSecondaryIndexesToTypes(input.GlobalSecondaryIndexes)); err != nil {
		return nil, err
	}

	if err := newTable.AddLocalIndexes(mapLocalSecondaryIndexesToTypes(input.LocalSecondaryIndexes)); err != nil {
		return nil, err
	}

	fd.tables[tableName] = newTable

	return &dynamodb.CreateTableOutput{
		TableDescription: mapTableDescriptionToDynamodb(newTable.Description(tableName)),
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

	desc := table.Description(tableName)

	delete(fd.tables, tableName)

	return &dynamodb.DeleteTableOutput{
		TableDescription: mapTableDescriptionToDynamodb(desc),
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
		table.SetAttributeDefinition(mapAttributeValueDefinitionToDynamodb(input.AttributeDefinitions))
	}

	for _, change := range input.GlobalSecondaryIndexUpdates {
		if err := table.ApplyIndexChange(mapGlobalSecondaryIndexUpdateToTypes(change)); err != nil {
			return &dynamodb.UpdateTableOutput{
				TableDescription: mapTableDescriptionToDynamodb(table.Description(tableName)),
			}, err
		}
	}

	return &dynamodb.UpdateTableOutput{
		TableDescription: mapTableDescriptionToDynamodb(table.Description(tableName)),
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
		Table: mapTableDescriptionToDynamodb(table.Description(tableName)),
	}

	return output, nil
}

// DescribeTableWithContext uses DescribeTableDescribeTable to return information about the table
func (fd *Client) DescribeTableWithContext(ctx aws.Context, input *dynamodb.DescribeTableInput, ops ...request.Option) (*dynamodb.DescribeTableOutput, error) {
	return fd.DescribeTable(input)
}

// PutItem mock response for dynamodb
func (fd *Client) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err = validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.StringValue(input.ConditionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	item, err := table.Put(mapPutItemInputToTypes(input))

	return &dynamodb.PutItemOutput{
		Attributes: mapAttributeValueToDynamodb(item),
	}, err
}

// PutItemWithContext mock response for dynamodb
func (fd *Client) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	return fd.PutItem(input)
}

// DeleteItem mock response for dynamodb
func (fd *Client) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err = validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.StringValue(input.ConditionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		items, _ := table.SearchData(core.QueryInput{
			Index:                     core.PrimaryIndexName,
			ExpressionAttributeValues: mapAttributeValueToTypes(input.ExpressionAttributeValues),
			Aliases:                   aws.StringValueMap(input.ExpressionAttributeNames),
			Limit:                     1,
			ConditionExpression:       input.ConditionExpression,
		})
		if len(items) == 0 {
			return &dynamodb.DeleteItemOutput{}, awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, core.ErrConditionalRequestFailed.Error(), nil)
		}
	}

	item, err := table.Delete(mapDeleteItemInputToTypes(input))
	if err != nil {
		return nil, err
	}

	if aws.StringValue(input.ReturnValues) == "ALL_OLD" {
		return &dynamodb.DeleteItemOutput{
			Attributes: mapAttributeValueToDynamodb(item),
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
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err = validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.StringValue(input.UpdateExpression), aws.StringValue(input.ConditionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	item, err := table.Update(mapUpdateItemInputToTypes(input))
	if err != nil {
		if errors.Is(err, interpreter.ErrSyntaxError) {
			return nil, awserr.New("ValidationException", err.Error(), nil)
		}

		return nil, err
	}

	output := &dynamodb.UpdateItemOutput{
		Attributes: mapAttributeValueToDynamodb(item),
	}

	return output, nil
}

// UpdateItemWithContext mock response for dynamodb
func (fd *Client) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return fd.UpdateItem(input)
}

// GetItem mock response for dynamodb
func (fd *Client) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.forceFailureErr != nil {
		return nil, fd.forceFailureErr
	}

	err = validateExpressionAttributes(input.ExpressionAttributeNames, nil, aws.StringValue(input.ProjectionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	key, err := table.KeySchema.GetKey(table.AttributesDef, mapAttributeValueToTypes(input.Key))
	if err != nil {
		return nil, awserr.New("ValidationException", err.Error(), nil)
	}

	item := copyItem(mapAttributeValueToDynamodb(table.Data[key]))

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

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.StringValue(input.KeyConditionExpression), aws.StringValue(input.FilterExpression), aws.StringValue(input.ProjectionExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	indexName := aws.StringValue(input.IndexName)

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	items, lastKey := table.SearchData(core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapAttributeValueToTypes(input.ExpressionAttributeValues),
		Aliases:                   aws.StringValueMap(input.ExpressionAttributeNames),
		Limit:                     aws.Int64Value(input.Limit),
		ExclusiveStartKey:         mapAttributeValueToTypes(input.ExclusiveStartKey),
		KeyConditionExpression:    *input.KeyConditionExpression,
		FilterExpression:          aws.StringValue(input.FilterExpression),
		ScanIndexForward:          aws.BoolValue(input.ScanIndexForward),
	})

	count := int64(len(items))

	output := &dynamodb.QueryOutput{
		Items:            mapItemSliceToDynamodb(items),
		Count:            &count,
		LastEvaluatedKey: mapAttributeValueToDynamodb(lastKey),
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

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.StringValue(input.ProjectionExpression), aws.StringValue(input.FilterExpression))
	if err != nil {
		return nil, err
	}

	table, err := fd.getTable(aws.StringValue(input.TableName))
	if err != nil {
		return nil, err
	}

	indexName := aws.StringValue(input.IndexName)

	items, lastKey := table.SearchData(core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapAttributeValueToTypes(input.ExpressionAttributeValues),
		Aliases:                   aws.StringValueMap(input.ExpressionAttributeNames),
		Limit:                     aws.Int64Value(input.Limit),
		ExclusiveStartKey:         mapAttributeValueToTypes(input.ExclusiveStartKey),
		FilterExpression:          aws.StringValue(input.FilterExpression),
		Scan:                      true,
		ScanIndexForward:          true,
	})

	count := int64(len(items))

	output := &dynamodb.ScanOutput{
		Items:            mapItemSliceToDynamodb(items),
		Count:            &count,
		LastEvaluatedKey: mapAttributeValueToDynamodb(lastKey),
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
	return fd.BatchWriteItem(input)
}

// BatchWriteItem mock response for dynamodb
func (fd *Client) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	if err := validateBatchWriteItemInput(input); err != nil {
		return &dynamodb.BatchWriteItemOutput{}, err
	}

	unprocessed := map[string][]*dynamodb.WriteRequest{}

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

func validateWriteRequest(req *dynamodb.WriteRequest) error {
	if req.DeleteRequest != nil && req.PutRequest != nil {
		return awserr.New("ValidationException", "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes", nil)
	}

	if req.DeleteRequest == nil && req.PutRequest == nil {
		return awserr.New("ValidationException", "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes", nil)
	}

	return nil
}

func validateBatchWriteItemInput(input *dynamodb.BatchWriteItemInput) error {
	err := input.Validate()
	if err != nil {
		return err
	}

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
		return awserr.New("ValidationException", "Too many items requested for the BatchWriteItem call", nil)
	}

	return nil
}

func executeBatchWriteRequest(fd *Client, table *string, req *dynamodb.WriteRequest) error {
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

func handleBatchWriteRequestError(table string, req *dynamodb.WriteRequest, unprocessed map[string][]*dynamodb.WriteRequest, err error) error {
	if err == nil {
		return nil
	}

	var aerr awserr.Error
	if ok := errors.As(err, &aerr); !ok {
		return err
	}

	if !(aerr.Code() == dynamodb.ErrCodeInternalServerError || aerr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException) {
		return err
	}

	if _, ok := unprocessed[table]; !ok {
		unprocessed[table] = []*dynamodb.WriteRequest{}
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
func (fd *Client) TransactWriteItemsWithContext(ctx aws.Context, input *dynamodb.TransactWriteItemsInput, opts ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return fd.TransactWriteItems(input)
}

func (fd *Client) getTable(tableName string) (*core.Table, error) {
	table, ok := fd.tables[tableName]
	if !ok {
		return nil, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "Cannot do operations on a non-existent table", nil)
	}

	return table, nil
}

func validateExpressionAttributes(exprNames map[string]*string, exprValues map[string]*dynamodb.AttributeValue, genericExpressions ...string) error {
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
		return awserr.New("ValidationException", fmt.Sprintf("%s: keys: {%s}", unusedExpressionAttributeNamesMsg, strings.Join(missingNames, ", ")), nil)
	}

	err := validateSyntaxExpression(expressionAttributeNamesRegex, flattenNames, invalidExpressionAttributeName)
	if err != nil {
		return err
	}

	if len(missingValues) > 0 {
		return awserr.New("ValidationException", fmt.Sprintf("%s: keys: {%s}", unusedExpressionAttributeValuesMsg, strings.Join(missingValues, ", ")), nil)
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
			return awserr.New("ValidationException", fmt.Sprintf("%s: Syntax error; key: %s", errorMsg, exprName), nil)
		}
	}

	return nil
}

func getKeysFromExpressionNames(m map[string]*string) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func getKeysFromExpressionValues(m map[string]*dynamodb.AttributeValue) []string {
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

// StreamRecord represents a stream record for AWS SDK v1
type StreamRecord struct {
	EventID        string
	EventName      string
	Keys           map[string]*dynamodb.AttributeValue
	NewImage       map[string]*dynamodb.AttributeValue
	OldImage       map[string]*dynamodb.AttributeValue
	SequenceNumber string
	StreamViewType string
}

func mapStringToStreamViewType(viewType string) minidynTypes.StreamViewType {
	switch viewType {
	case "KEYS_ONLY":
		return minidynTypes.StreamViewTypeKeysOnly
	case "NEW_IMAGE":
		return minidynTypes.StreamViewTypeNewImage
	case "OLD_IMAGE":
		return minidynTypes.StreamViewTypeOldImage
	case "NEW_AND_OLD_IMAGES":
		return minidynTypes.StreamViewTypeNewAndOldImages
	default:
		return minidynTypes.StreamViewTypeKeysOnly
	}
}

// EnableStreams enables DynamoDB Streams on a table
func (fd *Client) EnableStreams(tableName string, viewType string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	table, err := fd.getTable(tableName)
	if err != nil {
		return err
	}

	vt := mapStringToStreamViewType(viewType)
	table.EnableStream(vt)

	return nil
}

// DisableStreams disables DynamoDB Streams on a table
func (fd *Client) DisableStreams(tableName string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	table, err := fd.getTable(tableName)
	if err != nil {
		return err
	}

	table.DisableStream()

	return nil
}

// GetStreamRecords returns all stream records for a table
func (fd *Client) GetStreamRecords(tableName string) ([]StreamRecord, error) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	table, err := fd.getTable(tableName)
	if err != nil {
		return nil, err
	}

	records := table.GetStreamRecords()
	result := make([]StreamRecord, len(records))

	for i, record := range records {
		result[i] = StreamRecord{
			EventID:        record.EventID,
			EventName:      string(record.EventName),
			Keys:           mapAttributeValueToDynamodb(record.Keys),
			NewImage:       mapAttributeValueToDynamodb(record.NewImage),
			OldImage:       mapAttributeValueToDynamodb(record.OldImage),
			SequenceNumber: record.SequenceNumber,
			StreamViewType: string(record.StreamViewType),
		}
	}

	return result, nil
}

// ClearStreamRecords clears all stream records for a table
func (fd *Client) ClearStreamRecords(tableName string) error {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	table, err := fd.getTable(tableName)
	if err != nil {
		return err
	}

	table.ClearStreamRecords()

	return nil
}
