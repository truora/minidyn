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

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	key, err := table.KeySchema.GetKey(table.AttributesDef, mapAttributeValueMapToTypes(input.Key))
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
	}

	return &GetItemOutput{Item: mapTypesMapToAttributeValue(table.Data[key])}, nil
}

// Query searches items by key condition and optional filter.
func (c *Client) Query(ctx context.Context, input *QueryInput) (*QueryOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
	}

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	items, last := table.SearchData(core.QueryInput{
		Index:                     aws.ToString(input.IndexName),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapAttributeValueMapToTypes(input.ExclusiveStartKey),
		KeyConditionExpression:    aws.ToString(input.KeyConditionExpression),
		FilterExpression:          aws.ToString(input.FilterExpression),
		Limit:                     int64(aws.ToInt32(input.Limit)),
		ScanIndexForward:          aws.ToBool(input.ScanIndexForward),
	})

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

	table, err := c.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, err
	}

	items, last := table.SearchData(core.QueryInput{
		Index:                     aws.ToString(input.IndexName),
		ExpressionAttributeValues: mapAttributeValueMapToTypes(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapAttributeValueMapToTypes(input.ExclusiveStartKey),
		FilterExpression:          aws.ToString(input.FilterExpression),
		Limit:                     int64(aws.ToInt32(input.Limit)),
		Scan:                      true,
		ScanIndexForward:          true,
	})

	count := int32(len(items))

	return &ScanOutput{
		Items:            mapTypesSliceToAttributeValue(items),
		Count:            count,
		LastEvaluatedKey: mapTypesMapToAttributeValue(last),
	}, nil
}

// BatchWriteItem handles batch put/delete requests and returns unprocessed items on retriable errors.
//
//gocyclo:ignore
//gocognit:ignore
func (c *Client) BatchWriteItem(ctx context.Context, input *BatchWriteItemInput) (*BatchWriteItemOutput, error) {
	if c.forceFailureErr != nil {
		return nil, c.forceFailureErr
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
