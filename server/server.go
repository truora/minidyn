package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

// Server define a mock struct to be used
type Server struct {
	tables                map[string]*core.Table
	mu                    sync.Mutex
	itemCollectionMetrics map[string][]types.ItemCollectionMetrics
	langInterpreter       *interpreter.Language
	nativeInterpreter     *interpreter.Native
	useNativeInterpreter  bool
	forceFailureErr       error
	testServer            *httptest.Server
	URL                   string
}

// ErrorResponse json returned when dynamodb fails
type ErrorResponse struct {
	Typ     string `json:"__type"`
	Message string `json:"message"`
	ErrType string `json:"type"`
}

// NewServer initializes dynamodb client with a mock
func NewServer() *Server {
	srv := Server{
		tables:            map[string]*core.Table{},
		mu:                sync.Mutex{},
		nativeInterpreter: interpreter.NewNativeInterpreter(),
		langInterpreter:   &interpreter.Language{},
	}

	return &srv
}

// ConnectTestServer creates a server with an associated httptest.Server
func ConnectTestServer() *Server {
	srv := NewServer()
	srv.testServer = httptest.NewServer(srv)

	srv.URL = srv.testServer.URL

	return srv
}

// DisconnectTestServer disconnect the associated httptest.Server
func (srv *Server) DisconnectTestServer() {
	if srv.testServer == nil {
		return
	}

	srv.testServer.Close()
}

// EmulateFailure forces the fake client to fail
func (srv *Server) EmulateFailure(condition FailureCondition) {
	srv.setFailureCondition(condition)
}

// ActiveForceFailure active force operation to fail
func (srv *Server) ActiveForceFailure() {
	srv.setFailureCondition(FailureConditionDeprecated)
}

// DeactiveForceFailure deactive force operation to fail
func (srv *Server) DeactiveForceFailure() {
	srv.setFailureCondition(FailureConditionNone)
}

func responseErr(rw http.ResponseWriter, err error, status int) {
	rw.WriteHeader(status)

	er := ErrorResponse{
		Typ:     "minidyn",
		Message: err.Error(),
		ErrType: "server",
	}

	d, _ := json.Marshal(er)

	_, _ = rw.Write(d)
}

// Handler returns the http server handler
func (srv *Server) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	var bodyBytes []byte

	if srv.forceFailureErr != nil {
		responseErr(rw, srv.forceFailureErr, http.StatusInternalServerError)

		return
	}

	target := req.Header.Get("X-Amz-Target")
	_, op, _ := strings.Cut(target, ".")

	if req.Body != nil {
		var err error

		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			responseErr(rw, err, http.StatusBadRequest)
			return
		}

		defer func() {
			_ = req.Body.Close()
		}()
	}

	var output any

	switch op {
	case "CreateTable":
		input := dynamodb.CreateTableInput{}

		err := json.Unmarshal(bodyBytes, &input)
		if err != nil {
			responseErr(rw, err, http.StatusBadRequest)
			return
		}

		output, _ = srv.CreateTable(ctx, &input)
	case "PutItem":
		input := dynamodb.PutItemInput{}

		err := json.Unmarshal(bodyBytes, &input)
		if err != nil {
			responseErr(rw, err, http.StatusBadRequest)
			return
		}

		output, _ = srv.PutItem(ctx, &input)
	default:
	}

	if output != nil {
		d, _ := json.Marshal(output)
		_, _ = rw.Write(d)
	}
}

// ActivateDebug it activates the debug mode
func (srv *Server) ActivateDebug() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.langInterpreter.Debug = true
}

// ActivateNativeInterpreter it activates the debug mode
func (srv *Server) ActivateNativeInterpreter() {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.useNativeInterpreter = true

	for _, table := range srv.tables {
		table.UseNativeInterpreter = true
	}
}

func (srv *Server) setFailureCondition(condition FailureCondition) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.forceFailureErr = emulatingErrors[condition]
}

// SetInterpreter assigns a native interpreter
func (srv *Server) SetInterpreter(i interpreter.Interpreter) {
	native, ok := i.(*interpreter.Native)
	if !ok {
		panic("invalid interpreter type")
	}

	srv.nativeInterpreter = native

	for _, table := range srv.tables {
		table.NativeInterpreter = *native
	}
}

// GetNativeInterpreter returns native interpreter
func (srv *Server) GetNativeInterpreter() *interpreter.Native {
	return srv.nativeInterpreter
}

// CreateTable creates a new table
func (srv *Server) CreateTable(ctx context.Context, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	tableName := aws.ToString(input.TableName)
	if _, ok := srv.tables[tableName]; ok {
		return nil, &types.ResourceInUseException{Message: aws.String("Cannot create preexisting table")}
	}

	newTable := core.NewTable(tableName)
	newTable.SetAttributeDefinition(mapDynamoToTypesAttributeDefinitionSlice(input.AttributeDefinitions))
	newTable.BillingMode = aws.String(string(input.BillingMode))
	newTable.NativeInterpreter = *srv.nativeInterpreter
	newTable.UseNativeInterpreter = srv.useNativeInterpreter
	newTable.LangInterpreter = *srv.langInterpreter

	if err := newTable.CreatePrimaryIndex(mapDynamoToTypesCreateTableInput(input)); err != nil {
		return nil, mapKnownError(err)
	}

	if err := newTable.AddGlobalIndexes(mapDynamoToTypesGlobalSecondaryIndexes(input.GlobalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	if err := newTable.AddLocalIndexes(mapDynamoToTypesLocalSecondaryIndexes(input.LocalSecondaryIndexes)); err != nil {
		return nil, mapKnownError(err)
	}

	srv.tables[tableName] = newTable

	return &dynamodb.CreateTableOutput{
		TableDescription: mapTypesToDynamoTableDescription(newTable.Description(tableName)),
	}, nil
}

// DeleteTable deletes a table
func (srv *Server) DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := srv.getTable(tableName)
	if err != nil {
		return nil, mapKnownError(err)
	}

	desc := mapTypesToDynamoTableDescription(table.Description(tableName))

	delete(srv.tables, tableName)

	return &dynamodb.DeleteTableOutput{
		TableDescription: desc,
	}, nil
}

// UpdateTable update a table
func (srv *Server) UpdateTable(ctx context.Context, input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, ok := srv.tables[tableName]
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
func (srv *Server) DescribeTable(ctx context.Context, input *dynamodb.DescribeTableInput, ops ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	tableName := aws.ToString(input.TableName)

	table, err := srv.getTable(tableName)
	if err != nil {
		return nil, mapKnownError(err)
	}

	output := &dynamodb.DescribeTableOutput{
		Table: mapTypesToDynamoTableDescription(table.Description(tableName)),
	}

	return output, nil
}

// PutItem mock response for dynamodb
func (srv *Server) PutItem(ctx context.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := srv.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	item, err := table.Put(mapDynamoToTypesPutItemInput(input))

	return &dynamodb.PutItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
	}, mapKnownError(err)
}

// DeleteItem mock response for dynamodb
func (srv *Server) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := srv.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
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
func (srv *Server) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.UpdateExpression), aws.ToString(input.ConditionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := srv.getTable(aws.ToString(input.TableName))
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

	output := &dynamodb.UpdateItemOutput{
		Attributes: mapTypesToDynamoMapItem(item),
	}

	return output, nil
}

// GetItem mock response for dynamodb
func (srv *Server) GetItem(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := validateExpressionAttributes(input.ExpressionAttributeNames, nil, aws.ToString(input.ProjectionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := srv.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	key, err := table.KeySchema.GetKey(table.AttributesDef, mapDynamoToTypesMapItem(input.Key))
	if err != nil {
		return nil, &smithy.GenericAPIError{Code: "ValidationException", Message: err.Error()}
	}

	item := copyItem(mapTypesToDynamoMapItem(table.Data[key]))

	output := &dynamodb.GetItemOutput{
		Item: item,
	}

	return output, nil
}

// Query mock response for dynamodb
func (srv *Server) Query(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.KeyConditionExpression), aws.ToString(input.FilterExpression), aws.ToString(input.ProjectionExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := srv.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
	}

	indexName := aws.ToString(input.IndexName)

	if input.ScanIndexForward == nil {
		input.ScanIndexForward = aws.Bool(true)
	}

	items, lastKey := table.SearchData(mapDynamoToTypesQueryInput(input, indexName))

	count := int64(len(items))

	output := &dynamodb.QueryOutput{
		Items:            mapTypesToDynamoSliceMapItem(items),
		Count:            int32(count),
		LastEvaluatedKey: mapTypesToDynamoMapItem(lastKey),
	}

	return output, nil
}

// Scan mock scan operation
func (srv *Server) Scan(ctx context.Context, input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := validateExpressionAttributes(input.ExpressionAttributeNames, input.ExpressionAttributeValues, aws.ToString(input.ProjectionExpression), aws.ToString(input.FilterExpression))
	if err != nil {
		return nil, mapKnownError(err)
	}

	table, err := srv.getTable(aws.ToString(input.TableName))
	if err != nil {
		return nil, mapKnownError(err)
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

// SetItemCollectionMetrics set the value of the property itemCollectionMetrics
func (srv *Server) SetItemCollectionMetrics(itemCollectionMetrics map[string][]types.ItemCollectionMetrics) {
	srv.itemCollectionMetrics = itemCollectionMetrics
}

// BatchWriteItem mock response for dynamodb
func (srv *Server) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	if err := validateBatchWriteItemInput(input); err != nil {
		return &dynamodb.BatchWriteItemOutput{}, err
	}

	unprocessed := map[string][]types.WriteRequest{}

	for table, reqs := range input.RequestItems {
		for _, req := range reqs {
			err := executeBatchWriteRequest(ctx, srv, aws.String(table), req)

			err = handleBatchWriteRequestError(table, req, unprocessed, err)
			if err != nil {
				return &dynamodb.BatchWriteItemOutput{}, err
			}
		}
	}

	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems:      unprocessed,
		ItemCollectionMetrics: srv.itemCollectionMetrics,
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

func executeBatchWriteRequest(ctx context.Context, srv *Server, table *string, req types.WriteRequest) error {
	if req.PutRequest != nil {
		_, err := srv.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      req.PutRequest.Item,
			TableName: table,
		})

		return err
	}

	if req.DeleteRequest != nil {
		_, err := srv.DeleteItem(ctx, &dynamodb.DeleteItemInput{
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
func (srv *Server) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	if srv.forceFailureErr != nil {
		return nil, ErrForcedFailure
	}

	//TODO: Implement transact write

	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (srv *Server) getTable(tableName string) (*core.Table, error) {
	table, ok := srv.tables[tableName]
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
