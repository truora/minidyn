package client

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/types"
)

// maps dynamo to types

func mapDynamoToTypesAttributeDefinitionSlice(input []dynamodbtypes.AttributeDefinition) []*types.AttributeDefinition {
	if len(input) == 0 {
		return nil
	}

	output := []*types.AttributeDefinition{}

	for _, attributeDefinitionInput := range input {
		attributeDefinitionOutput := mapDynamoToTypesAttributeDefinition(attributeDefinitionInput)

		output = append(output, &attributeDefinitionOutput)
	}

	return output
}

func mapDynamoToTypesAttributeDefinition(input dynamodbtypes.AttributeDefinition) types.AttributeDefinition {
	return types.AttributeDefinition{
		AttributeName: input.AttributeName,
		AttributeType: toString(string(input.AttributeType)),
	}
}

func mapDynamoToTypesCreateTableInput(input *dynamodb.CreateTableInput) *types.CreateTableInput {
	if input == nil {
		return nil
	}

	return &types.CreateTableInput{
		ProvisionedThroughput: mapDynamoToTypesProvisionedThroughput(input.ProvisionedThroughput),
		KeySchema:             mapDynamoToTypesKeySchemaElements(input.KeySchema),
	}
}

func mapDynamoToTypesProvisionedThroughput(input *dynamodbtypes.ProvisionedThroughput) *types.ProvisionedThroughput {
	if input == nil {
		return nil
	}

	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  aws.ToInt64(input.ReadCapacityUnits),
		WriteCapacityUnits: aws.ToInt64(input.WriteCapacityUnits),
	}
}

func mapDynamoToTypesKeySchemaElements(input []dynamodbtypes.KeySchemaElement) []*types.KeySchemaElement {
	if len(input) == 0 {
		return nil
	}

	output := []*types.KeySchemaElement{}

	for _, keySchemaElement := range input {
		keySchemaElementOutput := &types.KeySchemaElement{
			AttributeName: aws.ToString(keySchemaElement.AttributeName),
			KeyType:       string(keySchemaElement.KeyType),
		}

		output = append(output, keySchemaElementOutput)
	}

	return output
}

func mapDynamoToTypesProjection(input *dynamodbtypes.Projection) *types.Projection {
	if input == nil {
		return nil
	}

	return &types.Projection{
		NonKeyAttributes: mapDynamoToTypesStringSlice(input.NonKeyAttributes),
		ProjectionType:   toString(string(input.ProjectionType)),
	}
}

func mapDynamoToTypesStringSlice(input []string) []*string {
	if len(input) == 0 {
		return nil
	}

	output := []*string{}

	for _, str := range input {
		output = append(output, toString(str))
	}

	return output
}

func mapDynamoToTypesStringMap(input map[string]string) map[string]*string {
	if len(input) == 0 {
		return nil
	}

	output := map[string]*string{}

	for key, value := range input {
		output[key] = toString(value)
	}

	return output
}

func mapDynamoToTypesGlobalSecondaryIndex(input dynamodbtypes.GlobalSecondaryIndex) *types.GlobalSecondaryIndex {
	return &types.GlobalSecondaryIndex{
		IndexName:             input.IndexName,
		KeySchema:             mapDynamoToTypesKeySchemaElements(input.KeySchema),
		Projection:            mapDynamoToTypesProjection(input.Projection),
		ProvisionedThroughput: mapDynamoToTypesProvisionedThroughput(input.ProvisionedThroughput),
	}
}

func mapDynamoToTypesGlobalSecondaryIndexes(input []dynamodbtypes.GlobalSecondaryIndex) []*types.GlobalSecondaryIndex {
	if input == nil {
		return nil
	}

	output := []*types.GlobalSecondaryIndex{}

	for _, index := range input {
		output = append(output, mapDynamoToTypesGlobalSecondaryIndex(index))
	}

	return output
}

func mapDynamoToTypesLocalSecondaryIndex(input dynamodbtypes.LocalSecondaryIndex) *types.LocalSecondaryIndex {
	return &types.LocalSecondaryIndex{
		IndexName:  input.IndexName,
		KeySchema:  mapDynamoToTypesKeySchemaElements(input.KeySchema),
		Projection: mapDynamoToTypesProjection(input.Projection),
	}
}

func mapDynamoToTypesLocalSecondaryIndexes(input []dynamodbtypes.LocalSecondaryIndex) []*types.LocalSecondaryIndex {
	if input == nil {
		return nil
	}

	output := []*types.LocalSecondaryIndex{}

	for _, index := range input {
		output = append(output, mapDynamoToTypesLocalSecondaryIndex(index))
	}

	return output
}

func mapDynamoTotypesGlobalSecondaryIndexUpdate(input dynamodbtypes.GlobalSecondaryIndexUpdate) *types.GlobalSecondaryIndexUpdate {
	return &types.GlobalSecondaryIndexUpdate{
		Create: mapDynamoToTypesCreateGlobalSecondaryIndexAction(input.Create),
		Delete: mapDynamoToTypesDeleteGlobalSecondaryIndexAction(input.Delete),
		Update: mapDynamoToTypesUpdateGlobalSecondaryIndexAction(input.Update),
	}
}

func mapDynamoToTypesCreateGlobalSecondaryIndexAction(input *dynamodbtypes.CreateGlobalSecondaryIndexAction) *types.CreateGlobalSecondaryIndexAction {
	if input == nil {
		return nil
	}

	return &types.CreateGlobalSecondaryIndexAction{
		IndexName:             input.IndexName,
		KeySchema:             mapDynamoToTypesKeySchemaElements(input.KeySchema),
		Projection:            mapDynamoToTypesProjection(input.Projection),
		ProvisionedThroughput: mapDynamoToTypesProvisionedThroughput(input.ProvisionedThroughput),
	}
}

func mapDynamoToTypesDeleteGlobalSecondaryIndexAction(input *dynamodbtypes.DeleteGlobalSecondaryIndexAction) *types.DeleteGlobalSecondaryIndexAction {
	if input == nil {
		return nil
	}

	return &types.DeleteGlobalSecondaryIndexAction{
		IndexName: input.IndexName,
	}
}

func mapDynamoToTypesUpdateGlobalSecondaryIndexAction(input *dynamodbtypes.UpdateGlobalSecondaryIndexAction) *types.UpdateGlobalSecondaryIndexAction {
	if input == nil {
		return nil
	}

	return &types.UpdateGlobalSecondaryIndexAction{
		IndexName:             input.IndexName,
		ProvisionedThroughput: mapDynamoToTypesProvisionedThroughput(input.ProvisionedThroughput),
	}
}

func mapDynamoToTypesPutItemInput(input *dynamodb.PutItemInput) *types.PutItemInput {
	if input == nil {
		return nil
	}

	return &types.PutItemInput{
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         toString(string(input.ConditionalOperator)),
		ExpressionAttributeNames:    input.ExpressionAttributeNames,
		ExpressionAttributeValues:   mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Item:                        mapDynamoToTypesMapItem(input.Item),
		ReturnConsumedCapacity:      toString(string(input.ReturnConsumedCapacity)),
		ReturnItemCollectionMetrics: toString(string(input.ReturnItemCollectionMetrics)),
		ReturnValues:                toString(string(input.ReturnValues)),
		TableName:                   input.TableName,
	}
}

func mapDynamoToTypesMapItem(input map[string]dynamodbtypes.AttributeValue) map[string]*types.Item {
	if len(input) == 0 {
		return nil
	}

	output := map[string]*types.Item{}

	for key, item := range input {
		output[strings.ToLower(key)] = mapDynamoToTypesItem(item)
	}

	return output
}

func mapDynamoToTypesSliceItem(input []dynamodbtypes.AttributeValue) []*types.Item {
	if len(input) == 0 {
		return nil
	}

	output := []*types.Item{}

	for _, item := range input {
		output = append(output, mapDynamoToTypesItem(item))
	}

	return output
}

func mapDynamoToTypesItem(item dynamodbtypes.AttributeValue) *types.Item {
	itemB, ok := item.(*dynamodbtypes.AttributeValueMemberB)
	if ok {
		return &types.Item{B: itemB.Value}
	}

	itemBOOL, ok := item.(*dynamodbtypes.AttributeValueMemberBOOL)
	if ok {
		return &types.Item{BOOL: &itemBOOL.Value}
	}

	itemBS, ok := item.(*dynamodbtypes.AttributeValueMemberBS)
	if ok {
		return &types.Item{BS: itemBS.Value}
	}

	itemS, ok := item.(*dynamodbtypes.AttributeValueMemberS)
	if ok {
		return &types.Item{S: types.ToString(itemS.Value)}
	}

	itemN, ok := item.(*dynamodbtypes.AttributeValueMemberN)
	if ok {
		return &types.Item{N: types.ToString(itemN.Value)}
	}

	itemNS, ok := item.(*dynamodbtypes.AttributeValueMemberNS)
	if ok {
		return &types.Item{NS: toStringSlice(itemNS.Value)}
	}

	itemSS, ok := item.(*dynamodbtypes.AttributeValueMemberSS)
	if ok {
		return &types.Item{SS: toStringSlice(itemSS.Value)}
	}

	return mapDynamoToTypesAttributeDefinitionMapOrList(item)
}

func mapDynamoToTypesAttributeDefinitionMapOrList(item dynamodbtypes.AttributeValue) *types.Item {
	itemL, ok := item.(*dynamodbtypes.AttributeValueMemberL)
	if ok {
		output := []*types.Item{}

		for _, itemLValue := range itemL.Value {
			output = append(output, mapDynamoToTypesItem(itemLValue))
		}

		return &types.Item{L: output}
	}

	itemM, ok := item.(*dynamodbtypes.AttributeValueMemberM)
	if ok {
		output := map[string]*types.Item{}

		for key, itemMValue := range itemM.Value {
			output[key] = mapDynamoToTypesItem(itemMValue)
		}

		return &types.Item{M: output}
	}

	nullTrue := true

	return &types.Item{NULL: &nullTrue}
}

func mapDynamoToTypesDeleteItemInput(input *dynamodb.DeleteItemInput) *types.DeleteItemInput {
	if input == nil {
		return nil
	}

	return &types.DeleteItemInput{
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         toString(string(input.ConditionalOperator)),
		Expected:                    mapDynamoToTypesExpectedAttributeValueMap(input.Expected),
		ExpressionAttributeNames:    mapDynamoToTypesStringMap(input.ExpressionAttributeNames),
		ExpressionAttributeValues:   mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Key:                         mapDynamoToTypesMapItem(input.Key),
		ReturnConsumedCapacity:      toString(string(input.ReturnConsumedCapacity)),
		ReturnItemCollectionMetrics: toString(string(input.ReturnItemCollectionMetrics)),
		ReturnValues:                toString(string(input.ReturnValues)),
		TableName:                   input.TableName,
	}
}

func mapDynamoToTypesExpectedAttributeValue(input dynamodbtypes.ExpectedAttributeValue) *types.ExpectedAttributeValue {
	return &types.ExpectedAttributeValue{
		AttributeValueList: mapDynamoToTypesSliceItem(input.AttributeValueList),
		ComparisonOperator: toString(string(input.ComparisonOperator)),
		Exists:             input.Exists,
		Value:              mapDynamoToTypesItem(input.Value),
	}
}

func mapDynamoToTypesExpectedAttributeValueMap(input map[string]dynamodbtypes.ExpectedAttributeValue) map[string]*types.ExpectedAttributeValue {
	if len(input) == 0 {
		return nil
	}

	output := map[string]*types.ExpectedAttributeValue{}

	for key, item := range input {
		output[key] = mapDynamoToTypesExpectedAttributeValue(item)
	}

	return output
}

func mapDynamoToTypesUpdateItemInput(input *dynamodb.UpdateItemInput) *types.UpdateItemInput {
	return &types.UpdateItemInput{
		ConditionExpression:                 input.ConditionExpression,
		ConditionalOperator:                 toString(string(input.ConditionalOperator)),
		Expected:                            mapDynamoToTypesExpectedAttributeValueMap(input.Expected),
		ExpressionAttributeNames:            input.ExpressionAttributeNames,
		ExpressionAttributeValues:           mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Key:                                 mapDynamoToTypesMapItem(input.Key),
		ReturnConsumedCapacity:              toString(string(input.ReturnConsumedCapacity)),
		ReturnItemCollectionMetrics:         toString(string(input.ReturnItemCollectionMetrics)),
		ReturnValues:                        toString(string(input.ReturnValues)),
		TableName:                           input.TableName,
		UpdateExpression:                    *input.UpdateExpression,
		ReturnValuesOnConditionCheckFailure: toString(string(input.ReturnValuesOnConditionCheckFailure)),
	}
}

func mapDynamoToTypesQueryInput(input *dynamodb.QueryInput, indexName string) core.QueryInput {
	output := core.QueryInput{
		Index:                     indexName,
		ExpressionAttributeValues: mapDynamoToTypesMapItem(input.ExpressionAttributeValues),
		Aliases:                   input.ExpressionAttributeNames,
		ExclusiveStartKey:         mapDynamoToTypesMapItem(input.ExclusiveStartKey),
	}

	if input.Limit != nil {
		output.Limit = int64(aws.ToInt32(input.Limit))
	}

	if input.KeyConditionExpression != nil {
		output.KeyConditionExpression = aws.ToString(input.KeyConditionExpression)
	}

	if input.FilterExpression != nil {
		output.FilterExpression = aws.ToString(input.FilterExpression)
	}

	if input.ScanIndexForward != nil {
		output.ScanIndexForward = aws.ToBool(input.ScanIndexForward)
	}

	return output
}

// map types to dynamo

func mapTypesToDynamoTableDescription(input *types.TableDescription) *dynamodbtypes.TableDescription {
	if input == nil {
		return nil
	}

	return &dynamodbtypes.TableDescription{
		TableName:              toString(input.TableName),
		ItemCount:              aws.Int64(input.ItemCount),
		KeySchema:              mapTypesToDynamoKeySchemaElements(input.KeySchema),
		GlobalSecondaryIndexes: mapTypesToDynamoTypesGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  mapTypesToDynamoLocalSecondaryIndexes(input.LocalSecondaryIndexes),
	}
}

func mapTypesToDynamoKeySchemaElements(input []types.KeySchemaElement) []dynamodbtypes.KeySchemaElement {
	if len(input) == 0 || input == nil {
		return nil
	}

	output := []dynamodbtypes.KeySchemaElement{}

	for _, keySchemaElement := range input {
		attributeName := keySchemaElement.AttributeName
		keySchemaElementOutput := dynamodbtypes.KeySchemaElement{
			AttributeName: &attributeName,
			KeyType:       dynamodbtypes.KeyType(keySchemaElement.KeyType),
		}

		output = append(output, keySchemaElementOutput)
	}

	return output
}

func mapTypesToDynamoKeySchemaElementsPointer(input []*types.KeySchemaElement) []dynamodbtypes.KeySchemaElement {
	if len(input) == 0 || input == nil {
		return nil
	}

	output := []dynamodbtypes.KeySchemaElement{}

	for _, keySchemaElement := range input {
		keySchemaElementOutput := dynamodbtypes.KeySchemaElement{
			AttributeName: &keySchemaElement.AttributeName,
			KeyType:       dynamodbtypes.KeyType(keySchemaElement.KeyType),
		}

		output = append(output, keySchemaElementOutput)
	}

	return output
}

func mapTypesToDynamoGlobalSecondaryIndex(input types.GlobalSecondaryIndexDescription) dynamodbtypes.GlobalSecondaryIndexDescription {
	return dynamodbtypes.GlobalSecondaryIndexDescription{
		IndexName:      input.IndexName,
		KeySchema:      mapTypesToDynamoKeySchemaElements(input.KeySchema),
		Projection:     mapTypesToDynamoProjection(input.Projection),
		Backfilling:    input.Backfilling,
		IndexArn:       input.IndexArn,
		IndexSizeBytes: input.IndexSizeBytes,
		IndexStatus:    dynamodbtypes.IndexStatus(aws.ToString(input.IndexStatus)),
		ItemCount:      aws.Int64(input.ItemCount),
	}
}

func mapTypesToDynamoProjection(input *types.Projection) *dynamodbtypes.Projection {
	if input == nil {
		return nil
	}

	return &dynamodbtypes.Projection{
		NonKeyAttributes: mapTypesToDynamoStringSlice(input.NonKeyAttributes),
	}
}

func mapTypesToDynamoProvisionedThroughput(input *types.ProvisionedThroughput) *dynamodbtypes.ProvisionedThroughputDescription {
	if input == nil {
		return nil
	}

	return &dynamodbtypes.ProvisionedThroughputDescription{
		ReadCapacityUnits:  aws.Int64(input.ReadCapacityUnits),
		WriteCapacityUnits: aws.Int64(input.WriteCapacityUnits),
	}
}

func mapTypesToDynamoStringSlice(input []*string) []string {
	if len(input) == 0 || input == nil {
		return nil
	}

	output := []string{}

	for _, str := range input {
		output = append(output, aws.ToString(str))
	}

	return output
}

func mapTypesToDynamoTypesGlobalSecondaryIndexes(input []types.GlobalSecondaryIndexDescription) []dynamodbtypes.GlobalSecondaryIndexDescription {
	if len(input) == 0 || input == nil {
		return nil
	}

	output := []dynamodbtypes.GlobalSecondaryIndexDescription{}

	for _, index := range input {
		output = append(output, mapTypesToDynamoGlobalSecondaryIndex(index))
	}

	return output
}

func mapTypesToDynamoLocalSecondaryIndex(input types.LocalSecondaryIndexDescription) dynamodbtypes.LocalSecondaryIndexDescription {
	return dynamodbtypes.LocalSecondaryIndexDescription{
		IndexName:  input.IndexName,
		KeySchema:  mapTypesToDynamoKeySchemaElements(input.KeySchema),
		Projection: mapTypesToDynamoProjection(input.Projection),
	}
}

func mapTypesToDynamoLocalSecondaryIndexes(input []types.LocalSecondaryIndexDescription) []dynamodbtypes.LocalSecondaryIndexDescription {
	output := []dynamodbtypes.LocalSecondaryIndexDescription{}

	for _, index := range input {
		output = append(output, mapTypesToDynamoLocalSecondaryIndex(index))
	}

	return output
}

func mapTypesToDynamoItem(item *types.Item) dynamodbtypes.AttributeValue {
	if len(item.B) != 0 {
		return &dynamodbtypes.AttributeValueMemberB{
			Value: item.B,
		}
	}

	if item.BOOL != nil {
		return &dynamodbtypes.AttributeValueMemberBOOL{
			Value: *item.BOOL,
		}
	}

	if len(item.BS) != 0 {
		return &dynamodbtypes.AttributeValueMemberBS{
			Value: item.BS,
		}
	}

	if item.N != nil {
		return &dynamodbtypes.AttributeValueMemberN{
			Value: types.StringValue(item.N),
		}
	}

	if len(item.NS) != 0 {
		return &dynamodbtypes.AttributeValueMemberNS{
			Value: toStringValueSlice(item.NS),
		}
	}

	if item.S != nil {
		return &dynamodbtypes.AttributeValueMemberS{
			Value: types.StringValue(item.S),
		}
	}

	if len(item.SS) != 0 {
		return &dynamodbtypes.AttributeValueMemberSS{
			Value: toStringValueSlice(item.SS),
		}
	}

	return mapTypesToDynamoAttributeDefinitionMapOrList(item)
}

func mapTypesToDynamoAttributeDefinitionMapOrList(item *types.Item) dynamodbtypes.AttributeValue {
	if len(item.L) != 0 {
		output := []dynamodbtypes.AttributeValue{}

		for _, itemLValue := range item.L {
			output = append(output, mapTypesToDynamoItem(itemLValue))
		}

		return &dynamodbtypes.AttributeValueMemberL{Value: output}
	}

	if len(item.M) != 0 {
		output := map[string]dynamodbtypes.AttributeValue{}

		for key, itemMValue := range item.M {
			output[key] = mapTypesToDynamoItem(itemMValue)
		}

		return &dynamodbtypes.AttributeValueMemberM{Value: output}
	}

	return &dynamodbtypes.AttributeValueMemberNULL{Value: true}
}

func mapTypesToDynamoMapItem(input map[string]*types.Item) map[string]dynamodbtypes.AttributeValue {
	output := map[string]dynamodbtypes.AttributeValue{}

	for key, item := range input {
		output[key] = mapTypesToDynamoItem(item)
	}

	return output
}

func mapTypesToDynamoSliceMapItem(input []map[string]*types.Item) []map[string]dynamodbtypes.AttributeValue {
	output := []map[string]dynamodbtypes.AttributeValue{}

	for _, item := range input {
		output = append(output, mapTypesToDynamoMapItem(item))
	}

	return output
}

func toStringSlice(slice []string) []*string {
	output := []*string{}

	for _, item := range slice {
		newItem := item
		output = append(output, &newItem)
	}

	return output
}

func toStringValueSlice(slice []*string) []string {
	output := []string{}

	for _, item := range slice {
		output = append(output, *item)
	}

	return output
}

func toString(str string) *string {
	if str == "" {
		return nil
	}

	return aws.String(str)
}

func mapKnownError(err error) error {
	var intErr types.Error

	if !errors.As(err, &intErr) {
		return err
	}

	switch intErr.Code() {
	case "ConditionalCheckFailedException":
		checkErr := &dynamodbtypes.ConditionalCheckFailedException{
			Message: aws.String(intErr.Message()),
		}

		var conditionalErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalErr) {
			checkErr.Item = mapTypesToDynamoMapItem(conditionalErr.Item)
		}

		return checkErr
	case "ResourceNotFoundException":
		return &dynamodbtypes.ResourceNotFoundException{Message: aws.String(intErr.Message())}
	}

	return err
}
