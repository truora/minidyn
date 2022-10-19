package client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/truora/minidyn/types"
)

func mapAttributeDefinitionToTypes(attrs []*dynamodb.AttributeDefinition) []*types.AttributeDefinition {
	tAttrs := make([]*types.AttributeDefinition, len(attrs))
	for i, attr := range attrs {
		tAttrs[i] = &types.AttributeDefinition{
			AttributeName: attr.AttributeName,
			AttributeType: attr.AttributeType,
		}
	}

	return tAttrs
}

// variable of type *dynamodb.CreateTableInput) as *types.CreateTableInput value in argument to newTable.CreatePrimaryIndex
func mapCreateTableInputToTypes(input *dynamodb.CreateTableInput) *types.CreateTableInput {
	if input == nil {
		return nil
	}

	createTableInput := &types.CreateTableInput{
		KeySchema: mapKeySchemaToTypes(input.KeySchema),
	}

	if input.ProvisionedThroughput != nil {
		createTableInput.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64Value(input.ProvisionedThroughput.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64Value(input.ProvisionedThroughput.WriteCapacityUnits),
		}
	}

	return createTableInput
}

func mapKeySchemaToTypes(ks []*dynamodb.KeySchemaElement) []*types.KeySchemaElement {
	keySchema := make([]*types.KeySchemaElement, len(ks))
	for i, ks := range ks {
		keySchema[i] = &types.KeySchemaElement{
			AttributeName: *ks.AttributeName,
			KeyType:       *ks.KeyType,
		}
	}

	return keySchema
}

func mapKeySchemaToDynamodb(ks []types.KeySchemaElement) []*dynamodb.KeySchemaElement {
	keySchema := make([]*dynamodb.KeySchemaElement, len(ks))
	for i, ks := range ks {
		keySchema[i] = &dynamodb.KeySchemaElement{
			AttributeName: aws.String(ks.AttributeName),
			KeyType:       aws.String(ks.KeyType),
		}
	}

	return keySchema
}

func mapGlobalSecondaryIndexesToTypes(input []*dynamodb.GlobalSecondaryIndex) []*types.GlobalSecondaryIndex {
	if input == nil {
		return nil
	}

	gsi := make([]*types.GlobalSecondaryIndex, len(input))
	for i, gs := range input {
		gsi[i] = &types.GlobalSecondaryIndex{
			IndexName:             gs.IndexName,
			Projection:            mapProjectionToTypes(gs.Projection),
			ProvisionedThroughput: mapProvisionedThroughputToTypes(gs.ProvisionedThroughput),
			KeySchema:             mapKeySchemaToTypes(gs.KeySchema),
		}
	}

	return gsi
}

func mapProjectionToTypes(prj *dynamodb.Projection) *types.Projection {
	if prj == nil {
		return nil
	}

	projection := &types.Projection{
		NonKeyAttributes: prj.NonKeyAttributes,
		ProjectionType:   prj.ProjectionType,
	}

	return projection
}

func mapProvisionedThroughputToTypes(pt *dynamodb.ProvisionedThroughput) *types.ProvisionedThroughput {
	if pt == nil {
		return nil
	}
	provisionedThroughput := &types.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64Value(pt.ReadCapacityUnits),
		WriteCapacityUnits: aws.Int64Value(pt.WriteCapacityUnits),
	}

	return provisionedThroughput
}

func mapLocalSecondaryIndexesToTypes(input []*dynamodb.LocalSecondaryIndex) []*types.LocalSecondaryIndex {
	if input == nil {
		return nil
	}

	lsi := make([]*types.LocalSecondaryIndex, len(input))
	for i, si := range input {
		lsi[i] = &types.LocalSecondaryIndex{
			IndexName:  si.IndexName,
			Projection: mapProjectionToTypes(si.Projection),
			KeySchema:  mapKeySchemaToTypes(si.KeySchema),
		}
	}

	return lsi
}

func mapGlobalSecondaryIndexDescriptionToDynamodb(input []types.GlobalSecondaryIndexDescription) []*dynamodb.GlobalSecondaryIndexDescription {
	gsi := make([]*dynamodb.GlobalSecondaryIndexDescription, len(input))
	for i, gs := range input {
		gsi[i] = &dynamodb.GlobalSecondaryIndexDescription{
			IndexName: gs.IndexName,
			Projection: &dynamodb.Projection{
				NonKeyAttributes: gs.Projection.NonKeyAttributes,
				ProjectionType:   gs.Projection.ProjectionType,
			},
			KeySchema: mapKeySchemaToDynamodb(gs.KeySchema),
		}
	}

	return gsi
}

func mapLocalSecondaryIndexDescriptionToDynamodb(input []types.LocalSecondaryIndexDescription) []*dynamodb.LocalSecondaryIndexDescription {
	lsi := make([]*dynamodb.LocalSecondaryIndexDescription, len(input))
	for i, si := range input {
		lsi[i] = &dynamodb.LocalSecondaryIndexDescription{
			IndexName: si.IndexName,
			Projection: &dynamodb.Projection{
				NonKeyAttributes: si.Projection.NonKeyAttributes,
				ProjectionType:   si.Projection.ProjectionType,
			},
			KeySchema: mapKeySchemaToDynamodb(si.KeySchema),
		}
	}

	return lsi
}

func mapTableDescriptionToDynamodb(td *types.TableDescription) *dynamodb.TableDescription {
	tableDescription := &dynamodb.TableDescription{
		TableName:              aws.String(td.TableName),
		ItemCount:              aws.Int64(td.ItemCount),
		KeySchema:              mapKeySchemaToDynamodb(td.KeySchema),
		GlobalSecondaryIndexes: mapGlobalSecondaryIndexDescriptionToDynamodb(td.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  mapLocalSecondaryIndexDescriptionToDynamodb(td.LocalSecondaryIndexes),
	}

	return tableDescription
}

func mapAttributeValueDefinitionToDynamodb(attrs []*dynamodb.AttributeDefinition) []*types.AttributeDefinition {
	if attrs == nil {
		return nil
	}

	attributeDefinitions := make([]*types.AttributeDefinition, len(attrs))

	for i, attr := range attrs {
		attributeDefinitions[i] = &types.AttributeDefinition{
			AttributeName: attr.AttributeName,
			AttributeType: attr.AttributeType,
		}
	}

	return attributeDefinitions
}

func mapGlobalSecondaryIndexUpdateToTypes(gsiUpdate *dynamodb.GlobalSecondaryIndexUpdate) *types.GlobalSecondaryIndexUpdate {
	if gsiUpdate == nil {
		return nil
	}

	gsiUpdates := &types.GlobalSecondaryIndexUpdate{}

	if gsiUpdate.Create != nil {
		gsiUpdates.Create = &types.CreateGlobalSecondaryIndexAction{
			IndexName:             gsiUpdate.Create.IndexName,
			KeySchema:             mapKeySchemaToTypes(gsiUpdate.Create.KeySchema),
			Projection:            mapProjectionToTypes(gsiUpdate.Create.Projection),
			ProvisionedThroughput: mapProvisionedThroughputToTypes(gsiUpdate.Create.ProvisionedThroughput),
		}
	}

	if gsiUpdate.Delete != nil {
		gsiUpdates.Delete = &types.DeleteGlobalSecondaryIndexAction{
			IndexName: gsiUpdate.Delete.IndexName,
		}
	}

	if gsiUpdate.Update != nil {
		gsiUpdates.Update = &types.UpdateGlobalSecondaryIndexAction{
			IndexName:             gsiUpdate.Update.IndexName,
			ProvisionedThroughput: mapProvisionedThroughputToTypes(gsiUpdate.Update.ProvisionedThroughput),
		}
	}

	return gsiUpdates
}

func mapDeleteItemInputToTypes(input *dynamodb.DeleteItemInput) *types.DeleteItemInput {
	if input == nil {
		return nil
	}

	deleteInput := &types.DeleteItemInput{
		TableName:                   input.TableName,
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         input.ConditionalOperator,
		ReturnValues:                input.ReturnValues,
		ExpressionAttributeNames:    input.ExpressionAttributeNames,
		ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
		ExpressionAttributeValues:   mapAttributeValueToTypes(input.ExpressionAttributeValues),
		Key:                         mapAttributeValueToTypes(input.Key),
	}

	return deleteInput
}

func mapPutItemInputToTypes(input *dynamodb.PutItemInput) *types.PutItemInput {
	if input == nil {
		return nil
	}

	putInput := &types.PutItemInput{
		TableName:                   input.TableName,
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         input.ConditionalOperator,
		ReturnValues:                input.ReturnValues,
		ExpressionAttributeNames:    aws.StringValueMap(input.ExpressionAttributeNames),
		ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
		Item:                        mapAttributeValueToTypes(input.Item),
		ExpressionAttributeValues:   mapAttributeValueToTypes(input.ExpressionAttributeValues),
	}

	return putInput
}

func mapUpdateItemInputToTypes(input *dynamodb.UpdateItemInput) *types.UpdateItemInput {
	updateInput := &types.UpdateItemInput{
		TableName:                   input.TableName,
		ConditionExpression:         input.ConditionExpression,
		ConditionalOperator:         input.ConditionalOperator,
		ReturnValues:                input.ReturnValues,
		ExpressionAttributeNames:    aws.StringValueMap(input.ExpressionAttributeNames),
		ReturnConsumedCapacity:      input.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: input.ReturnItemCollectionMetrics,
		ExpressionAttributeValues:   mapAttributeValueToTypes(input.ExpressionAttributeValues),
		Key:                         mapAttributeValueToTypes(input.Key),
		UpdateExpression:            aws.StringValue(input.UpdateExpression),
	}

	return updateInput
}

func mapAttributeValueToTypes(attrs map[string]*dynamodb.AttributeValue) map[string]*types.Item {
	mapItems := make(map[string]*types.Item, len(attrs))

	for key, attr := range attrs {
		mapItems[key] = &types.Item{
			B:    attr.B,
			BOOL: attr.BOOL,
			BS:   attr.BS,
			L:    mapAttributeValueListToTypes(attr.L),
			M:    mapAttributeValueToTypes(attr.M),
			N:    attr.N,
			NS:   attr.NS,
			NULL: attr.NULL,
			S:    attr.S,
			SS:   mapSlicePointers(attr.SS),
		}
	}

	return mapItems
}

func mapSlicePointers(arr []*string) []*string {
	newArr := make([]*string, len(arr))
	for i, st := range arr {
		newArr[i] = aws.String(*st)
	}

	return newArr
}

func mapAttributeValueListToTypes(attrs []*dynamodb.AttributeValue) []*types.Item {
	mapItems := make([]*types.Item, len(attrs))
	for i, attr := range attrs {
		mapItems[i] = &types.Item{
			B:    attr.B,
			BOOL: attr.BOOL,
			BS:   attr.BS,
			L:    mapAttributeValueListToTypes(attr.L),
			M:    mapAttributeValueToTypes(attr.M),
			N:    attr.N,
			NS:   attr.NS,
			NULL: attr.NULL,
			S:    attr.S,
			SS:   mapSlicePointers(attr.SS),
		}
	}

	return mapItems
}

func mapAttributeValueToDynamodb(attrs map[string]*types.Item) map[string]*dynamodb.AttributeValue {
	mapItems := make(map[string]*dynamodb.AttributeValue, len(attrs))

	for key, attr := range attrs {
		mapItems[key] = &dynamodb.AttributeValue{
			B:    attr.B,
			BOOL: attr.BOOL,
			BS:   attr.BS,
			L:    mapAttributeValueListToDynamodb(attr.L),
			M:    mapAttributeValueToDynamodb(attr.M),
			N:    attr.N,
			NS:   attr.NS,
			NULL: attr.NULL,
			S:    attr.S,
			SS:   attr.SS,
		}
	}

	return mapItems
}

func mapItemSliceToDynamodb(items []map[string]*types.Item) []map[string]*dynamodb.AttributeValue {
	mapAttrs := make([]map[string]*dynamodb.AttributeValue, 0)

	for _, item := range items {
		mapAttrs = append(mapAttrs, mapAttributeValueToDynamodb(item))
	}

	return mapAttrs
}

func mapAttributeValueListToDynamodb(attrs []*types.Item) []*dynamodb.AttributeValue {
	mapItems := make([]*dynamodb.AttributeValue, len(attrs))

	for i, attr := range attrs {
		mapItems[i] = &dynamodb.AttributeValue{
			B:    attr.B,
			BOOL: attr.BOOL,
			BS:   attr.BS,
			L:    mapAttributeValueListToDynamodb(attr.L),
			M:    mapAttributeValueToDynamodb(attr.M),
			N:    attr.N,
			NS:   attr.NS,
			NULL: attr.NULL,
			S:    attr.S,
			SS:   attr.SS,
		}
	}

	return mapItems
}
