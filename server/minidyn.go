package client

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// FailureCondition describe the failure condtion to emulate
type FailureCondition string

const (
	// FailureConditionNone emulates the system is working
	FailureConditionNone FailureCondition = "none"
	// FailureConditionInternalServerError emulates dynamodb having internal issues
	FailureConditionInternalServerError FailureCondition = "internal_server"
	// FailureConditionDeprecated returns the old error
	FailureConditionDeprecated FailureCondition = "deprecated"
)

var (
	// emulatedInternalServeError represents the error for dynamodb internal server error
	emulatedInternalServeError = types.InternalServerError{Message: aws.String("emulated error")}
	// ErrForcedFailure when the error is forced
	// Deprecated: use EmulateFailure instead
	ErrForcedFailure = errors.New("forced failure response")

	emulatingErrors = map[FailureCondition]error{
		FailureConditionNone:                nil,
		FailureConditionInternalServerError: &emulatedInternalServeError,
		FailureConditionDeprecated:          ErrForcedFailure,
	}
)

// AddTable add a new table
func AddTable(ctx context.Context, client *dynamodb.Client, tableName, partitionKey, rangeKey string) error {
	input := generateAddTableInput(tableName, partitionKey, rangeKey)

	_, err := client.CreateTable(ctx, input)

	return err
}

// AddIndex add a new index to the table table
func AddIndex(ctx context.Context, client *dynamodb.Client, tableName, indexName, partitionKey, rangeKey string) error {
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String(partitionKey),
			KeyType:       types.KeyTypeHash,
		},
	}

	attributes := []types.AttributeDefinition{
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: types.ScalarAttributeTypeS,
		},
	}

	if rangeKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(rangeKey),
			KeyType:       types.KeyTypeRange,
		})

		attributes = append(attributes, types.AttributeDefinition{
			AttributeName: aws.String(rangeKey),
			AttributeType: types.ScalarAttributeTypeS,
		})
	}

	input := &dynamodb.UpdateTableInput{
		AttributeDefinitions: attributes,
		TableName:            aws.String(tableName),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{
				Create: &types.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(indexName),
					KeySchema: keySchema,
					Projection: &types.Projection{
						ProjectionType: types.ProjectionTypeAll,
					},
				},
			},
		},
	}

	_, err := client.UpdateTable(ctx, input)

	return err
}

// ClearTable removes all data from a specific table
func ClearTable(client *dynamodb.Client, tableName string) error {
	// dynamodb.Client, ok := client.(*Client)
	// if !ok {
	// 	panic("ClearTable: invalid client type")
	// }

	// table, err := dynamodb.Client.getTable(tableName)
	// if err != nil {
	// 	return err
	// }

	// dynamodb.Client.mu.Lock()
	// defer dynamodb.Client.mu.Unlock()

	// table.Clear()

	// for _, index := range table.Indexes {
	// 	index.Clear()
	// }

	return nil
}

func generateAddTableInput(tableName, hashKey, rangeKey string) *dynamodb.CreateTableInput {
	var cunit int64 = 10

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		TableName: aws.String(tableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &cunit,
			WriteCapacityUnits: &cunit,
		},
	}

	if rangeKey != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			types.AttributeDefinition{
				AttributeName: aws.String(rangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		)

		input.KeySchema = append(input.KeySchema,
			types.KeySchemaElement{
				AttributeName: aws.String(rangeKey),
				KeyType:       types.KeyTypeRange,
			},
		)
	}

	return input
}

func copyItem(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	copy := map[string]types.AttributeValue{}
	for key, val := range item {
		copy[key] = val
	}

	return copy
}
