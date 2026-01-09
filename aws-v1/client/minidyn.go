package client

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
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
	emulatedInternalServeError = awserr.New(dynamodb.ErrCodeInternalServerError, "emulated error", nil)
	// ErrForcedFailure when the error is forced
	//
	// Deprecated: use EmulateFailure instead
	ErrForcedFailure = errors.New("forced failure response")

	emulatingErrors = map[FailureCondition]error{
		FailureConditionNone:                nil,
		FailureConditionInternalServerError: emulatedInternalServeError,
		FailureConditionDeprecated:          ErrForcedFailure,
	}
)

// EmulateFailure forces the fake client to fail
func EmulateFailure(client dynamodbiface.DynamoDBAPI, condition FailureCondition) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("EmulateFailure: invalid client type")
	}

	fakeClient.setFailureCondition(condition)
}

// ActiveForceFailure active force operation to fail
func ActiveForceFailure(client dynamodbiface.DynamoDBAPI) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("ActiveForceFailure: invalid client type")
	}

	fakeClient.setFailureCondition(FailureConditionDeprecated)
}

// DeactiveForceFailure deactive force operation to fail
func DeactiveForceFailure(client dynamodbiface.DynamoDBAPI) {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("DeactiveForceFailure: invalid client type")
	}

	fakeClient.setFailureCondition(FailureConditionNone)
}

// AddTable add a new table
func AddTable(client dynamodbiface.DynamoDBAPI, tableName, partitionKey, rangeKey string) error {
	input := generateAddTableInput(tableName, partitionKey, rangeKey)

	_, err := client.CreateTable(input)

	return err
}

// AddIndex add a new index to the table table
func AddIndex(client dynamodbiface.DynamoDBAPI, tableName, indexName, partitionKey, rangeKey string) error {
	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(partitionKey),
			KeyType:       aws.String("HASH"),
		},
	}

	attributes := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(partitionKey),
			AttributeType: aws.String("S"),
		},
	}

	if rangeKey != "" {
		keySchema = append(keySchema, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(rangeKey),
			KeyType:       aws.String("RANGE"),
		})

		attributes = append(attributes, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(rangeKey),
			AttributeType: aws.String("S"),
		})
	}

	input := &dynamodb.UpdateTableInput{
		AttributeDefinitions: attributes,
		TableName:            aws.String(tableName),
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{
			{
				Create: &dynamodb.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(indexName),
					KeySchema: keySchema,
					Projection: &dynamodb.Projection{
						ProjectionType: aws.String("ALL"),
					},
				},
			},
		},
	}

	_, err := client.UpdateTable(input)

	return err
}

// ClearTable removes all data from a specific table
func ClearTable(client dynamodbiface.DynamoDBAPI, tableName string) error {
	fakeClient, ok := client.(*Client)
	if !ok {
		panic("ClearTable: invalid client type")
	}

	table, err := fakeClient.getTable(tableName)
	if err != nil {
		return err
	}

	fakeClient.mu.Lock()
	defer fakeClient.mu.Unlock()

	table.Clear()

	for _, index := range table.Indexes {
		index.Clear()
	}

	return nil
}

func generateAddTableInput(tableName, hashKey, rangeKey string) *dynamodb.CreateTableInput {
	var cunit int64 = 10

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: aws.String("S"),
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       aws.String("HASH"),
			},
		},
		TableName: aws.String(tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  &cunit,
			WriteCapacityUnits: &cunit,
		},
	}

	if rangeKey != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(rangeKey),
				AttributeType: aws.String("S"),
			},
		)

		input.KeySchema = append(input.KeySchema,
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String(rangeKey),
				KeyType:       aws.String("RANGE"),
			},
		)
	}

	return input
}

func copyItem(item map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	copy := map[string]*dynamodb.AttributeValue{}
	for key, val := range item {
		copy[key] = val
	}

	return copy
}
