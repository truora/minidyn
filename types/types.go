package types

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/private/protocol"
)

type Item struct {
	_ struct{} `type:"structure"`

	// An attribute of type Binary. For example:
	//
	// "B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
	// B is automatically base64 encoded/decoded by the SDK.
	B []byte `type:"blob"`

	// An attribute of type Boolean. For example:
	//
	// "BOOL": true
	BOOL *bool `type:"boolean"`

	// An attribute of type Binary Set. For example:
	//
	// "BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]
	BS [][]byte `type:"list"`

	// An attribute of type List. For example:
	//
	// "L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]
	L []Item `type:"list"`

	// An attribute of type Map. For example:
	//
	// "M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
	M map[string]Item `type:"map"`

	// An attribute of type Number. For example:
	//
	// "N": "123.45"
	//
	// Numbers are sent across the network to DynamoDB as strings, to maximize compatibility
	// across languages and libraries. However, DynamoDB treats them as number type
	// attributes for mathematical operations.
	N string `type:"string"`

	// An attribute of type Number Set. For example:
	//
	// "NS": ["42.2", "-19", "7.5", "3.14"]
	//
	// Numbers are sent across the network to DynamoDB as strings, to maximize compatibility
	// across languages and libraries. However, DynamoDB treats them as number type
	// attributes for mathematical operations.
	NS []string `type:"list"`

	// An attribute of type Null. For example:
	//
	// "NULL": true
	NULL *bool `type:"boolean"`

	// An attribute of type String. For example:
	//
	// "S": "Hello"
	S string `type:"string"`

	// An attribute of type String Set. For example:
	//
	// "SS": ["Giraffe", "Hippo" ,"Zebra"]
	SS []string `type:"list"`
}

type AttributeDefinition struct {
	_             struct{} `type:"structure"`
	AttributeName *string  `min:"1" type:"string" required:"true"`
	AttributeType *string  `type:"string" required:"true" enum:"ScalarAttributeType"`
}

type KeySchemaElement struct {
	_             struct{} `type:"structure"`
	AttributeName string   `min:"1" type:"string" required:"true"`
	KeyType       string   `type:"string" required:"true" enum:"KeyType"`
}

type ProvisionedThroughput struct {
	_                  struct{} `type:"structure"`
	ReadCapacityUnits  int64    `min:"1" type:"long" required:"true"`
	WriteCapacityUnits int64    `min:"1" type:"long" required:"true"`
}

type CreateTableInput struct {
	ProvisionedThroughput *ProvisionedThroughput `type:"structure"`
	KeySchema             []*KeySchemaElement    `min:"1" type:"list" required:"true"`
}

type Projection struct {
	_                struct{}  `type:"structure"`
	NonKeyAttributes []*string `min:"1" type:"list"`
	ProjectionType   *string   `type:"string" enum:"ProjectionType"`
}

// Represents the properties of a global secondary index.
type GlobalSecondaryIndex struct {
	_                     struct{}               `type:"structure"`
	IndexName             *string                `min:"3" type:"string" required:"true"`
	KeySchema             []*KeySchemaElement    `min:"1" type:"list" required:"true"`
	Projection            *Projection            `type:"structure" required:"true"`
	ProvisionedThroughput *ProvisionedThroughput `type:"structure"`
}

type GlobalSecondaryIndexUpdate struct {
	_      struct{}                          `type:"structure"`
	Create *CreateGlobalSecondaryIndexAction `type:"structure"`
	Delete *DeleteGlobalSecondaryIndexAction `type:"structure"`
	Update *UpdateGlobalSecondaryIndexAction `type:"structure"`
}

// Represents the new provisioned throughput settings to be applied to a global
// secondary index.
type UpdateGlobalSecondaryIndexAction struct {
	_                     struct{}               `type:"structure"`
	IndexName             *string                `min:"3" type:"string" required:"true"`
	ProvisionedThroughput *ProvisionedThroughput `type:"structure" required:"true"`
}

// Represents a global secondary index to be deleted from an existing table.
type DeleteGlobalSecondaryIndexAction struct {
	_         struct{} `type:"structure"`
	IndexName *string  `min:"3" type:"string" required:"true"`
}

// Represents a new global secondary index to be added to an existing table.
type CreateGlobalSecondaryIndexAction struct {
	_                     struct{}               `type:"structure"`
	IndexName             *string                `min:"3" type:"string" required:"true"`
	KeySchema             []*KeySchemaElement    `min:"1" type:"list" required:"true"`
	Projection            *Projection            `type:"structure" required:"true"`
	ProvisionedThroughput *ProvisionedThroughput `type:"structure"`
}

// Represents the properties of a local secondary index.
type LocalSecondaryIndex struct {
	_          struct{}            `type:"structure"`
	IndexName  *string             `min:"3" type:"string" required:"true"`
	KeySchema  []*KeySchemaElement `min:"1" type:"list" required:"true"`
	Projection *Projection         `type:"structure" required:"true"`
}

// Represents the properties of a global secondary index.
type GlobalSecondaryIndexDescription struct {
	_              struct{}           `type:"structure"`
	Backfilling    *bool              `type:"boolean"`
	IndexArn       *string            `type:"string"`
	IndexName      *string            `min:"3" type:"string"`
	IndexSizeBytes *int64             `type:"long"`
	IndexStatus    *string            `type:"string" enum:"IndexStatus"`
	ItemCount      int64              `type:"long"`
	KeySchema      []KeySchemaElement `min:"1" type:"list"`
	Projection     *Projection        `type:"structure"`
}

// Represents the properties of a local secondary index.
type LocalSecondaryIndexDescription struct {
	_              struct{}           `type:"structure"`
	IndexArn       *string            `type:"string"`
	IndexName      *string            `min:"3" type:"string"`
	IndexSizeBytes *int64             `type:"long"`
	ItemCount      int64              `type:"long"`
	KeySchema      []KeySchemaElement `min:"1" type:"list"`
	Projection     *Projection        `type:"structure"`
}

// A condition specified in the operation could not be evaluated.
type ConditionalCheckFailedException struct {
	_            struct{}                  `type:"structure"`
	RespMetadata protocol.ResponseMetadata `json:"-" xml:"-"`
	Message_     string                    `locationName:"message" type:"string"`
}

// String returns the string representation
func (s ConditionalCheckFailedException) String() string {
	return awsutil.Prettify(s)
}

// GoString returns the string representation
func (s ConditionalCheckFailedException) GoString() string {
	return s.String()
}

func newErrorConditionalCheckFailedException(v protocol.ResponseMetadata) error {
	return &ConditionalCheckFailedException{
		RespMetadata: v,
	}
}

// Code returns the exception type name.
func (s *ConditionalCheckFailedException) Code() string {
	return "ConditionalCheckFailedException"
}

// Message returns the exception's message.
func (s *ConditionalCheckFailedException) Message() string {
	if s.Message_ != "" {
		return s.Message_
	}
	return ""
}

// OrigErr always returns nil, satisfies awserr.Error interface.
func (s *ConditionalCheckFailedException) OrigErr() error {
	return nil
}

func (s *ConditionalCheckFailedException) Error() string {
	return fmt.Sprintf("%s: %s", s.Code(), s.Message())
}

// Status code returns the HTTP status code for the request's response error.
func (s *ConditionalCheckFailedException) StatusCode() int {
	return s.RespMetadata.StatusCode
}

// RequestID returns the service's response RequestID for request.
func (s *ConditionalCheckFailedException) RequestID() string {
	return s.RespMetadata.RequestID
}

// Represents the input of a PutItem operation.
type PutItemInput struct {
	_                           struct{}          `type:"structure"`
	ConditionExpression         *string           `type:"string"`
	ConditionalOperator         *string           `type:"string" enum:"ConditionalOperator"`
	ExpressionAttributeNames    map[string]string `type:"map"`
	ExpressionAttributeValues   map[string]Item   `type:"map"`
	Item                        map[string]Item   `type:"map" required:"true"`
	ReturnConsumedCapacity      *string           `type:"string" enum:"ReturnConsumedCapacity"`
	ReturnItemCollectionMetrics *string           `type:"string" enum:"ReturnItemCollectionMetrics"`
	ReturnValues                *string           `type:"string" enum:"ReturnValue"`
	TableName                   *string           `min:"3" type:"string" required:"true"`
}

// Represents the input of an UpdateItem operation.
type UpdateItemInput struct {
	_                           struct{}                           `type:"structure"`
	AttributeUpdates            map[string]*AttributeValueUpdate   `type:"map"`
	ConditionExpression         *string                            `type:"string"`
	ConditionalOperator         *string                            `type:"string" enum:"ConditionalOperator"`
	Expected                    map[string]*ExpectedAttributeValue `type:"map"`
	ExpressionAttributeNames    map[string]string                  `type:"map"`
	ExpressionAttributeValues   map[string]Item                    `type:"map"`
	Key                         map[string]Item                    `type:"map" required:"true"`
	ReturnConsumedCapacity      *string                            `type:"string" enum:"ReturnConsumedCapacity"`
	ReturnItemCollectionMetrics *string                            `type:"string" enum:"ReturnItemCollectionMetrics"`
	ReturnValues                *string                            `type:"string" enum:"ReturnValue"`
	TableName                   *string                            `min:"3" type:"string" required:"true"`
	UpdateExpression            string                             `type:"string"`
}

// Represents the input of a DeleteItem operation.
type DeleteItemInput struct {
	_                           struct{}                           `type:"structure"`
	ConditionExpression         *string                            `type:"string"`
	ConditionalOperator         *string                            `type:"string" enum:"ConditionalOperator"`
	Expected                    map[string]*ExpectedAttributeValue `type:"map"`
	ExpressionAttributeNames    map[string]*string                 `type:"map"`
	ExpressionAttributeValues   map[string]Item                    `type:"map"`
	Key                         map[string]Item                    `type:"map" required:"true"`
	ReturnConsumedCapacity      *string                            `type:"string" enum:"ReturnConsumedCapacity"`
	ReturnItemCollectionMetrics *string                            `type:"string" enum:"ReturnItemCollectionMetrics"`
	ReturnValues                *string                            `type:"string" enum:"ReturnValue"`
	TableName                   *string                            `min:"3" type:"string" required:"true"`
}

type ExpectedAttributeValue struct {
	_                  struct{} `type:"structure"`
	AttributeValueList []Item   `type:"list"`
	ComparisonOperator *string  `type:"string" enum:"ComparisonOperator"`
	Exists             *bool    `type:"boolean"`
	Value              Item     `type:"structure"`
}

type AttributeValueUpdate struct {
	_      struct{} `type:"structure"`
	Action *string  `type:"string" enum:"AttributeAction"`
	Value  Item     `type:"structure"`
}

// Represents the properties of a table.
type TableDescription struct {
	_                      struct{}                          `type:"structure"`
	GlobalSecondaryIndexes []GlobalSecondaryIndexDescription `type:"list"`
	GlobalTableVersion     string                            `type:"string"`
	ItemCount              int64                             `type:"long"`
	KeySchema              []KeySchemaElement                `min:"1" type:"list"`
	LatestStreamArn        string                            `min:"37" type:"string"`
	LatestStreamLabel      string                            `type:"string"`
	LocalSecondaryIndexes  []LocalSecondaryIndexDescription  `type:"list"`
	TableArn               string                            `type:"string"`
	TableId                string                            `type:"string"`
	TableName              string                            `min:"3" type:"string"`
	TableSizeBytes         int64                             `type:"long"`
	TableStatus            string                            `type:"string" enum:"TableStatus"`
}
