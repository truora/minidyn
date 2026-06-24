package server

// Minimal output shapes encoded back to the client.
// They mirror DynamoDB JSON responses closely enough for SDK decoding.

// CreateTableOutput mirrors DynamoDB CreateTableOutput.
type CreateTableOutput struct {
	TableDescription any `json:"TableDescription,omitempty"`
}

// DeleteTableOutput mirrors DynamoDB DeleteTableOutput.
type DeleteTableOutput struct {
	TableDescription any `json:"TableDescription,omitempty"`
}

// UpdateTableOutput mirrors DynamoDB UpdateTableOutput.
type UpdateTableOutput struct {
	TableDescription any `json:"TableDescription,omitempty"`
}

// DescribeTableOutput mirrors DynamoDB DescribeTableOutput.
type DescribeTableOutput struct {
	Table any `json:"Table,omitempty"`
}

// Capacity mirrors DynamoDB Capacity (throughput consumed on a table or index).
type Capacity struct {
	CapacityUnits      *float64 `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits  *float64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits *float64 `json:"WriteCapacityUnits,omitempty"`
}

// ConsumedCapacity mirrors DynamoDB ConsumedCapacity.
type ConsumedCapacity struct {
	TableName              string              `json:"TableName,omitempty"`
	CapacityUnits          *float64            `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits      *float64            `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits     *float64            `json:"WriteCapacityUnits,omitempty"`
	Table                  *Capacity           `json:"Table,omitempty"`
	GlobalSecondaryIndexes map[string]Capacity `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  map[string]Capacity `json:"LocalSecondaryIndexes,omitempty"`
}

// PutItemOutput mirrors DynamoDB PutItemOutput.
type PutItemOutput struct {
	Attributes       map[string]*AttributeValue `json:"Attributes,omitempty"`
	ConsumedCapacity *ConsumedCapacity          `json:"ConsumedCapacity,omitempty"`
}

// DeleteItemOutput mirrors DynamoDB DeleteItemOutput.
type DeleteItemOutput struct {
	Attributes       map[string]*AttributeValue `json:"Attributes,omitempty"`
	ConsumedCapacity *ConsumedCapacity          `json:"ConsumedCapacity,omitempty"`
}

// UpdateItemOutput mirrors DynamoDB UpdateItemOutput.
type UpdateItemOutput struct {
	Attributes       map[string]*AttributeValue `json:"Attributes,omitempty"`
	ConsumedCapacity *ConsumedCapacity          `json:"ConsumedCapacity,omitempty"`
}

// GetItemOutput mirrors DynamoDB GetItemOutput.
type GetItemOutput struct {
	Item             map[string]*AttributeValue `json:"Item,omitempty"`
	ConsumedCapacity *ConsumedCapacity          `json:"ConsumedCapacity,omitempty"`
}

// QueryOutput mirrors DynamoDB QueryOutput.
type QueryOutput struct {
	Items            []map[string]*AttributeValue `json:"Items,omitempty"`
	Count            int32                        `json:"Count,omitempty"`
	ScannedCount     int32                        `json:"ScannedCount,omitempty"`
	LastEvaluatedKey map[string]*AttributeValue   `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity            `json:"ConsumedCapacity,omitempty"`
}

// ScanOutput mirrors DynamoDB ScanOutput.
type ScanOutput struct {
	Items            []map[string]*AttributeValue `json:"Items,omitempty"`
	Count            int32                        `json:"Count,omitempty"`
	ScannedCount     int32                        `json:"ScannedCount,omitempty"`
	LastEvaluatedKey map[string]*AttributeValue   `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity            `json:"ConsumedCapacity,omitempty"`
}

// BatchWriteItemOutput mirrors DynamoDB BatchWriteItemOutput.
type BatchWriteItemOutput struct {
	UnprocessedItems map[string][]WriteRequest `json:"UnprocessedItems,omitempty"`
	ConsumedCapacity []ConsumedCapacity        `json:"ConsumedCapacity,omitempty"`
}

// BatchGetItemOutput mirrors DynamoDB BatchGetItemOutput.
type BatchGetItemOutput struct {
	Responses        map[string][]map[string]*AttributeValue `json:"Responses,omitempty"`
	UnprocessedKeys  map[string]KeysAndAttributes            `json:"UnprocessedKeys,omitempty"`
	ConsumedCapacity []ConsumedCapacity                      `json:"ConsumedCapacity,omitempty"`
}

// TransactWriteItemsOutput mirrors DynamoDB TransactWriteItemsOutput.
type TransactWriteItemsOutput struct {
	ConsumedCapacity []ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
}

// ItemResponse mirrors DynamoDB ItemResponse.
type ItemResponse struct {
	Item map[string]*AttributeValue `json:"Item,omitempty"`
}

// TransactGetItemsOutput mirrors DynamoDB TransactGetItemsOutput.
type TransactGetItemsOutput struct {
	Responses        []ItemResponse     `json:"Responses,omitempty"`
	ConsumedCapacity []ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
}
