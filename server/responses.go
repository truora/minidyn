package server

// Minimal output shapes encoded back to the client.
// They mirror DynamoDB JSON responses closely enough for SDK decoding.

// CreateTableOutput mirrors DynamoDB CreateTableOutput.
type CreateTableOutput struct {
	TableDescription interface{} `json:"TableDescription,omitempty"`
}

// DeleteTableOutput mirrors DynamoDB DeleteTableOutput.
type DeleteTableOutput struct {
	TableDescription interface{} `json:"TableDescription,omitempty"`
}

// UpdateTableOutput mirrors DynamoDB UpdateTableOutput.
type UpdateTableOutput struct {
	TableDescription interface{} `json:"TableDescription,omitempty"`
}

// DescribeTableOutput mirrors DynamoDB DescribeTableOutput.
type DescribeTableOutput struct {
	Table interface{} `json:"Table,omitempty"`
}

// PutItemOutput mirrors DynamoDB PutItemOutput.
type PutItemOutput struct {
	Attributes map[string]*AttributeValue `json:"Attributes,omitempty"`
}

// DeleteItemOutput mirrors DynamoDB DeleteItemOutput.
type DeleteItemOutput struct {
	Attributes map[string]*AttributeValue `json:"Attributes,omitempty"`
}

// UpdateItemOutput mirrors DynamoDB UpdateItemOutput.
type UpdateItemOutput struct {
	Attributes map[string]*AttributeValue `json:"Attributes,omitempty"`
}

// GetItemOutput mirrors DynamoDB GetItemOutput.
type GetItemOutput struct {
	Item map[string]*AttributeValue `json:"Item,omitempty"`
}

// QueryOutput mirrors DynamoDB QueryOutput.
type QueryOutput struct {
	Items            []map[string]*AttributeValue `json:"Items,omitempty"`
	Count            int32                        `json:"Count,omitempty"`
	ScannedCount     int32                        `json:"ScannedCount,omitempty"`
	LastEvaluatedKey map[string]*AttributeValue   `json:"LastEvaluatedKey,omitempty"`
}

// ScanOutput mirrors DynamoDB ScanOutput.
type ScanOutput struct {
	Items            []map[string]*AttributeValue `json:"Items,omitempty"`
	Count            int32                        `json:"Count,omitempty"`
	ScannedCount     int32                        `json:"ScannedCount,omitempty"`
	LastEvaluatedKey map[string]*AttributeValue   `json:"LastEvaluatedKey,omitempty"`
}

// BatchWriteItemOutput mirrors DynamoDB BatchWriteItemOutput.
type BatchWriteItemOutput struct {
	UnprocessedItems map[string][]WriteRequest `json:"UnprocessedItems,omitempty"`
}
