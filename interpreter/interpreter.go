package interpreter

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	// ErrSyntaxError when a syntax error is detected
	ErrSyntaxError = errors.New("syntax error")
	// ErrUnsupportedFeature when an expression or attribute type in not yet supported by the interpreter
	ErrUnsupportedFeature = errors.New("unsupported expression or attribute type")
)

// ExpressionType type of the evaluated expression
type ExpressionType string

const (
	// ExpressionTypeKey expression used to evalute primary key values
	ExpressionTypeKey ExpressionType = "key"
	// ExpressionTypeFilter expression used to filter items
	ExpressionTypeFilter ExpressionType = "filter"
	// ExpressionTypeConditional expression used for conditional writes
	ExpressionTypeConditional ExpressionType = "conditional"
)

// MatchInput parameters to use match function
type MatchInput struct {
	TableName      string
	Expression     string
	ExpressionType ExpressionType
	Item           map[string]*dynamodb.AttributeValue
	Attributes     map[string]*dynamodb.AttributeValue
	Aliases        map[string]*string
}

// UpdateInput parameters to use Update function
type UpdateInput struct {
	TableName  string
	Expression string
	Item       map[string]*dynamodb.AttributeValue
	Attributes map[string]*dynamodb.AttributeValue
}

// Interpreter dynamodb expression interpreter interface
type Interpreter interface {
	Match(input MatchInput) (bool, error)
	Update(input UpdateInput) error
}
