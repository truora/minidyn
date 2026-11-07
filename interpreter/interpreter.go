package interpreter

import (
	"errors"
	"fmt"

	"github.com/truora/minidyn/interpreter/language"
	"github.com/truora/minidyn/types"
)

var (
	// ErrSyntaxError when a syntax error is detected
	ErrSyntaxError = errors.New("Syntax error") //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors
	// ErrUnsupportedFeature when an expression or attribute type in not yet supported by the interpreter
	ErrUnsupportedFeature = errors.New("unsupported expression or attribute type")
)

// undeclaredAttributeNameError reports an expression that references an attribute name
// placeholder (#name) not declared in ExpressionAttributeNames. Its message mirrors
// DynamoDB's ValidationException wording; it unwraps to ErrSyntaxError so the same error
// gating that maps expression syntax errors to ValidationException also covers this case.
type undeclaredAttributeNameError struct {
	expressionType string
	name           string
}

func (e *undeclaredAttributeNameError) Error() string {
	//nolint:stylecheck,staticcheck,ST1005 // DynamoDB ValidationException wording (parity)
	return fmt.Sprintf(
		"Invalid %s: An expression attribute name used in the document path is not defined; attribute name: %s",
		e.expressionType, e.name,
	)
}

func (e *undeclaredAttributeNameError) Unwrap() error { return ErrSyntaxError }

// operandCountError reports a built-in function called with the wrong number of operands. Its
// message mirrors DynamoDB's ValidationException wording; like undeclaredAttributeNameError it
// unwraps to ErrSyntaxError so the same gating maps it to ValidationException.
type operandCountError struct {
	expressionType string
	cause          *language.OperandCountError
}

func (e *operandCountError) Error() string {
	return "Invalid " + e.expressionType + ": " + e.cause.Error()
}

func (e *operandCountError) Unwrap() error { return ErrSyntaxError }

// ValidateFunctionOperandCounts returns an error when the condition expression calls a built-in
// function with the wrong number of operands. DynamoDB rejects this before reading any item,
// so callers use it to fail even when no item would be evaluated. Expressions that don't parse
// are left to the evaluation that reports their syntax error. expressionType is the DynamoDB
// expression label used in the message (e.g. "FilterExpression").
func ValidateFunctionOperandCounts(expressionType, expression string) error {
	parsed := parseCondition(expression)
	if parsed.operandErr == nil {
		return nil
	}

	return &operandCountError{expressionType: expressionType, cause: parsed.operandErr}
}

// ValidateExpressionAttributeNamesDeclared returns an error when expression references an
// attribute name placeholder (#name) that is absent from aliases (ExpressionAttributeNames),
// matching DynamoDB which reports the first undeclared name. expressionType is the DynamoDB
// expression label used in the message (e.g. "UpdateExpression", "FilterExpression").
func ValidateExpressionAttributeNamesDeclared(expressionType, expression string, aliases map[string]string) error {
	undeclared := language.UndeclaredExpressionAttributeNames(expression, aliases)
	if len(undeclared) == 0 {
		return nil
	}

	return &undeclaredAttributeNameError{expressionType: expressionType, name: undeclared[0]}
}

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
	Item           map[string]*types.Item
	Attributes     map[string]*types.Item
	Aliases        map[string]string
}

// UpdateInput parameters to use Update function
type UpdateInput struct {
	TableName  string
	Expression string
	Item       map[string]*types.Item
	Attributes map[string]*types.Item
	Aliases    map[string]string
}

// Interpreter types expression interpreter interface
type Interpreter interface {
	Match(input MatchInput) (bool, error)
	Update(input UpdateInput) error
}
