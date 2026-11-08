package interpreter

import (
	"errors"
	"fmt"
	"slices"

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

// undeclaredAttributeValueError reports an expression that references an attribute value
// placeholder (:value) not declared in ExpressionAttributeValues. Like
// undeclaredAttributeNameError it unwraps to ErrSyntaxError.
type undeclaredAttributeValueError struct {
	expressionType string
	value          string
}

func (e *undeclaredAttributeValueError) Error() string {
	//nolint:stylecheck,staticcheck,ST1005 // DynamoDB ValidationException wording (parity)
	return fmt.Sprintf(
		"Invalid %s: An expression attribute value used in expression is not defined; attribute value: %s",
		e.expressionType, e.value,
	)
}

func (e *undeclaredAttributeValueError) Unwrap() error { return ErrSyntaxError }

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

// LabeledExpression is an expression with the DynamoDB label used in its error messages
// (e.g. "UpdateExpression", "FilterExpression").
type LabeledExpression struct {
	Label      string
	Expression string
}

// ValidateExpressionAttributeNamesDeclared returns an error when an expression references an
// attribute name placeholder (#name) that is absent from aliases (ExpressionAttributeNames),
// matching DynamoDB which reports the first undeclared name in the order given. Expressions
// are scanned once and cached.
func ValidateExpressionAttributeNamesDeclared(aliases map[string]string, exprs ...LabeledExpression) error {
	for _, e := range exprs {
		names, _ := exprPlaceholders(e.Expression)
		if err := validateDeclaredNames(e.Label, names, aliases); err != nil {
			return err
		}
	}

	return nil
}

// ValidateExpressionAttributeValuesDeclared returns an error when an expression references an
// attribute value placeholder (:value) that is absent from values (ExpressionAttributeValues),
// matching DynamoDB which reports the first undefined value in the order given. Expressions
// are scanned once and cached.
func ValidateExpressionAttributeValuesDeclared(values map[string]*types.Item, exprs ...LabeledExpression) error {
	for _, e := range exprs {
		_, names := exprPlaceholders(e.Expression)
		if err := validateDeclaredValues(e.Label, names, values); err != nil {
			return err
		}
	}

	return nil
}

// ValidateExpressionPlaceholders checks the placeholders of all of a request's expressions:
// an undeclared name in any of them is reported before an undefined value, as DynamoDB does.
func ValidateExpressionPlaceholders(aliases map[string]string, values map[string]*types.Item, exprs ...LabeledExpression) error {
	if err := ValidateExpressionAttributeNamesDeclared(aliases, exprs...); err != nil {
		return err
	}

	return ValidateExpressionAttributeValuesDeclared(values, exprs...)
}

// HasUndeclaredExpressionAttributeValues reports whether expression references a value
// placeholder that declared rejects. Front ends use it to let the undefined-value error win
// over the unused-value one, as DynamoDB does.
func HasUndeclaredExpressionAttributeValues(expression string, declared func(string) bool) bool {
	_, values := exprPlaceholders(expression)

	return slices.ContainsFunc(values, func(name string) bool { return !declared(name) })
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
