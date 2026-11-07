package language

import "fmt"

// OperandCountError reports a built-in function called with the wrong number of operands.
// Its message is DynamoDB's, without the "Invalid <Expression>: " prefix, which depends on the
// expression the call appears in.
type OperandCountError struct {
	Function string
	Count    int
}

func (e *OperandCountError) Error() string {
	return fmt.Sprintf(
		"Incorrect number of operands for operator or function; operator or function: %s, number of operands: %d",
		e.Function, e.Count,
	)
}

// operandCountError returns the error for a call to fn with count operands, or nil when
// the count is right.
func operandCountError(fn *Function, count int) *OperandCountError {
	if count == fn.Arity {
		return nil
	}

	return &OperandCountError{Function: fn.Name, Count: count}
}

// FindOperandCountError returns the first call (in source order) to a built-in function with
// the wrong number of operands in the parsed condition or update expression, or nil. It only
// looks at the tree, so it works before any item is read.
func FindOperandCountError(n Node) *OperandCountError {
	switch node := n.(type) {
	case *ConditionalExpression:
		return findInExpression(node.Expression)
	case *UpdateStatement:
		return findInExpression(node.Expression)
	case Expression:
		return findInExpression(node)
	}

	return nil
}

func findInExpression(e Expression) *OperandCountError {
	if e == nil {
		return nil
	}

	switch node := e.(type) {
	case *CallExpression:
		return findInCall(node)
	case *PrefixExpression:
		return findInExpression(node.Right)
	case *InfixExpression:
		return findInExpressions(node.Left, node.Right)
	case *IndexExpression:
		return findInExpressions(node.Left, node.Index)
	case *BetweenExpression:
		return findInExpressions(node.Left, node.Range[0], node.Range[1])
	case *InExpression:
		return findInExpressions(append([]Expression{node.Left}, node.Range...)...)
	case *ActionExpression:
		return findInExpressions(node.Left, node.Right)
	case *UpdateExpression:
		return findInExpressions(node.Expressions...)
	}

	return nil
}

func findInExpressions(exprs ...Expression) *OperandCountError {
	for _, e := range exprs {
		if err := findInExpression(e); err != nil {
			return err
		}
	}

	return nil
}

func findInCall(call *CallExpression) *OperandCountError {
	if id, ok := call.Function.(*Identifier); ok {
		if fn, known := functions[id.Value]; known {
			if err := operandCountError(fn, len(call.Arguments)); err != nil {
				return err
			}
		}
	}

	return findInExpressions(call.Arguments...)
}
