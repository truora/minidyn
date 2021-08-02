package language

import (
	"bytes"
	"fmt"
	"reflect"
)

// Eval runs the expression in the environment
func Eval(n Node, env *Environment) Object {
	switch node := n.(type) {
	case *DynamoExpression:
		return Eval(node.Statement, env)
	case *ExpressionStatement:
		return Eval(node.Expression, env)
	case *PrefixExpression:
		return evalPrefixExpression(node, env)
	case *InfixExpression:
		return evalInfixParts(node, env)
	case *BetweenExpression:
		return evalBetween(node, env)
	case *CallExpression:
		return evalFunctionCall(node, env)
	case *Identifier:
		return evalIdentifier(node, env)
	}

	return newError("unsupported expression: %s", n.String())
}

func newError(format string, a ...interface{}) *Error {
	return &Error{Message: fmt.Sprintf(format, a...)}
}

func isError(obj Object) bool {
	if obj != nil {
		return obj.Type() == ObjectTypeError
	}

	return false
}

func isNumber(obj Object) bool {
	if obj != nil {
		return obj.Type() == ObjectTypeNumber
	}

	return false
}

func isString(obj Object) bool {
	if obj != nil {
		return obj.Type() == ObjectTypeString
	}

	return false
}

func isUndefined(obj Object) bool {
	return obj == nil || obj == NULL
}

func isComparable(obj Object) bool {
	return comparableTypes[obj.Type()] || isUndefined(obj)
}

func matchTypes(typ ObjectType, objs ...Object) bool {
	for _, o := range objs {
		if typ != o.Type() {
			return false
		}
	}

	return true
}

func evalPrefixExpression(node *PrefixExpression, env *Environment) Object {
	right := Eval(node.Right, env)
	if isError(right) {
		return right
	}

	if node.Operator == NOT {
		return evalBangOperatorExpression(right)
	}

	return newError("unknown operator: %s %s", node.Operator, right.Type())
}

func evalBangOperatorExpression(right Object) Object {
	switch right {
	case TRUE:
		return FALSE
	case FALSE:
		return TRUE
	default:
		return newError("unknown operator: NOT %s", right.Type())
	}
}

func evalInfixParts(node *InfixExpression, env *Environment) Object {
	left := Eval(node.Left, env)
	if isError(left) {
		return left
	}

	right := Eval(node.Right, env)
	if isError(right) {
		return right
	}

	return evalInfixExpression(node.Operator, left, right)
}

func evalInfixExpression(operator string, left, right Object) Object {
	switch {
	case isComparable(left) && isComparable(right):
		return evalComparableInfixExpression(operator, left, right)
	case matchTypes(ObjectTypeBoolean, left, right):
		return evalBooleanInfixExpression(operator, left, right)
	case matchTypes(ObjectTypeNull, left, right):
		return evalNullInfixExpression(operator, left, right)
	case operator == "=":
		return nativeBoolToBooleanObject(equalObject(left, right))
	case operator == "<>":
		return nativeBoolToBooleanObject(!equalObject(left, right))
	case !matchTypes(left.Type(), left, right):
		return newError("type mismatch: %s %s %s", left.Type(), operator, right.Type())
	default:
		return newError("unknown operator: %s %s %s", left.Type(), operator, right.Type())
	}
}

func evalNullInfixExpression(operator string, left, right Object) Object {
	switch operator {
	case "=":
		if isUndefined(left) || isUndefined(right) {
			return FALSE
		}

		return TRUE
	case "<>":
		if isUndefined(left) || isUndefined(right) {
			return TRUE
		}

		return FALSE
	}

	return FALSE
}

func evalComparableInfixExpression(operator string, left, right Object) Object {
	if isUndefined(left) || isUndefined(right) {
		return FALSE
	}

	switch left.Type() {
	case ObjectTypeNumber:
		return evalNumberInfixExpression(operator, left, right)
	case ObjectTypeString:
		return evalStringInfixExpression(operator, left, right)
	case ObjectTypeBinary:
		return evalBinaryInfixExpression(operator, left, right)
	}

	return newError("comparing types are not supported: %s %s %s", left.Type(), operator, right.Type())
}

func evalNumberInfixExpression(operator string, left, right Object) Object {
	leftVal := left.(*Number).Value
	rightVal := right.(*Number).Value

	switch operator {
	case "<":
		return nativeBoolToBooleanObject(leftVal < rightVal)
	case "<=":
		return nativeBoolToBooleanObject(leftVal <= rightVal)
	case ">":
		return nativeBoolToBooleanObject(leftVal > rightVal)
	case ">=":
		return nativeBoolToBooleanObject(leftVal >= rightVal)
	case "=":
		return nativeBoolToBooleanObject(leftVal == rightVal)
	case "<>":
		return nativeBoolToBooleanObject(leftVal != rightVal)
	default:
		return newError("unknown operator: %s %s %s", left.Type(), operator, right.Type())
	}
}

func evalStringInfixExpression(operator string, left, right Object) Object {
	leftVal := left.(*String).Value
	rightVal := right.(*String).Value

	switch operator {
	case "<":
		return nativeBoolToBooleanObject(leftVal < rightVal)
	case "<=":
		return nativeBoolToBooleanObject(leftVal <= rightVal)
	case ">":
		return nativeBoolToBooleanObject(leftVal > rightVal)
	case ">=":
		return nativeBoolToBooleanObject(leftVal >= rightVal)
	case "=":
		return nativeBoolToBooleanObject(leftVal == rightVal)
	case "<>":
		return nativeBoolToBooleanObject(leftVal != rightVal)
	default:
		return newError("unknown operator: %s %s %s", left.Type(), operator, right.Type())
	}
}

func evalBinaryInfixExpression(operator string, left, right Object) Object {
	leftVal := left.(*Binary).Value
	rightVal := right.(*Binary).Value

	switch operator {
	case "<":
		return nativeBoolToBooleanObject(bytes.Compare(leftVal, rightVal) < 0)
	case "<=":
		return nativeBoolToBooleanObject(bytes.Compare(leftVal, rightVal) <= 0)
	case ">":
		return nativeBoolToBooleanObject(bytes.Compare(leftVal, rightVal) > 0)
	case ">=":
		return nativeBoolToBooleanObject(bytes.Compare(leftVal, rightVal) >= 0)
	case "=":
		return nativeBoolToBooleanObject(bytes.Equal(leftVal, rightVal))
	case "<>":
		return nativeBoolToBooleanObject(!bytes.Equal(leftVal, rightVal))
	default:
		return newError("unknown operator: %s %s %s", left.Type(), operator, right.Type())
	}
}

func evalBooleanInfixExpression(operator string, left, right Object) Object {
	if isUndefined(left) || isUndefined(right) {
		return FALSE
	}

	leftVal := left.(*Boolean).Value
	rightVal := right.(*Boolean).Value

	switch operator {
	case "AND":
		return nativeBoolToBooleanObject(leftVal && rightVal)
	case "OR":
		return nativeBoolToBooleanObject(leftVal || rightVal)
	case "=":
		return nativeBoolToBooleanObject(left == right)
	case "<>":
		return nativeBoolToBooleanObject(left != right)
	default:
		return newError("unknown operator: %s %s %s", left.Type(), operator, right.Type())
	}
}

func equalObject(left, right Object) bool {
	if !matchTypes(left.Type(), left, right) {
		return false
	}

	return reflect.DeepEqual(left, right)
}

func evalIdentifier(node *Identifier, env *Environment) Object {
	val, ok := env.Get(node.Value)
	if !ok {
		return NULL
	}

	return val
}

func evalBetween(node *BetweenExpression, env *Environment) Object {
	val := evalBetweenOperand(node.Left, env)
	if isError(val) {
		return val
	}

	min := evalBetweenOperand(node.Range[0], env)
	if isError(min) {
		return min
	}

	max := evalBetweenOperand(node.Range[1], env)
	if isError(max) {
		return max
	}

	if isUndefined(val) || isUndefined(min) || isUndefined(max) {
		return FALSE
	}

	if !matchTypes(val.Type(), val, min, max) {
		return newError("mismatch type: BETWEEN operands must have the same type")
	}

	b := compareRange(val, min, max)

	return b
}

func compareRange(value, min, max Object) Object {
	switch val := value.(type) {
	case *Number:
		left := evalNumberInfixExpression("<=", min, val)
		right := evalNumberInfixExpression("<=", val, max)

		return evalBooleanInfixExpression("AND", left, right)
	case *String:
		left := evalStringInfixExpression("<=", min, val)
		right := evalStringInfixExpression("<=", val, max)

		return evalBooleanInfixExpression("AND", left, right)
	case *Binary:
		left := evalBinaryInfixExpression("<=", min, val)
		right := evalBinaryInfixExpression("<=", val, max)

		return evalBooleanInfixExpression("AND", left, right)
	}

	return newError("unsupported type: between do not support comparing %s", value.Type())
}

func evalBetweenOperand(exp Expression, env *Environment) Object {
	identifier, ok := exp.(*Identifier)
	if !ok {
		return newError("identifier expected: got %q", exp.String())
	}

	val := evalIdentifier(identifier, env)
	if !comparableTypes[val.Type()] && !isUndefined(val) {
		return newError("unexpected type: %q should be a comparable type(N,S,B) got %q", exp.String(), val.Type())
	}

	return val
}

func evalFunctionCall(node *CallExpression, env *Environment) Object {
	fn := evalFunctionCallIdentifer(node, env)
	if isError(fn) {
		return fn
	}

	args := evalExpressions(node.Arguments, env)
	if len(args) == 1 && isError(args[0]) {
		return args[0]
	}

	return fn.(*Function).Value(args...)
}

func evalFunctionCallIdentifer(node *CallExpression, env *Environment) Object {
	functionIdentifier, ok := node.Function.(*Identifier)
	if !ok {
		return newError("bad function syntax")
	}

	name := functionIdentifier.Value

	fn, ok := functions[name]
	if !ok {
		return newError("function not found: " + name)
	}

	return fn
}

func evalExpressions(exps []Expression, env *Environment) []Object {
	var result []Object

	for _, e := range exps {
		evaluated := Eval(e, env)
		if isError(evaluated) {
			return []Object{evaluated}
		}

		result = append(result, evaluated)
	}

	return result
}
