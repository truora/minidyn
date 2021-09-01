package language

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

// Eval runs the expression in the environment
func Eval(n Node, env *Environment) Object {
	switch node := n.(type) {
	case *ConditionalExpression:
		return Eval(node.Expression, env)
	case *UpdateExpression:
		return Eval(node.Expression, env)
	case *PrefixExpression:
		return evalPrefixExpression(node, env)
	case *InfixExpression:
		return evalInfixParts(node, env)
	case *IndexExpression:
		return evalIndex(node, env)
	case *BetweenExpression:
		return evalBetween(node, env)
	case *CallExpression:
		return evalFunctionCall(node, env)
	case *Identifier:
		return evalIdentifier(node, env)
	}

	return newError("unsupported expression: %s", n.String())
}

// EvalUpdate runs the update expression in the environment
func EvalUpdate(n Node, env *Environment) Object {
	switch node := n.(type) {
	case *UpdateExpression:
		return EvalUpdate(node.Expression, env)
	case *SetExpression:
		return evalSetExpression(node, env)
	case *InfixExpression:
		return evalInfixUpdate(node, env)
	case *CallExpression:
		return evalUpdateFunctionCall(node, env)
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

func evalIndex(node *IndexExpression, env *Environment) Object {
	positions, l, errObj := evalIndexPositions(node, env)
	if isError(errObj) {
		return errObj
	}

	var obj Object = l
	for _, pos := range positions {
		obj = pos.Get(obj)
		if isError(obj) {
			return obj
		}
	}

	return obj
}

type indexAccessor struct {
	kind     ObjectType
	val      interface{}
	operator Token
}

func (i indexAccessor) Get(container Object) Object {
	switch c := container.(type) {
	case *List:
		pos, ok := i.val.(int64)
		if i.kind == ObjectTypeList && ok {
			return c.Value[pos]
		}
	case *Map:
		pos, ok := i.val.(string)
		if i.kind == ObjectTypeMap && ok {
			return nativeNilToNullObject(c.Value[pos])
		}
	}

	return newError("index operator %s not supporter: %q", i.operator.Literal, container.Type())
}

func (i indexAccessor) Set(container, val Object) Object {
	switch c := container.(type) {
	case *List:
		pos, ok := i.val.(int64)
		if i.kind == ObjectTypeList && ok {
			if int64(len(c.Value)) > pos {
				c.Value[pos] = val

				return NULL
			}

			c.Value = append(c.Value, val)
		}
	case *Map:
		pos, ok := i.val.(string)
		if i.kind == ObjectTypeMap && ok {
			c.Value[pos] = val
		}

		return NULL
	}

	return newError("index operator not supporter: %q", container.Type())
}

func evalIndexPositions(n Expression, env *Environment) ([]indexAccessor, Object, Object) {
	positions := []indexAccessor{}

	for {
		switch node := n.(type) {
		case *Identifier:
			obj, errObj := evalIndexObj(n, env)
			if isError(errObj) {
				return nil, nil, errObj
			}

			return positions, obj, nil
		case *IndexExpression:
			identifier, ok := node.Index.(*Identifier)
			if !ok {
				return nil, nil, newError("identifier expected: got %q", identifier.String())
			}

			pos, errObj := evalIndexValue(identifier, node, env)
			if isError(errObj) {
				return positions, nil, errObj
			}

			positions = append(positions, pos)

			n = node.Left
		default:
			return nil, nil, newError("index operator not supported: got %q", n.String())
		}
	}
}

func evalIndexObj(identifierExpression Expression, env *Environment) (Object, Object) {
	identifier, ok := identifierExpression.(*Identifier)
	if !ok {
		return nil, newError("identifier expected: got %q", identifierExpression.String())
	}

	obj := evalIdentifier(identifier, env)
	switch obj.(type) {
	case *List:
		return obj, nil
	case *Map:
		return obj, nil
	case *Error:
		return nil, obj
	}

	return nil, newError("index operator not supported for %q", obj.Type())
}

func evalIndexValue(node *Identifier, indexNode *IndexExpression, env *Environment) (indexAccessor, Object) {
	switch indexNode.Type {
	case ObjectTypeList:
		pos, errObj := evalListIndexValue(node, env)
		if isError(errObj) {
			return indexAccessor{}, errObj
		}

		return indexAccessor{val: pos, kind: indexNode.Type, operator: indexNode.Token}, NULL
	case ObjectTypeMap:
		pos, errObj := evalMapIndexValue(node, env)
		if isError(errObj) {
			return indexAccessor{}, errObj
		}

		return indexAccessor{val: pos, kind: indexNode.Type, operator: indexNode.Token}, NULL
	}

	return indexAccessor{}, newError("index operator not supported: got %q", node.String())
}

func evalListIndexValue(node *Identifier, env *Environment) (int64, Object) {
	if n, err := strconv.Atoi(node.Token.Literal); err == nil {
		return int64(n), nil
	}

	obj := evalIdentifier(node, env)
	if isError(obj) {
		return 0, obj
	}

	number, ok := obj.(*Number)
	if !ok {
		return 0, newError("access index with [] only support N as index : got %q", obj.Type())
	}

	return int64(number.Value), nil
}

func evalMapIndexValue(node *Identifier, env *Environment) (string, Object) {
	obj := evalIdentifier(node, env)
	if isError(obj) {
		return "", obj
	}

	str, ok := obj.(*String)
	if !ok {
		return node.Token.Literal, nil
	}

	return str.Value, nil
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

	funcObj, ok := fn.(*Function)
	if !ok {
		return newError("invalid function call; expression: " + node.String())
	}

	if funcObj.ForUpdate {
		return newError("the function is not allowed in an condition expression; function: " + funcObj.Name)
	}

	args := evalExpressions(node.Arguments, env)
	if len(args) == 1 && isError(args[0]) {
		return args[0]
	}

	return fn.(*Function).Value(args...)
}

func evalUpdateFunctionCall(node *CallExpression, env *Environment) Object {
	fn := evalFunctionCallIdentifer(node, env)
	if isError(fn) {
		return fn
	}

	funcObj, ok := fn.(*Function)
	if !ok {
		return newError("invalid function call; expression: " + node.String())
	}

	if !funcObj.ForUpdate {
		return newError("the function is not allowed in an update expression; function: " + funcObj.Name)
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
		return newError("invalid function name; function: " + name)
	}

	return fn
}

func evalSetExpression(node *SetExpression, env *Environment) Object {
	if len(node.Expressions) == 0 {
		return newError("SET expression must have at least one action")
	}

	for _, act := range node.Expressions {
		infix, ok := act.(*InfixExpression)
		if !ok {
			return newError("invalid infix action")
		}

		result := EvalUpdate(infix, env)
		if isError(result) {
			return result
		}
	}

	return NULL
}

func evalInfixUpdate(node *InfixExpression, env *Environment) Object {
	switch node.Operator {
	case "=":
		val := EvalUpdate(node.Right, env)
		if isError(val) {
			return val
		}

		id, ok := node.Left.(*Identifier)
		if ok {
			env.Set(id.Value, val)
			return NULL
		}

		indexField, ok := node.Left.(*IndexExpression)
		if ok {
			errObj := evalAssignIndex(indexField, []int{}, val, env)
			if isError(errObj) {
				return errObj
			}

			return NULL
		}

		return newError("invalid assignation to: %s", node.String())
	case "+":
		augend, addend, errObj := evalArithmeticTerms(node, env)
		if isError(errObj) {
			return errObj
		}

		return &Number{Value: augend.Value + addend.Value}
	case "-":
		minuend, subtrahend, errObj := evalArithmeticTerms(node, env)
		if isError(errObj) {
			return errObj
		}

		return &Number{Value: minuend.Value - subtrahend.Value}
	}

	return newError("unknown operator: %s", node.Operator)
}

func evalAssignIndex(n Expression, i []int, val Object, env *Environment) Object {
	positions, l, errObj := evalIndexPositions(n, env)
	if isError(errObj) {
		return errObj
	}

	var obj Object = l

	for _, pos := range positions[:len(positions)-1] {
		l, ok := obj.(*List)
		if !ok {
			newError("index operator not supporter: %q", obj.Type())
		}

		obj = pos.Get(l)
	}

	pos := positions[len(positions)-1]
	pos.Set(obj, val)

	return NULL
}

func evalArithmeticTerms(node *InfixExpression, env *Environment) (*Number, *Number, Object) {
	leftTerm := EvalUpdate(node.Left, env)
	if isError(leftTerm) {
		return nil, nil, leftTerm
	}

	rightTerm := EvalUpdate(node.Right, env)
	if isError(rightTerm) {
		return nil, nil, rightTerm
	}

	leftNumber, ok := leftTerm.(*Number)
	if !ok {
		return nil, nil, newError("invalid operation: %s %s %s", leftTerm.Type(), node.Operator, rightTerm.Type())
	}

	rightNumber, ok := rightTerm.(*Number)
	if !ok {
		return nil, nil, newError("invalid operation: %s %s %s", leftTerm.Type(), node.Operator, rightTerm.Type())
	}

	return leftNumber, rightNumber, NULL
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
