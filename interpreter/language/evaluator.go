package language

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var (
	syntaxErrorTemplate = "syntax error; token: %s" // TODO: See how to add ", near: %s" to the error
)

// Eval runs the expression in the environment
func Eval(n Node, env *Environment) Object {
	switch node := n.(type) {
	case *ConditionalExpression:
		return evalConditional(node, env)
	case *PrefixExpression:
		return evalPrefixExpression(node, env)
	case *InfixExpression:
		return evalInfixParts(node, env)
	case *IndexExpression:
		return evalIndex(node, env)
	case *BetweenExpression:
		return evalBetween(node, env)
	case *InExpression:
		return evalIn(node, env)
	case *CallExpression:
		return evalFunctionCall(node, env)
	case *Identifier:
		return evalIdentifier(node, env, true)
	}

	return newError("unsupported expression: %s", n.String())
}

// EvalUpdate runs the update expression in the environment
func EvalUpdate(n Node, env *Environment) Object {
	switch node := n.(type) {
	case *UpdateStatement:
		ue, ok := node.Expression.(*UpdateExpression)
		if !ok {
			return newError("invalid update expression: %s", n.String())
		}

		return evalUpdateExpression(ue, env)
	case *InfixExpression:
		return evalInfixUpdate(node, env)
	case *IndexExpression:
		return evalIndex(node, env)
	case *CallExpression:
		return evalUpdateFunctionCall(node, env)
	case *Identifier:
		return evalIdentifier(node, env, true)
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

func isExpressionIdentifier(expr Expression) bool {
	_, ok := expr.(*Identifier)

	return ok
}

func matchTypes(typ ObjectType, objs ...Object) bool {
	for _, o := range objs {
		if typ != o.Type() {
			return false
		}
	}

	return true
}

func evalConditional(n *ConditionalExpression, env *Environment) Object {
	obj := Eval(n.Expression, env)
	if isError(obj) {
		return obj
	}

	if obj.Type() != ObjectTypeBoolean {
		return newError("a condition must evaluate to a BOOL, %q evaluates to %q", n.String(), obj.Type())
	}

	return obj
}

func evalPrefixExpression(node *PrefixExpression, env *Environment) Object {
	if isExpressionIdentifier(node.Right) {
		return newError(syntaxErrorTemplate, node.Right.String())
	}

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
	syntaxObj := checkSyntaxInfixParts(node)
	if isError(syntaxObj) {
		return syntaxObj
	}

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

func checkSyntaxInfixParts(node *InfixExpression) Object {
	isLeftIdentifier := isExpressionIdentifier(node.Left)
	isRightIdentifier := isExpressionIdentifier(node.Right)
	_, operatorIsKeyword := keywords[node.Operator]

	expr := node.Right

	if isLeftIdentifier {
		expr = node.Left
	}

	if operatorIsKeyword && (isLeftIdentifier || isRightIdentifier) {
		return newError(syntaxErrorTemplate, expr.String())
	}

	return nil
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
	oneSideIsUndefined := isUndefined(left) || isUndefined(right)

	if oneSideIsUndefined {
		return evalNullInfixExpression(operator, left, right)
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

func evalIdentifier(node *Identifier, env *Environment, toplevel bool) Object {
	attributeName := strings.ToUpper(node.Token.Literal)

	if toplevel && IsReservedWord(attributeName) {
		return newError(fmt.Sprintf("reserved word %s found in expression", attributeName))
	}

	val := env.Get(node.Value)
	if isError(val) {
		return val
	}

	return val
}

func evalIndex(node *IndexExpression, env *Environment) Object {
	positions, o, errObj := evalIndexPositions(node, env)
	if isError(errObj) {
		return errObj
	}

	var obj Object = o

	for i := len(positions) - 1; i >= 0; i-- {
		pos := positions[i]

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
			return c.Get(pos)
		}
	case *Map:
		pos, ok := i.val.(string)
		if i.kind == ObjectTypeMap && ok {
			return c.Get(pos)
		}
	}

	if isUndefined(container) {
		return container
	}

	return newError("index operator %s not supporter: %q", i.operator.Literal, container.Type())
}

func setListValue(list *List, value Object, index int64) Object {
	if int64(len(list.Value)) > index {
		list.Value[index] = value

		return NULL
	}

	list.Value = append(list.Value, value)

	return NULL
}

func (i indexAccessor) Set(container, val Object) Object {
	switch c := container.(type) {
	case *List:
		pos, ok := i.val.(int64)
		if i.kind == ObjectTypeList && ok {
			return setListValue(c, val, pos)
		}
	case *Map:
		pos, ok := i.val.(string)

		if i.kind == ObjectTypeMap && ok {
			c.Value[pos] = val
		}

		return NULL
	}

	return newError("index assignation for %q type is not supported", container.Type())
}

func handleIndexIdentifier(n Expression, env *Environment, positions []indexAccessor) ([]indexAccessor, Object, Object) {
	obj, errObj := evalIndexObj(n, env)
	if isError(errObj) {
		return nil, nil, errObj
	}

	return positions, obj, nil
}

func (i indexAccessor) Remove(container Object) Object {
	switch c := container.(type) {
	case *List:
		pos, ok := i.val.(int64)
		if i.kind == ObjectTypeList && ok {
			return c.Remove(pos)
		}
	case *Map:
		pos, ok := i.val.(string)

		if i.kind == ObjectTypeMap && ok {
			delete(c.Value, pos)
		}

		return NULL
	}

	return newError("index removal for %q type is not supported", container.Type())
}

func evalIndexPositions(n Expression, env *Environment) ([]indexAccessor, Object, Object) {
	positions := []indexAccessor{}
	exp := n

	for {
		switch node := exp.(type) {
		case *Identifier:
			return handleIndexIdentifier(exp, env, positions)
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

			exp = node.Left
		default:
			return nil, nil, newError("index operator not supported: got %q", exp.String())
		}
	}
}

func evalIndexObj(identifierExpression Expression, env *Environment) (Object, Object) {
	identifier, ok := identifierExpression.(*Identifier)
	if !ok {
		return nil, newError("identifier expected: got %q", identifierExpression.String())
	}

	obj := evalIdentifier(identifier, env, true)
	switch obj.(type) {
	case *List:
		return obj, nil
	case *Map:
		return obj, nil
	case *Error:
		return nil, obj
	}

	if isUndefined(obj) {
		return obj, nil
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

	obj := evalIdentifier(node, env, true)
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
	obj := evalIdentifier(node, env, false)
	if isError(obj) {
		return "", obj
	}

	str, ok := obj.(*String)
	if !ok {
		name := node.Token.Literal
		if alias, ok := env.Aliases[name]; ok {
			name = alias
		}

		return name, nil
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

func evalIn(node *InExpression, env *Environment) Object {
	val := evalIdentifierOperand(node.Left, env)
	if isError(val) {
		return val
	}

	rangeObjects := &List{Value: make([]Object, 0, len(node.Range))}

	for _, exp := range node.Range {
		obj := evalIdentifierOperand(exp, env)
		if isError(obj) {
			return obj
		}

		if val.Type() != obj.Type() {
			continue
		}

		rangeObjects.Value = append(rangeObjects.Value, obj)
	}

	if isUndefined(val) {
		return FALSE
	}

	b := rangeObjects.Contains(val)

	return nativeBoolToBooleanObject(b)
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

	val := evalIdentifier(identifier, env, true)
	if val.Type() == ObjectTypeError {
		return val
	}

	if !comparableTypes[val.Type()] && !isUndefined(val) {
		return newError("unexpected type: %q should be a comparable type(N,S,B) got %q", exp.String(), val.Type())
	}

	return val
}

func evalIdentifierOperand(exp Expression, env *Environment) Object {
	identifier, ok := exp.(*Identifier)
	if !ok {
		return newError("identifier expected: got %q", exp.String())
	}

	val := evalIdentifier(identifier, env, true)
	if val.Type() == ObjectTypeError {
		return val
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

func evalUpdateExpression(node *UpdateExpression, env *Environment) Object {
	if len(node.Expressions) == 0 {
		return newError(node.TokenLiteral() + " expression must have at least one action")
	}

	for _, act := range node.Expressions {
		action, ok := act.(*ActionExpression)
		if !ok {
			return newError("invalid infix action")
		}

		result := evalAction(action, env)
		if isError(result) {
			return result
		}
	}

	env.Compact()

	return NULL
}

func evalActionSet(node *ActionExpression, env *Environment) Object {
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
}

func evalActionAdd(node *ActionExpression, env *Environment) Object {
	val := EvalUpdate(node.Right, env)
	if isError(val) {
		return val
	}

	id, ok := node.Left.(*Identifier)
	if ok {
		obj := env.Get(id.Value)

		if obj == NULL {
			env.Set(id.Value, val)
			return obj
		}

		addObj, ok := obj.(AppendableObject)
		if !ok {
			return newError("an operand in the update expression has an incorrect data type")
		}

		return addObj.Add(val)
	}

	return NULL
}

func evalActionDelete(node *ActionExpression, env *Environment) Object {
	val := EvalUpdate(node.Right, env)
	if isError(val) {
		return val
	}

	id, ok := node.Left.(*Identifier)
	if ok {
		obj := env.Get(id.Value)

		if obj == NULL {
			env.Set(id.Value, val)
			return obj
		}

		addObj, ok := obj.(DetachableObject)
		if !ok {
			return newError("an operand in the update expression has an incorrect data type")
		}

		return addObj.Delete(val)
	}

	return NULL
}

func evalActionRemove(node *ActionExpression, env *Environment) Object {
	id, ok := node.Left.(*Identifier)
	if ok {
		env.Remove(id.Value)

		return NULL
	}

	indexField, ok := node.Left.(*IndexExpression)
	if ok {
		positions, o, errObj := evalIndexPositions(indexField, env)
		if isError(errObj) {
			return errObj
		}

		var (
			obj Object = o
			pos indexAccessor
		)

		for i := len(positions) - 1; i > 0; i-- {
			pos = positions[i]
			obj = pos.Get(obj)
		}

		errObj = positions[0].Remove(obj)
		if isError(errObj) {
			return errObj
		}

		env.MarkToCompact(obj)

		return NULL
	}

	return newError("invalid remove to: %s", node.String())
}

func evalAction(node *ActionExpression, env *Environment) Object {
	switch node.Token.Type {
	case SET:
		return evalActionSet(node, env)
	case ADD:
		return evalActionAdd(node, env)
	case REMOVE:
		return evalActionRemove(node, env)
	case DELETE:
		return evalActionDelete(node, env)
	}

	return newError("unknown update action type: %s", node.Token.Literal)
}

func evalInfixUpdate(node *InfixExpression, env *Environment) Object {
	switch node.Operator {
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
	positions, o, errObj := evalIndexPositions(n, env)
	if isError(errObj) {
		return errObj
	}

	var obj Object = o

	for idx := len(positions) - 1; idx >= 0; idx-- {
		pos := positions[idx]
		if idx == 0 {
			errObj := pos.Set(obj, val)
			if isError(errObj) {
				return errObj
			}

			break
		}

		obj = pos.Get(obj)
	}

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
