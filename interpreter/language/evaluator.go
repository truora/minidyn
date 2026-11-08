package language

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/truora/minidyn/types"
)

var syntaxErrorTemplate = "syntax error; token: %s" // TODO: See how to add ", near: %s" to the error

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

const (
	// missingOperandMessage is DynamoDB's error for an update expression that reads a path
	// that does not resolve in the item.
	missingOperandMessage = "The provided expression refers to an attribute that does not exist in the item"
	// operandTypeMessage is DynamoDB's error for an update expression operand of the wrong
	// type, found in the item.
	operandTypeMessage = "An operand in the update expression has an incorrect data type"
)

const invalidDocumentPathMessage = "The document path provided in the update expression is invalid for update"

func newError(format string, a ...any) *Error {
	return &Error{Message: fmt.Sprintf(format, a...)}
}

// newMissingOperandError is DynamoDB's error for an update operand that does not resolve,
// with minidyn detail naming it.
func newMissingOperandError(detail ...string) *Error {
	return &Error{Message: types.WithDetail(missingOperandMessage, detail...)}
}

// newOperandTypeError is DynamoDB's error for an update operand of the wrong type read from
// the item, with minidyn detail naming it and its type.
func newOperandTypeError(operand Object, detail ...string) *Error {
	return &Error{Message: types.WithDetail(operandTypeMessage, append(detail, "operand type", string(operand.Type()))...)}
}

func newInvalidDocumentPathError(fieldID, path string) *Error {
	return &Error{Message: types.WithDetail(invalidDocumentPathMessage, "field id", fieldID, "path", path)}
}

// newInvalidFunctionCallError is minidyn's message for a call whose callee is not a function;
// DynamoDB has no counterpart, so the expression is minidyn-only detail.
func newInvalidFunctionCallError(node *CallExpression) *Error {
	return &Error{Message: types.WithDetail("invalid function call", "expression", node.String())}
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
	if obj == nil {
		return true
	}

	nullObj, ok := obj.(*Null)
	if !ok {
		return false
	}

	return nullObj.IsUndefined
}

// IsUndefinedObject reports whether obj is the undefined projection / null sentinel.
func IsUndefinedObject(obj Object) bool {
	return isUndefined(obj)
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

	if errObj := staticInfixError(node, left, right); errObj != nil {
		return errObj
	}

	return evalInfixExpression(node.Operator, left, right)
}

// isValueOperand reports whether expr is an expression attribute value (:name). Its type is
// known before reading any item, unlike the type of a document path.
func isValueOperand(expr Expression) bool {
	id, ok := expr.(*Identifier)

	return ok && strings.HasPrefix(id.Value, ":")
}

func isOrderingOperator(operator string) bool {
	switch operator {
	case "<", "<=", ">", ">=":
		return true
	}

	return false
}

// staticInfixError returns the error for an ordering comparison DynamoDB rejects before
// reading any item: a value that is not N, S or B, or two values of different types. A
// stored value of another type is not an error, the comparison is just false.
func staticInfixError(node *InfixExpression, left, right Object) Object {
	if !isOrderingOperator(node.Operator) {
		return nil
	}

	leftIsValue, rightIsValue := isValueOperand(node.Left), isValueOperand(node.Right)
	bothDefined := !isUndefined(left) && !isUndefined(right)

	// A value that cannot be ordered is rejected even when the other side is a missing path.
	invalid := (leftIsValue && !isUndefined(left) && !comparableTypes[left.Type()]) ||
		(rightIsValue && !isUndefined(right) && !comparableTypes[right.Type()]) ||
		(bothDefined && leftIsValue && rightIsValue && left.Type() != right.Type())
	if !invalid {
		return nil
	}

	if left.Type() != right.Type() {
		return newError("type mismatch: %s %s %s", left.Type(), node.Operator, right.Type())
	}

	return newError("unknown operator: %s %s %s", left.Type(), node.Operator, right.Type())
}

// mismatchedOperandsResult is the result of comparing values of different types, or of types
// that cannot be ordered: they are never equal, so only <> holds.
func mismatchedOperandsResult(operator string) Object {
	return nativeBoolToBooleanObject(operator == "<>")
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
	default:
		return mismatchedOperandsResult(operator)
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

	if left.Type() != right.Type() {
		return mismatchedOperandsResult(operator)
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
	leftNum, ok := left.(*Number)
	if !ok {
		return newError("left operand is not a number: %s", left.Type())
	}

	rightNum, ok := right.(*Number)
	if !ok {
		return newError("right operand is not a number: %s", right.Type())
	}

	leftVal := leftNum.Value
	rightVal := rightNum.Value

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
	leftStr, ok := left.(*String)
	if !ok {
		return newError("left operand is not a string: %s", left.Type())
	}

	rightStr, ok := right.(*String)
	if !ok {
		return newError("right operand is not a string: %s", right.Type())
	}

	leftVal := leftStr.Value
	rightVal := rightStr.Value

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
	leftBin, ok := left.(*Binary)
	if !ok {
		return newError("left operand is not binary: %s", left.Type())
	}

	rightBin, ok := right.(*Binary)
	if !ok {
		return newError("right operand is not binary: %s", right.Type())
	}

	leftVal := leftBin.Value
	rightVal := rightBin.Value

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

	leftBool, ok := left.(*Boolean)
	if !ok {
		return newError("left operand is not boolean: %s", left.Type())
	}

	rightBool, ok := right.(*Boolean)
	if !ok {
		return newError("right operand is not boolean: %s", right.Type())
	}

	return evalBooleanOperator(operator, leftBool.Value, rightBool.Value, left, right)
}

func evalBooleanOperator(operator string, leftVal, rightVal bool, left, right Object) Object {
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
		return newError("reserved word %s found in expression", attributeName)
	}

	val := env.Get(node.Value)
	if isError(val) {
		return val
	}

	return val
}

func evalIndex(node *IndexExpression, env *Environment) Object {
	positions, o, errObj := evalIndexPositions(node, env, false)
	if isError(errObj) {
		return errObj
	}

	obj := o

	// Reading a path that does not resolve, such as a key of a NULL or an index of a map,
	// gives no value, it is not an error.
	for _, pos := range slices.Backward(positions) {
		obj = pos.lookup(obj)
	}

	return obj
}

type indexAccessor struct {
	kind       ObjectType
	val        any
	operator   Token
	fieldID    string
	pathSuffix string
}

func indexFieldID(id *Identifier, env *Environment) string {
	if alias, ok := env.Aliases[id.Value]; ok {
		return alias
	}

	return id.Value
}

func (i indexAccessor) indexName() string {
	switch i.kind {
	case ObjectTypeMap:
		if s, ok := i.val.(string); ok {
			return s
		}
	case ObjectTypeList:
		if n, ok := i.val.(int64); ok {
			return strconv.FormatInt(n, 10)
		}
	}

	return ""
}

// documentPathSuffix formats index segments after the root attribute (root-first,
// dot-separated). positions are innermost-first from evalIndexPositions.
func documentPathSuffix(positions []indexAccessor) string {
	if len(positions) == 0 {
		return ""
	}

	parts := make([]string, len(positions))
	for i, pos := range positions {
		parts[len(positions)-1-i] = pos.indexName()
	}

	return strings.Join(parts, ".")
}

// lookup reads the segment from container, or UNDEFINED when container is not a map or list
// matching the segment or has no such entry.
func (i indexAccessor) lookup(container Object) Object {
	switch c := container.(type) {
	case *List:
		if pos, ok := i.val.(int64); ok && i.kind == ObjectTypeList {
			return c.Get(pos)
		}
	case *Map:
		if pos, ok := i.val.(string); ok && i.kind == ObjectTypeMap {
			return c.Get(pos)
		}
	}

	return UNDEFINED
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

		return UNDEFINED
	}

	list.Value = append(list.Value, value)

	return UNDEFINED
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

		return UNDEFINED
	}

	return newInvalidDocumentPathError(i.fieldID, i.pathSuffix)
}

func handleIndexIdentifier(n Expression, env *Environment, positions []indexAccessor, assign bool) ([]indexAccessor, Object, Object) {
	obj, errObj := evalIndexObj(n, env, assign)
	if isError(errObj) {
		if assign {
			fieldID := ""
			if id, ok := n.(*Identifier); ok {
				fieldID = indexFieldID(id, env)
			}

			return nil, nil, newInvalidDocumentPathError(fieldID, documentPathSuffix(positions))
		}

		return nil, nil, errObj
	}

	if id, ok := n.(*Identifier); ok {
		fieldID := indexFieldID(id, env)
		suffix := documentPathSuffix(positions)

		for j := range positions {
			positions[j].fieldID = fieldID
			positions[j].pathSuffix = suffix
		}
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

		return UNDEFINED
	}

	return newError("index removal for %q type is not supported", container.Type())
}

func evalIndexPositions(n Expression, env *Environment, assign bool) ([]indexAccessor, Object, Object) {
	positions := []indexAccessor{}
	exp := n

	for {
		switch node := exp.(type) {
		case *Identifier:
			return handleIndexIdentifier(exp, env, positions, assign)
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

// evalIndexObj evaluates the root of a document path. When reading, a root that is not a map
// or list is returned as is and the path just does not resolve; assigning to it is an error.
func evalIndexObj(identifierExpression Expression, env *Environment, assign bool) (Object, Object) {
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

	if isUndefined(obj) || !assign {
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

		return indexAccessor{val: pos, kind: indexNode.Type, operator: indexNode.Token}, UNDEFINED
	case ObjectTypeMap:
		pos, errObj := evalMapIndexValue(node, env)
		if isError(errObj) {
			return indexAccessor{}, errObj
		}

		return indexAccessor{val: pos, kind: indexNode.Type, operator: indexNode.Token}, UNDEFINED
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

	name := node.Token.Literal
	if alias, ok := env.Aliases[name]; ok {
		return alias, nil
	}

	return name, nil
}

func evalBetween(node *BetweenExpression, env *Environment) Object {
	val := evalOperand(node.Left, env)
	if isError(val) {
		return val
	}

	min := evalOperand(node.Range[0], env)
	if isError(min) {
		return min
	}

	max := evalOperand(node.Range[1], env)
	if isError(max) {
		return max
	}

	// The bounds that are values have a known type: DynamoDB rejects a wrong one even when
	// the path is missing.
	if errObj := staticBetweenError(node, min, max); errObj != nil {
		return errObj
	}

	if isUndefined(val) || isUndefined(min) || isUndefined(max) {
		return FALSE
	}

	// A stored value of another type is just not between the bounds.
	if !comparableTypes[val.Type()] || !matchTypes(val.Type(), val, min, max) {
		return FALSE
	}

	return compareRange(val, min, max)
}

func staticBetweenError(node *BetweenExpression, min, max Object) Object {
	bounds := []Object{min, max}

	for i, bound := range bounds {
		if isValueOperand(node.Range[i]) && !isUndefined(bound) && !comparableTypes[bound.Type()] {
			return newError(
				"unexpected type: %q should be a comparable type(N,S,B) got %q", node.Range[i].String(), bound.Type(),
			)
		}
	}

	if isValueOperand(node.Range[0]) && isValueOperand(node.Range[1]) && min.Type() != max.Type() {
		return newError("mismatch type: BETWEEN operands must have the same type")
	}

	return nil
}

func evalIn(node *InExpression, env *Environment) Object {
	val := evalOperand(node.Left, env)
	if isError(val) {
		return val
	}

	rangeObjects := &List{Value: make([]Object, 0, len(node.Range))}

	for _, exp := range node.Range {
		obj := evalOperand(exp, env)
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

// evalOperand evaluates a path or value operand of BETWEEN and IN.
func evalOperand(exp Expression, env *Environment) Object {
	return Eval(exp, env)
}

func evalFunctionCall(node *CallExpression, env *Environment) Object {
	fn := evalFunctionCallIdentifer(node, env)
	if isError(fn) {
		return fn
	}

	funcObj, ok := fn.(*Function)
	if !ok {
		return newInvalidFunctionCallError(node)
	}

	if funcObj.ForUpdate {
		return newError("the function is not allowed in a condition expression; function: %s", funcObj.Name)
	}

	if err := operandCountError(funcObj, len(node.Arguments)); err != nil {
		return newError("%s", err.Error())
	}

	args := evalExpressions(node.Arguments, env)
	if len(args) == 1 && isError(args[0]) {
		return args[0]
	}

	if errObj := staticFunctionError(funcObj, node.Arguments, args); errObj != nil {
		return errObj
	}

	return funcObj.Value(args...)
}

// staticFunctionError returns the error for an operand whose type DynamoDB rejects before
// reading any item: only values have a known type, a stored value of another type makes the
// function false (or its size missing) instead.
func staticFunctionError(fn *Function, exprs []Expression, args []Object) Object {
	for i, arg := range args {
		if i >= len(exprs) || !isValueOperand(exprs[i]) {
			continue
		}

		switch fn.Name {
		case "begins_with":
			if arg.Type() != ObjectTypeString && arg.Type() != ObjectTypeBinary && !isUndefined(arg) {
				if i == 0 {
					return newError("invalid type %s", arg.Type())
				}

				return newError("invalid substr type %s", arg.Type())
			}
		case "size":
			if _, ok := arg.(SizableObject); !ok {
				return newError("type not supported: size %s", arg.Type())
			}
		}
	}

	return nil
}

func evalUpdateFunctionCall(node *CallExpression, env *Environment) Object {
	fn := evalFunctionCallIdentifer(node, env)
	if isError(fn) {
		return fn
	}

	funcObj, ok := fn.(*Function)
	if !ok {
		return newInvalidFunctionCallError(node)
	}

	if !funcObj.ForUpdate {
		return newError("the function is not allowed in an update expression; function: %s", funcObj.Name)
	}

	if err := operandCountError(funcObj, len(node.Arguments)); err != nil {
		return newError("%s", err.Error())
	}

	if errObj := staticUpdateFunctionError(funcObj, node.Arguments, env); errObj != nil {
		return errObj
	}

	args := evalUpdateExpressions(node.Arguments, env)
	if len(args) == 1 && isError(args[0]) {
		return args[0]
	}

	function, ok := fn.(*Function)
	if !ok {
		return newInvalidFunctionCallError(node)
	}

	return function.Value(args...)
}

// staticUpdateFunctionError returns the errors DynamoDB reports for an update function call
// before reading the item: values of the wrong type and if_not_exists operands that repeat
// the first one.
func staticUpdateFunctionError(fn *Function, exprs []Expression, env *Environment) Object {
	switch fn.Name {
	case "list_append":
		for _, expr := range exprs {
			if !isValueOperand(expr) {
				continue
			}

			if val := EvalUpdate(expr, env); !isError(val) && !isUndefined(val) && val.Type() != ObjectTypeList {
				return newIncorrectOperandTypeError(fn.Name, val)
			}
		}
	case "if_not_exists":
		return staticIfNotExistsError(exprs, env)
	}

	return nil
}

func staticIfNotExistsError(exprs []Expression, env *Environment) Object {
	first, ok := documentPathText(exprs[0], env)
	if !ok {
		return nil
	}

	for _, expr := range exprs[1:] {
		if other, isPath := documentPathText(expr, env); isPath && other == first {
			return newError("Invalid UpdateExpression: The first operand must be distinct from the remaining "+
				"operands for this operator or function; operator: if_not_exists, first operand: %s", first)
		}
	}

	return nil
}

// documentPathText formats a document path as DynamoDB does in its messages, such as
// [m, k] or [l, [0]]; ok is false when expr is not a document path.
func documentPathText(expr Expression, env *Environment) (string, bool) {
	if isValueOperand(expr) {
		return "", false
	}

	elements, err := ExtractPath(expr, env)
	if err != nil {
		return "", false
	}

	parts := make([]string, len(elements))

	for i, el := range elements {
		if el.Kind == PathKindListIndex {
			parts[i] = "[" + strconv.FormatInt(el.Index, 10) + "]"

			continue
		}

		parts[i] = el.Key
	}

	return "[" + strings.Join(parts, ", ") + "]", true
}

// operandText formats an update operand for error detail: a document path as written in an
// expression, with names resolved (m.k or l[0]), and anything else as is.
func operandText(expr Expression, env *Environment) string {
	elements, err := ExtractPath(expr, env)
	if err != nil {
		return expr.String()
	}

	var b strings.Builder

	for i, el := range elements {
		switch {
		case el.Kind == PathKindListIndex:
			b.WriteString("[" + strconv.FormatInt(el.Index, 10) + "]")
		case i > 0:
			b.WriteString("." + el.Key)
		default:
			b.WriteString(el.Key)
		}
	}

	return b.String()
}

func evalFunctionCallIdentifer(node *CallExpression, env *Environment) Object {
	functionIdentifier, ok := node.Function.(*Identifier)
	if !ok {
		return newError("bad function syntax")
	}

	name := functionIdentifier.Value

	fn, ok := functions[name]
	if !ok {
		return newError("invalid function name; function: %s", name)
	}

	return fn
}

func evalUpdateExpression(node *UpdateExpression, env *Environment) Object {
	if len(node.Expressions) == 0 {
		return newError("%s expression must have at least one action", node.TokenLiteral())
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

	return UNDEFINED
}

func evalActionSet(node *ActionExpression, env *Environment) Object {
	val := EvalUpdate(node.Right, env)
	if isError(val) {
		return val
	}

	if isUndefined(val) {
		return newMissingOperandError("operand", operandText(node.Right, env))
	}

	id, ok := node.Left.(*Identifier)
	if ok {
		// We need to validate left hand side is not a keyword
		obj := evalIdentifier(id, env, true)
		if isError(obj) {
			return obj
		}

		env.Set(id.Value, val)

		return UNDEFINED
	}

	indexField, ok := node.Left.(*IndexExpression)
	if ok {
		errObj := evalAssignIndex(indexField, []int{}, val, env)
		if isError(errObj) {
			return errObj
		}

		return UNDEFINED
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
		// We need to validate left hand side is not a keyword
		obj := evalIdentifier(id, env, true)
		if isError(obj) {
			return obj
		}

		return addToTarget(obj, val, func(created Object) { env.Set(id.Value, created) })
	}

	indexField, ok := node.Left.(*IndexExpression)
	if ok {
		// The operand is validated before the path, as DynamoDB does.
		if !isAddOperand(val) {
			return newIncorrectAddOperandError(UNDEFINED, val)
		}

		path, errObj := resolveNestedTarget(indexField, env)
		if isError(errObj) {
			return errObj
		}

		// resolveNestedTarget only returns a target whose container matches the accessor, so
		// Set cannot fail here.
		return addToTarget(path.target, val, func(created Object) { _ = path.pos.Set(path.container, created) })
	}

	return UNDEFINED
}

// addToTarget applies ADD of val to obj; create stores val when obj does not exist.
func addToTarget(obj, val Object, create func(Object)) Object {
	if obj == UNDEFINED {
		if !isAddOperand(val) {
			return newIncorrectAddOperandError(obj, val)
		}

		create(val)

		return obj
	}

	addObj, ok := obj.(AppendableObject)
	if !ok {
		return newIncorrectDataTypeError("ADD", obj, val)
	}

	return addObj.Add(val)
}

// isAddOperand reports whether val can be the operand of ADD: a number or a set, whether or
// not the target exists. Sets are the only DetachableObjects.
func isAddOperand(val Object) bool {
	_, isNumber := val.(*Number)
	_, isSet := val.(DetachableObject)

	return isNumber || isSet
}

// nestedTarget is the resolved left side of an action on a document path: the map or list
// holding the final segment, the accessor of that segment and the current value (UNDEFINED
// when missing).
type nestedTarget struct {
	container Object
	pos       indexAccessor
	target    Object
}

// resolveNestedTarget walks a document path such as m.ss or l[0]. Like DynamoDB, it fails
// with the invalid document path error when the parent of the last segment is missing or is
// not a map/list matching that segment.
func resolveNestedTarget(node *IndexExpression, env *Environment) (nestedTarget, Object) {
	positions, container, errObj := evalIndexPositions(node, env, true)
	if isError(errObj) {
		return nestedTarget{}, errObj
	}

	last := positions[0]
	invalid := newInvalidDocumentPathError(last.fieldID, last.pathSuffix)

	for i := len(positions) - 1; i > 0; i-- {
		container = positions[i].Get(container)
		if isError(container) {
			return nestedTarget{}, invalid
		}
	}

	target := last.Get(container)
	if isError(target) || isUndefined(container) {
		return nestedTarget{}, invalid
	}

	return nestedTarget{container: container, pos: last, target: target}, nil
}

func evalActionDelete(node *ActionExpression, env *Environment) Object {
	val := EvalUpdate(node.Right, env)
	if isError(val) {
		return val
	}

	// DELETE only takes a set operand, whether or not the target exists. Sets are the only
	// DetachableObjects.
	if _, ok := val.(DetachableObject); !ok {
		return newError(
			"Invalid UpdateExpression: Incorrect operand type for operator or function; "+
				"operator: DELETE, operand type: %s, typeSet: ALLOWED_FOR_DELETE_OPERAND",
			dynamoDBTypeName(val.Type()),
		)
	}

	id, ok := node.Left.(*Identifier)
	if ok {
		// We need to validate left hand side is not a keyword
		obj := evalIdentifier(id, env, true)
		if isError(obj) {
			return obj
		}

		return deleteFromTarget(obj, val, func() { env.Remove(id.Value) })
	}

	indexField, ok := node.Left.(*IndexExpression)
	if ok {
		path, errObj := resolveNestedTarget(indexField, env)
		if isError(errObj) {
			return errObj
		}

		return deleteFromTarget(path.target, val, func() {
			// The target exists in a container that matches the accessor, so Remove cannot fail.
			_ = path.pos.Remove(path.container)
			env.MarkToCompact(path.container)
		})
	}

	return UNDEFINED
}

// deleteFromTarget applies DELETE of the set val from obj; remove drops obj when it ends up
// empty.
func deleteFromTarget(obj, val Object, remove func()) Object {
	// Deleting from a set that doesn't exist leaves the item unchanged.
	if obj == UNDEFINED {
		return obj
	}

	delObj, ok := obj.(DetachableObject)
	if !ok {
		return newIncorrectDeleteOperandError(obj, val)
	}

	result := delObj.Delete(val)
	if isError(result) {
		return result
	}

	// DynamoDB does not keep empty sets: removing the last elements removes the attribute.
	if sized, ok := obj.(SizableObject); ok && sized.Size() == 0 {
		remove()
	}

	return result
}

// dynamoDBTypeName returns the long type name DynamoDB uses in validation messages.
func dynamoDBTypeName(t ObjectType) string {
	names := map[ObjectType]string{
		ObjectTypeString:    "STRING",
		ObjectTypeNumber:    "NUMBER",
		ObjectTypeBinary:    "BINARY",
		ObjectTypeBoolean:   "BOOLEAN",
		ObjectTypeNull:      "NULL",
		ObjectTypeList:      "LIST",
		ObjectTypeMap:       "MAP",
		ObjectTypeStringSet: "STRING_SET",
		ObjectTypeNumberSet: "NUMBER_SET",
		ObjectTypeBinarySet: "BINARY_SET",
	}

	if name, ok := names[t]; ok {
		return name
	}

	return string(t)
}

func evalActionRemove(node *ActionExpression, env *Environment) Object {
	id, ok := node.Left.(*Identifier)
	if ok {
		// We need to validate left hand side is not a keyword
		val := evalIdentifier(id, env, true)
		if isError(val) {
			return val
		}

		env.Remove(id.Value)

		return UNDEFINED
	}

	indexField, ok := node.Left.(*IndexExpression)
	if ok {
		// Like DynamoDB, removing a path whose parent does not resolve is an invalid document
		// path, while a missing last segment is left alone.
		path, errObj := resolveNestedTarget(indexField, env)
		if isError(errObj) {
			return errObj
		}

		if isUndefined(path.target) {
			return UNDEFINED
		}

		errObj = path.pos.Remove(path.container)
		if isError(errObj) {
			return errObj
		}

		env.MarkToCompact(path.container)

		return UNDEFINED
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
	positions, o, errObj := evalIndexPositions(n, env, true)
	if isError(errObj) {
		return errObj
	}

	obj := o

	for idx, pos := range slices.Backward(positions) {
		if idx == 0 {
			errObj := pos.Set(obj, val)
			if isError(errObj) {
				return errObj
			}

			break
		}

		obj = pos.Get(obj)
	}

	return UNDEFINED
}

func evalArithmeticTerms(node *InfixExpression, env *Environment) (*Number, *Number, Object) {
	// The values are checked first: DynamoDB knows their type before reading the item.
	for _, operand := range []Expression{node.Left, node.Right} {
		if !isValueOperand(operand) {
			continue
		}

		if val := EvalUpdate(operand, env); !isError(val) && !isUndefined(val) && !isNumber(val) {
			return nil, nil, newIncorrectOperandTypeError(node.Operator, val)
		}
	}

	leftNumber, errObj := evalArithmeticTerm(node.Operator, node.Left, env)
	if isError(errObj) {
		return nil, nil, errObj
	}

	rightNumber, errObj := evalArithmeticTerm(node.Operator, node.Right, env)
	if isError(errObj) {
		return nil, nil, errObj
	}

	return leftNumber, rightNumber, UNDEFINED
}

// evalArithmeticTerm evaluates one operand of + or -: it must exist and be a number.
func evalArithmeticTerm(operator string, expr Expression, env *Environment) (*Number, Object) {
	term := EvalUpdate(expr, env)
	if isError(term) {
		return nil, term
	}

	if isUndefined(term) {
		return nil, newMissingOperandError("operator", operator, "operand", operandText(expr, env))
	}

	number, ok := term.(*Number)
	if !ok {
		return nil, newOperandTypeError(term, "operator", operator, "operand", operandText(expr, env))
	}

	return number, nil
}

// newIncorrectOperandTypeError is DynamoDB's error for a value of the wrong type used as
// operand of an operator or function in an update expression.
func newIncorrectOperandTypeError(operator string, operand Object) *Error {
	return newError(
		"Invalid UpdateExpression: Incorrect operand type for operator or function; "+
			"operator or function: %s, operand type: %s", operator, operand.Type(),
	)
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

func evalUpdateExpressions(exps []Expression, env *Environment) []Object {
	var result []Object

	for _, e := range exps {
		evaluated := EvalUpdate(e, env)
		if isError(evaluated) {
			return []Object{evaluated}
		}

		result = append(result, evaluated)
	}

	return result
}
