package language

import (
	"slices"
	"strconv"

	"github.com/truora/minidyn/types"
)

// Function represents a function in the types expression
type Function struct {
	Name      string
	Value     func(...Object) Object
	Arity     int // the exact number of operands DynamoDB accepts
	ForUpdate bool
}

// Inspect returns the readable value of the object
func (fn *Function) Inspect() string {
	return fn.Name
}

// Type returns the object type
func (fn *Function) Type() ObjectType {
	return ObjectTypeFunction
}

// ToDynamoDB returns the types attribute value
func (fn *Function) ToDynamoDB() types.Item {
	return types.Item{}
}

var functions = map[string]*Function{
	"attribute_exists": {
		Name:  "attribute_exists",
		Arity: 1,
		Value: attributeExists,
	},
	"attribute_not_exists": {
		Name:  "attribute_not_exists",
		Arity: 1,
		Value: attributeNotExists,
	},
	"attribute_type": {
		Name:  "attribute_type",
		Arity: 2,
		Value: attributeType,
	},
	"begins_with": {
		Name:  "begins_with",
		Arity: 2,
		Value: beginsWith,
	},
	"contains": {
		Name:  "contains",
		Arity: 2,
		Value: contains,
	},
	"size": {
		Name:  "size",
		Arity: 1,
		Value: objectSize,
	},
	"if_not_exists": {
		Name:      "if_not_exists",
		Arity:     2,
		Value:     ifNotExists,
		ForUpdate: true,
	},
	"list_append": {
		Name:      "list_append",
		Arity:     2,
		Value:     listAppend,
		ForUpdate: true,
	},
}

func attributeExists(args ...Object) Object {
	path := args[0]

	return nativeBoolToBooleanObject(!isUndefined(path))
}

func attributeNotExists(args ...Object) Object {
	path := args[0]

	return nativeBoolToBooleanObject(isUndefined(path))
}

func attributeType(args ...Object) Object {
	path := args[0]
	typ := args[1]

	if typ.Type() == ObjectTypeString {
		strObj, _ := typ.(*String)
		if !dynamodbTypes[ObjectType(strObj.Value)] {
			return newError("invalid type %s", strObj.Value)
		}

		if isUndefined(path) {
			return FALSE
		}

		return nativeBoolToBooleanObject(path.Type() == ObjectType(strObj.Value))
	}

	return newError("invalid type %s", typ.Type())
}

// A path that is missing or holds a value of another type never makes begins_with, contains
// or size fail: DynamoDB evaluates them to false (or no size), see #143.

func beginsWith(args ...Object) Object {
	prefixed, ok := args[0].(PrefixObject)
	if !ok {
		return FALSE
	}

	return nativeBoolToBooleanObject(prefixed.HasPrefix(args[1]))
}

func contains(args ...Object) Object {
	path := args[0]
	operand := args[1]

	container, ok := path.(ContainerObject)
	if !ok || !container.CanContain(operand.Type()) {
		return FALSE
	}

	return nativeBoolToBooleanObject(container.Contains(operand))
}

func objectSize(args ...Object) Object {
	path := args[0]

	if sizable, ok := path.(SizableObject); ok {
		return &Number{Value: float64(sizable.Size())}
	}

	return UNDEFINED
}

func ifNotExists(args ...Object) Object {
	if isUndefined(args[1]) {
		return newMissingOperandError("function", "if_not_exists", "operand", "2")
	}

	if isUndefined(args[0]) {
		return args[1]
	}

	return args[0]
}

func listAppend(args ...Object) Object {
	lists := make([]*List, 0, len(args))

	for i, arg := range args {
		operand := strconv.Itoa(i + 1)

		if isUndefined(arg) {
			return newMissingOperandError("function", "list_append", "operand", operand)
		}

		list, ok := arg.(*List)
		if !ok {
			return newOperandTypeError(arg, "function", "list_append", "operand", operand)
		}

		lists = append(lists, list)
	}

	return &List{Value: append(slices.Clone(lists[0].Value), lists[1].Value...)}
}
