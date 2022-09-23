package language

import (
	"bytes"
	"strings"

	"github.com/truora/minidyn/types"
)

// Function represents a function in the types expression
type Function struct {
	Name      string
	Value     func(...Object) Object
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

var (
	functions = map[string]*Function{
		"attribute_exists": &Function{
			Name:  "attribute_exists",
			Value: attributeExists,
		},
		"attribute_not_exists": &Function{
			Name:  "attribute_not_exists",
			Value: attributeNotExists,
		},
		"attribute_type": &Function{
			Name:  "attribute_type",
			Value: attributeType,
		},
		"begins_with": &Function{
			Name:  "begins_with",
			Value: beginsWith,
		},
		"contains": &Function{
			Name:  "contains",
			Value: contains,
		},
		"size": &Function{
			Name:  "size",
			Value: objectSize,
		},
		"if_not_exists": &Function{
			Name:      "if_not_exists",
			Value:     ifNotExists,
			ForUpdate: true,
		},
		"list_append": &Function{
			Name:      "list_append",
			Value:     listAppend,
			ForUpdate: true,
		},
	}
)

func attributeExists(args ...Object) Object {
	path := args[0]

	return nativeBoolToBooleanObject(path.Type() != ObjectTypeNull)
}

func attributeNotExists(args ...Object) Object {
	path := args[0]

	return nativeBoolToBooleanObject(path.Type() == ObjectTypeNull)
}

func attributeType(args ...Object) Object {
	path := args[0]
	typ := args[1]

	if typ.Type() == ObjectTypeString {
		strObj, _ := typ.(*String)
		if !dynamodbTypes[ObjectType(strObj.Value)] {
			return newError("invalid type %s", strObj.Value)
		}

		return nativeBoolToBooleanObject(path.Type() == ObjectType(strObj.Value))
	}

	return newError("invalid type %s", typ.Type())
}

func beginsWith(args ...Object) Object {
	path := args[0]
	substr := args[1]

	if path.Type() == ObjectTypeString {
		if substr.Type() != ObjectTypeString {
			return newError("invalid substr type %s", substr.Type())
		}

		return nativeBoolToBooleanObject(strings.HasPrefix(path.Inspect(), substr.Inspect()))
	}

	if path.Type() == ObjectTypeBinary {
		if substr.Type() != ObjectTypeBinary {
			return newError("invalid substr type %s", substr.Type())
		}

		binarySubstr, _ := substr.(*Binary)
		binaryPath, _ := path.(*Binary)

		return nativeBoolToBooleanObject(bytes.HasPrefix(binaryPath.Value, binarySubstr.Value))
	}

	return newError("invalid type %s", path.Type())
}

func contains(args ...Object) Object {
	path := args[0]
	operand := args[1]

	container, ok := path.(ContainerObject)
	if !ok {
		return newError("contains is not supported for path=%s", path.Type())
	}

	if !container.CanContain(operand.Type()) {
		return newError("contains is not supported for path=%s operand=%s", path.Type(), operand.Type())
	}

	return nativeBoolToBooleanObject(container.Contains(operand))
}

func objectSize(args ...Object) Object {
	path := args[0]

	switch path.Type() {
	case ObjectTypeString:
		str, _ := path.(*String)

		return &Number{Value: float64(len(str.Value))}
	case ObjectTypeBinary:
		bin, _ := path.(*Binary)

		return &Number{Value: float64(len(bin.Value))}
	}

	return newError("type not supported: size %s", path.Type())
}

func ifNotExists(args ...Object) Object {
	obj := args[0]

	if obj == nil || obj.Type() == ObjectTypeNull {
		return args[1]
	}

	return obj
}

func listAppend(args ...Object) Object {
	value1 := args[0]
	if value1.Type() != ObjectTypeList {
		return newError("list_append is not supported for list1=%s", value1.Type())
	}

	value2 := args[1]
	if value2.Type() != ObjectTypeList {
		return newError("list_append is not supported for list2=%s", value2.Type())
	}

	list1 := value1.(*List)
	list2 := value2.(*List)

	return &List{Value: append(list1.Value, list2.Value...)}
}
