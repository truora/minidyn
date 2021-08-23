package language

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	// NULL definel the global null value
	NULL = &Null{}
	// TRUE definel the global true value
	TRUE = &Boolean{Value: true}
	// FALSE definel the global false value
	FALSE = &Boolean{Value: false}
)

// ObjectType defines the object type enum
type ObjectType string

const (
	// ObjectTypeBinary type used to represent binaries
	ObjectTypeBinary ObjectType = "B"
	// ObjectTypeBinarySet type used to represent set of binaries
	ObjectTypeBinarySet ObjectType = "BS"
	// ObjectTypeBoolean type used to represent booleans
	ObjectTypeBoolean ObjectType = "BOOL"
	// ObjectTypeList type used to represent lists
	ObjectTypeList ObjectType = "L"
	// ObjectTypeMap type used to represent maps
	ObjectTypeMap ObjectType = "M"
	// ObjectTypeNull type used to represent the null value
	ObjectTypeNull ObjectType = "NULL"
	// ObjectTypeNumber type used to represent numbers
	ObjectTypeNumber ObjectType = "N"
	// ObjectTypeNumberSet type used to represent sets of numbers
	ObjectTypeNumberSet ObjectType = "NS"
	// ObjectTypeStringSet type used to represent sets of strings
	ObjectTypeStringSet ObjectType = "SS"
	// ObjectTypeString type used to represent strings
	ObjectTypeString ObjectType = "S"
	// ObjectTypeError type used to represent errors
	ObjectTypeError ObjectType = "ERR"
	// ObjectTypeFunction type used to represent functions
	ObjectTypeFunction ObjectType = "FN"
)

var dynamodbTypes = map[ObjectType]bool{
	ObjectTypeBinary:    true,
	ObjectTypeBinarySet: true,
	ObjectTypeBoolean:   true,
	ObjectTypeList:      true,
	ObjectTypeMap:       true,
	ObjectTypeNull:      true,
	ObjectTypeNumber:    true,
	ObjectTypeNumberSet: true,
	ObjectTypeStringSet: true,
	ObjectTypeString:    true,
}

// comparableTypes Types that support < <= >= >
var comparableTypes = map[ObjectType]bool{
	ObjectTypeNumber: true,
	ObjectTypeString: true,
	ObjectTypeBinary: true,
}

var setTypes = map[ObjectType]bool{
	ObjectTypeBinarySet: true,
	ObjectTypeStringSet: true,
	ObjectTypeNumberSet: true,
}

// Object abstraction of the object values
type Object interface {
	Type() ObjectType
	Inspect() string
	ToDynamoDB() *dynamodb.AttributeValue
}

// ContainerObject abstraction of the object collections
type ContainerObject interface {
	Object
	Contains(obj Object) bool
	CanContain(objType ObjectType) bool
}

// Number is the representation of numbers
type Number struct {
	Value float64
}

// Inspect returns the readable value of the object
func (i *Number) Inspect() string {
	return fmt.Sprintf("%f", i.Value)
}

// Type returns the object type
func (i *Number) Type() ObjectType {
	return ObjectTypeNumber
}

// ToDynamoDB returns the dynamodb attribute value
func (i *Number) ToDynamoDB() *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%f", i.Value))}
}

// Boolean is the representation of boolean
type Boolean struct {
	Value bool
}

// Inspect returns the readable value of the object
func (b *Boolean) Inspect() string {
	return fmt.Sprintf("%t", b.Value)
}

// Type returns the object type
func (b *Boolean) Type() ObjectType {
	return ObjectTypeBoolean
}

// ToDynamoDB returns the dynamodb attribute value
func (b *Boolean) ToDynamoDB() *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{BOOL: aws.Bool(b.Value)}
}

func nativeBoolToBooleanObject(input bool) *Boolean {
	if input {
		return TRUE
	}

	return FALSE
}

// Binary is the representation of binaries
type Binary struct {
	Value []byte
}

// Inspect returns the readable value of the object
func (b *Binary) Inspect() string {
	return fmt.Sprintf("%v", b.Value)
}

// Type returns the object type
func (b *Binary) Type() ObjectType {
	return ObjectTypeBinary
}

// ToDynamoDB returns the dynamodb attribute value
func (b *Binary) ToDynamoDB() *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{B: b.Value}
}

// Contains whether or not the obj is contained in the binary
func (b *Binary) Contains(obj Object) bool {
	bin, ok := obj.(*Binary)
	if !ok {
		return false
	}

	return bytes.Contains(b.Value, bin.Value)
}

// CanContain whether or not the string can contain the objType
func (b *Binary) CanContain(objType ObjectType) bool {
	return objType == ObjectTypeBinary
}

// Null is the representation of nil values
type Null struct{}

// Type returns the object type
func (n *Null) Type() ObjectType { return ObjectTypeNull }

// Inspect returns the readable value of the object
func (n *Null) Inspect() string {
	return "null"
}

// ToDynamoDB returns the dynamodb attribute value
func (n *Null) ToDynamoDB() *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{NULL: aws.Bool(true)}
}

// Error is the representation of errors
type Error struct {
	Message string
}

// Type returns the object type
func (e *Error) Type() ObjectType { return ObjectTypeError }

// Inspect returns the readable value of the object
func (e *Error) Inspect() string { return "ERROR: " + e.Message }

// ToDynamoDB returns the dynamodb attribute value
func (e *Error) ToDynamoDB() *dynamodb.AttributeValue {
	return nil
}

// String is the representation of strings
type String struct {
	Value string
}

// Inspect returns the readable value of the object
func (s *String) Inspect() string {
	return s.Value
}

// Type returns the object type
func (s *String) Type() ObjectType {
	return ObjectTypeString
}

// Contains whether or not the obj is contained in the string
func (s *String) Contains(obj Object) bool {
	str, ok := obj.(*String)
	if !ok {
		return false
	}

	return strings.Contains(s.Value, str.Value)
}

// ToDynamoDB returns the dynamodb attribute value
func (s *String) ToDynamoDB() *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{S: aws.String(s.Value)}
}

// CanContain whether or not the string can contain the objType
func (s *String) CanContain(objType ObjectType) bool {
	return objType == ObjectTypeString
}

// Map is the representation of map
type Map struct {
	Value map[string]Object
}

// Inspect returns the readable value of the object
func (m *Map) Inspect() string {
	var out bytes.Buffer

	out.WriteString("{")

	for k, obj := range m.Value {
		out.WriteString("\n  \"")
		out.WriteString(k)
		out.WriteString("\" : ")

		out.WriteString(obj.Inspect())
		out.WriteString("<")
		out.WriteString(string(obj.Type()))
		out.WriteString(">,\n")
	}

	out.WriteString("}")

	return out.String()
}

// Type returns the object type
func (m *Map) Type() ObjectType {
	return ObjectTypeMap
}

// ToDynamoDB returns the dynamodb attribute value
func (m *Map) ToDynamoDB() *dynamodb.AttributeValue {
	attr := &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{}}

	for k, v := range m.Value {
		attr.M[k] = v.ToDynamoDB()
	}

	return attr
}

// List is the representation of list
type List struct {
	Value []Object
}

// Inspect returns the readable value of the object
func (l *List) Inspect() string {
	var out bytes.Buffer

	out.WriteString("[ ")

	for _, obj := range l.Value {
		out.WriteString(obj.Inspect())
		out.WriteString("<")
		out.WriteString(string(obj.Type()))
		out.WriteString("> ")
	}

	out.WriteString("]")

	return out.String()
}

// Type returns the object type
func (l *List) Type() ObjectType {
	return ObjectTypeList
}

// ToDynamoDB returns the dynamodb attribute value
func (l *List) ToDynamoDB() *dynamodb.AttributeValue {
	attr := &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}

	for _, v := range l.Value {
		attr.L = append(attr.L, v.ToDynamoDB())
	}

	return attr
}

// Contains whether or not the obj is contained in the list
func (l *List) Contains(obj Object) bool {
	if !l.CanContain(obj.Type()) {
		return false
	}

	for _, e := range l.Value {
		if equalObject(obj, e) {
			return true
		}
	}

	return false
}

// CanContain whether or not the list can contain the objType
func (l *List) CanContain(objType ObjectType) bool {
	return !(objType == ObjectTypeList || objType == ObjectTypeMap || setTypes[objType])
}

// StringSet is the representation of map
type StringSet struct {
	Value map[string]bool
}

// Inspect returns the readable value of the object
func (ss *StringSet) Inspect() string {
	var out bytes.Buffer

	vals := make([]string, 0, len(ss.Value))
	for v := range ss.Value {
		vals = append(vals, v)
	}

	sort.Strings(vals)
	out.WriteString("[ ")

	for _, k := range vals {
		out.WriteString(k)
		out.WriteString(" ")
	}

	out.WriteString("]<SS>")

	return out.String()
}

// Type returns the object type
func (ss *StringSet) Type() ObjectType {
	return ObjectTypeStringSet
}

// Contains returns if the collection contains the object
func (ss *StringSet) Contains(obj Object) bool {
	if obj.Type() == ObjectTypeStringSet {
		ssInput, ok := obj.(*StringSet)
		if !ok {
			return false
		}

		for str := range ssInput.Value {
			if !ss.Value[str] {
				return false
			}
		}

		return true
	}

	str, ok := obj.(*String)
	if !ok {
		return false
	}

	return ss.Value[str.Value]
}

// ToDynamoDB returns the dynamodb attribute value
func (ss *StringSet) ToDynamoDB() *dynamodb.AttributeValue {
	attr := &dynamodb.AttributeValue{SS: make([]*string, 0, len(ss.Value))}

	for v := range ss.Value {
		attr.SS = append(attr.SS, aws.String(v))
	}

	return attr
}

// CanContain whether or not the string set can contain the objType
func (ss *StringSet) CanContain(objType ObjectType) bool {
	return objType == ObjectTypeString || objType == ObjectTypeStringSet
}

// BinarySet is the representation of a binary set
type BinarySet struct {
	Value [][]byte
}

// Inspect returns the readable value of the object
func (bs *BinarySet) Inspect() string {
	var out bytes.Buffer

	out.WriteString("[ ")

	for _, k := range bs.Value {
		out.WriteString(fmt.Sprintf("%v", k))
		out.WriteString(" ")
	}

	out.WriteString("]<BS>")

	return out.String()
}

// Type returns the object type
func (bs *BinarySet) Type() ObjectType {
	return ObjectTypeBinarySet
}

func containedInBinaryArray(container [][]byte, bin []byte) bool {
	for _, b := range container {
		if bytes.Equal(b, bin) {
			return true
		}
	}

	return false
}

// ToDynamoDB returns the dynamodb attribute value
func (bs *BinarySet) ToDynamoDB() *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{BS: bs.Value}
}

// Contains returns if the collection contains the object
func (bs *BinarySet) Contains(obj Object) bool {
	if obj.Type() == ObjectTypeBinarySet {
		bsInput, ok := obj.(*BinarySet)
		if !ok {
			return false
		}

		for _, bin := range bsInput.Value {
			if !containedInBinaryArray(bs.Value, bin) {
				return false
			}
		}

		return true
	}

	bin, ok := obj.(*Binary)
	if !ok {
		return false
	}

	return containedInBinaryArray(bs.Value, bin.Value)
}

// CanContain whether or not the binary set can contain the objType
func (bs *BinarySet) CanContain(objType ObjectType) bool {
	return objType == ObjectTypeBinary || objType == ObjectTypeBinarySet
}

// NumberSet is the representation of a number set
type NumberSet struct {
	Value map[float64]bool
}

// Inspect returns the readable value of the object
func (ns *NumberSet) Inspect() string {
	var out bytes.Buffer

	vals := make([]float64, 0, len(ns.Value))
	for v := range ns.Value {
		vals = append(vals, v)
	}

	sort.Float64s(vals)
	out.WriteString("[ ")

	for _, k := range vals {
		out.WriteString(fmt.Sprintf("%v", k))
		out.WriteString(" ")
	}

	out.WriteString("]<NS>")

	return out.String()
}

// Type returns the object type
func (ns *NumberSet) Type() ObjectType {
	return ObjectTypeNumberSet
}

// ToDynamoDB returns the dynamodb attribute value
func (ns *NumberSet) ToDynamoDB() *dynamodb.AttributeValue {
	attr := &dynamodb.AttributeValue{NS: make([]*string, 0, len(ns.Value))}

	for v := range ns.Value {
		attr.NS = append(attr.NS, aws.String(fmt.Sprintf("%f", v)))
	}

	return attr
}

// Contains returns if the collection contains the object
func (ns *NumberSet) Contains(obj Object) bool {
	if obj.Type() == ObjectTypeNumberSet {
		nsInput, ok := obj.(*NumberSet)
		if !ok {
			return false
		}

		for str := range nsInput.Value {
			if !ns.Value[str] {
				return false
			}
		}

		return true
	}

	n, ok := obj.(*Number)
	if !ok {
		return false
	}

	return ns.Value[n.Value]
}

// CanContain whether or not the number set can contain the objType
func (ns *NumberSet) CanContain(objType ObjectType) bool {
	return objType == ObjectTypeNumber || objType == ObjectTypeNumberSet
}
