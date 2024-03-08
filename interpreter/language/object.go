package language

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/truora/minidyn/types"
)

var (
	// UNDEFINED definel the global null value
	UNDEFINED = &Null{IsUndefined: true}
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

// Object abstraction of the object values
type Object interface {
	Type() ObjectType
	Inspect() string
	ToDynamoDB() types.Item
}

// ContainerObject abstraction of the object collections
type ContainerObject interface {
	Object
	Contains(obj Object) bool
	CanContain(objType ObjectType) bool
}

// AppendableObject abstraction of the objects that can add other objects
type AppendableObject interface {
	Object
	Add(obj Object) Object
}

// DetachableObject abstraction of the object that can delete other objects
type DetachableObject interface {
	Object
	Delete(obj Object) Object
}

// Number is the representation of numbers
type Number struct {
	Value float64
}

// Inspect returns the readable value of the object
func (i *Number) Inspect() string {
	return numToString(i.Value)
}

// Type returns the object type
func (i *Number) Type() ObjectType {
	return ObjectTypeNumber
}

// ToDynamoDB returns the types attribute value
func (i *Number) ToDynamoDB() types.Item {
	str := numToString(i.Value)

	return types.Item{N: types.ToString(str)}
}

func numToString(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// Add if the obj is an number it adds the value to the number
func (i *Number) Add(obj Object) Object {
	n, ok := obj.(*Number)
	if !ok {
		return newError("Incorrect operand type for operator or function; operator: ADD, operand type: %s", obj.Type())
	}

	i.Value += n.Value

	return UNDEFINED
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

// ToDynamoDB returns the types attribute value
func (b *Boolean) ToDynamoDB() types.Item {
	return types.Item{BOOL: &b.Value}
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

// ToDynamoDB returns the types attribute value
func (b *Binary) ToDynamoDB() types.Item {
	return types.Item{B: b.Value}
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
type Null struct {
	IsUndefined bool
}

// Type returns the object type
func (n *Null) Type() ObjectType { return ObjectTypeNull }

// Inspect returns the readable value of the object
func (n *Null) Inspect() string {
	return "null"
}

// ToDynamoDB returns the types attribute value
func (n *Null) ToDynamoDB() types.Item {
	return types.Item{NULL: &TRUE.Value}
}

// Error is the representation of errors
type Error struct {
	Message string
}

// Type returns the object type
func (e *Error) Type() ObjectType { return ObjectTypeError }

// Inspect returns the readable value of the object
func (e *Error) Inspect() string { return "ERROR: " + e.Message }

// ToDynamoDB returns the types attribute value
func (e *Error) ToDynamoDB() types.Item {
	return types.Item{}
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

// ToDynamoDB returns the types attribute value
func (s *String) ToDynamoDB() types.Item {
	return types.Item{S: types.ToString(s.Value)}
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

	out.WriteString("{\n")

	keys := make([]string, 0, len(m.Value))

	for k := range m.Value {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		obj := m.Value[k]

		out.WriteString("\t\"")
		out.WriteString(k)
		out.WriteString("\" : ")

		str := obj.Inspect()

		if obj.Type() == ObjectTypeMap {
			str = strings.ReplaceAll(str, "\n", "\n\t")
		}

		out.WriteString(str)

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

// ToDynamoDB returns the types attribute value
func (m *Map) ToDynamoDB() types.Item {
	attr := types.Item{M: map[string]*types.Item{}}

	for k, v := range m.Value {
		value := v.ToDynamoDB()
		attr.M[k] = &value
	}

	return attr
}

// Get returns the contained object in the field
func (m *Map) Get(field string) Object {
	obj, ok := m.Value[field]
	if !ok {
		return UNDEFINED
	}

	return obj
}

// List is the representation of list
type List struct {
	Value []Object
	dirty bool
}

// Inspect returns the readable value of the object
func (l *List) Inspect() string {
	var out bytes.Buffer

	out.WriteString("[ ")

	for _, obj := range l.Value {
		if obj == nil {
			continue
		}

		out.WriteString(obj.Inspect())
		out.WriteString("<")
		out.WriteString(string(obj.Type()))
		out.WriteString("> ")
	}

	out.WriteString("]")

	if l.dirty {
		out.WriteString("<Dirty>")
	}

	return out.String()
}

// Remove eliminates the value form the element in the pos index
// the element replaces with a nil value.
func (l *List) Remove(pos int64) Object {
	if int64(len(l.Value)) > pos {
		l.Value[pos] = nil
		l.dirty = true

		return UNDEFINED
	}

	// types does nothing when the index greater than the list size

	return UNDEFINED
}

// Compact removes nil elements from the list
func (l *List) Compact() {
	copy := make([]Object, 0, len(l.Value))

	for _, obj := range l.Value {
		if obj == nil {
			continue
		}

		copy = append(copy, obj)
	}

	l.Value = copy
	l.dirty = false
}

// Type returns the object type
func (l *List) Type() ObjectType {
	return ObjectTypeList
}

// ToDynamoDB returns the types attribute value
func (l *List) ToDynamoDB() types.Item {
	attr := types.Item{L: []*types.Item{}}

	for _, v := range l.Value {
		value := v.ToDynamoDB()
		attr.L = append(attr.L, &value)
	}

	return attr
}

// Get returns the contained object in the position
func (l *List) Get(position int64) Object {
	obj := l.Value[position]
	if obj == nil {
		return UNDEFINED
	}

	return obj
}

// Contains whether or not the obj is contained in the list
func (l *List) Contains(obj Object) bool {
	for _, e := range l.Value {
		if equalObject(obj, e) {
			return true
		}
	}

	return false
}

// CanContain whether or not the list can contain the objType
func (l *List) CanContain(objType ObjectType) bool {
	return true
}

// Add if the obj adds the value to the list
func (l *List) Add(obj Object) Object {
	if obj.Type() == ObjectTypeList {
		list := obj.(*List)
		l.Value = append(l.Value, list.Value...)

		return UNDEFINED
	}

	l.Value = append(l.Value, obj)

	return UNDEFINED
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

// ToDynamoDB returns the types attribute value
func (ss *StringSet) ToDynamoDB() types.Item {
	attr := types.Item{SS: make([]*string, 0, len(ss.Value))}

	for v := range ss.Value {
		attr.SS = append(attr.SS, types.ToString(v))
	}

	return attr
}

// CanContain whether or not the string set can contain the objType
func (ss *StringSet) CanContain(objType ObjectType) bool {
	return objType == ObjectTypeString || objType == ObjectTypeStringSet
}

// Add if the obj is an string it adds the value to Set
func (ss *StringSet) Add(obj Object) Object {
	switch obj.Type() {
	case ObjectTypeStringSet:
		ssInput, ok := obj.(*StringSet)
		if ok {
			for str := range ssInput.Value {
				ss.Value[str] = true
			}

			return UNDEFINED
		}
	case ObjectTypeString:
		str, ok := obj.(*String)
		if ok {
			ss.Value[str.Value] = true

			return UNDEFINED
		}
	}

	return newError("Incorrect operand type for operator or function; operator: ADD, operand type: %s", obj.Type())
}

// Delete if the obj is an string it removes the value from the Set
func (ss *StringSet) Delete(obj Object) Object {
	switch obj.Type() {
	case ObjectTypeStringSet:
		ssInput, ok := obj.(*StringSet)
		if ok {
			for str := range ssInput.Value {
				delete(ss.Value, str)
			}

			return UNDEFINED
		}
	case ObjectTypeString:
		str, ok := obj.(*String)
		if ok {
			delete(ss.Value, str.Value)

			return UNDEFINED
		}
	}

	return newError("Incorrect operand type for operator or function; operator: REMOVE, operand type: %s", obj.Type())
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

func removeBinaries(container [][]byte, others [][]byte) [][]byte {
	out := [][]byte{}

	for _, element := range container {
		if containedInBinaryArray(others, element) {
			continue
		}

		out = append(out, element)
	}

	return out
}

// ToDynamoDB returns the types attribute value
func (bs *BinarySet) ToDynamoDB() types.Item {
	return types.Item{BS: bs.Value}
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

// Delete if the obj is an binary it removes the value from the Set
func (bs *BinarySet) Delete(obj Object) Object {
	switch obj.Type() {
	case ObjectTypeBinarySet:
		bsInput, ok := obj.(*BinarySet)
		if ok {
			bs.Value = removeBinaries(bs.Value, bsInput.Value)

			return UNDEFINED
		}
	case ObjectTypeBinary:
		bin, ok := obj.(*Binary)
		if ok {
			bs.Value = removeBinaries(bs.Value, [][]byte{bin.Value})

			return UNDEFINED
		}
	}

	return newError("Incorrect operand type for operator or function; operator: REMOVE, operand type: %s", obj.Type())
}

func (bs *BinarySet) addBinarySetValues(binaryValues [][]byte) {
	for _, bin := range binaryValues {
		if containedInBinaryArray(bs.Value, bin) {
			continue
		}

		bs.Value = append(bs.Value, bin)
	}
}

// Add if the obj is a binary it adds the value to Set
func (bs *BinarySet) Add(obj Object) Object {
	switch obj.Type() {
	case ObjectTypeBinarySet:
		bsInput, ok := obj.(*BinarySet)
		if ok {
			bs.addBinarySetValues(bsInput.Value)
		}

		return UNDEFINED
	case ObjectTypeBinary:
		bin, ok := obj.(*Binary)
		if ok && !bs.Contains(bin) {
			bs.Value = append(bs.Value, bin.Value)
		}

		return UNDEFINED
	}

	return newError("Incorrect operand type for operator or function; operator: ADD, operand type: %s", obj.Type())
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

// ToDynamoDB returns the types attribute value
func (ns *NumberSet) ToDynamoDB() types.Item {
	attr := types.Item{NS: make([]*string, 0, len(ns.Value))}

	for v := range ns.Value {
		str := numToString(v)

		attr.NS = append(attr.NS, types.ToString(str))
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

// Add if the obj is an number or number set it adds the value to Set
func (ns *NumberSet) Add(obj Object) Object {
	switch obj.Type() {
	case ObjectTypeNumberSet:
		nsInput, ok := obj.(*NumberSet)
		if ok {
			for n := range nsInput.Value {
				ns.Value[n] = true
			}

			return UNDEFINED
		}
	case ObjectTypeNumber:
		n, ok := obj.(*Number)
		if ok {
			ns.Value[n.Value] = true

			return UNDEFINED
		}
	}

	return newError("Incorrect operand type for operator or function; operator: ADD, operand type: %s", obj.Type())
}

// Delete if the obj is an number or number set it removes the value from the Set
func (ns *NumberSet) Delete(obj Object) Object {
	switch obj.Type() {
	case ObjectTypeNumberSet:
		nsInput, ok := obj.(*NumberSet)
		if ok {
			for n := range nsInput.Value {
				delete(ns.Value, n)
			}

			return UNDEFINED
		}
	case ObjectTypeNumber:
		n, ok := obj.(*Number)
		if ok {
			delete(ns.Value, n.Value)

			return UNDEFINED
		}
	}

	return newError("Incorrect operand type for operator or function; operator: REMOVE, operand type: %s", obj.Type())
}
