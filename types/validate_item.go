package types

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// ValidateItemAttributeValue walks an attribute value, including nested List (L) and Map (M)
// entries, and returns the first problem DynamoDB rejects, in the order it meets them: an N or
// NS member that is not a number DynamoDB stores (see ValidateNumber), a String Set (SS),
// Number Set (NS) or Binary Set (BS) with duplicate members, or a nil element of an L or value
// of an M. Every problem is a *ValueError: its text is DynamoDB's, followed by minidyn detail.
// Callers typically wrap the result with NewError("ValidationException", msg, nil).
//
// A nil av is valid at the top level. NS duplicates are detected by exact wire string equality
// of each element (e.g. "1" and "1.0" are not duplicates). SS uses the same string comparison
// as StringValue. BS duplicates use bytes.Equal (nil and empty slices compare equal).
func ValidateItemAttributeValue(av *Item) error {
	// Not "return validateValue(av)": a nil *ValueError would become a non-nil error.
	if ve := validateValue(av); ve != nil { //nolint:revive // if-return: typed nil
		return ve
	}

	return nil
}

// ValidateItemMap runs ValidateItemAttributeValue on every value in m, naming each attribute in
// the path of a number error. A nil value is rejected as DynamoDB rejects a null
// AttributeValue. Nil or empty maps are valid.
func ValidateItemMap(m map[string]*Item) error {
	return validateEntries(m, func(_ string, ve *ValueError) error { return ve })
}

// ValidateExpressionAttributeValues is ValidateItemMap for the values of an expression. A
// problem is worded the way DynamoDB Local words it for a placeholder:
// "ExpressionAttributeValues contains invalid value: <text> for key :v". A placeholder that is
// itself null has no attribute to name, so its text is the bare one.
func ValidateExpressionAttributeValues(m map[string]*Item) error {
	return validateEntries(m, func(key string, ve *ValueError) error {
		return &expressionValueError{value: ve, key: key}
	})
}

// validateEntries validates each value of m, and returns what wrap makes of the first problem.
func validateEntries(m map[string]*Item, wrap func(name string, ve *ValueError) error) error {
	for name, v := range m {
		var ve *ValueError

		if v == nil {
			ve = &ValueError{kind: kindNullValue, attribute: name, top: true}
		} else if ve = validateValue(v); ve != nil {
			ve.prependKey(name)
		}

		if ve != nil {
			return wrap(name, ve)
		}
	}

	return nil
}

type valueErrorKind int

const (
	kindNumber valueErrorKind = iota
	kindDuplicate
	kindNullValue
	kindNullElement
)

// nullAttributeValueText is DynamoDB's text for an attribute value that is JSON null. A Go
// caller of the aws-v2 client sends the same shape as a nil AttributeValue (#268).
const nullAttributeValueText = "The supplied Item contains a null AttributeValue"

// ValueError is a problem DynamoDB finds in an attribute value: a number it cannot store, a
// set with a duplicate member, or a null. Its text is DynamoDB's followed by minidyn detail,
// and a number error unwraps to the number error (ErrInvalidNumber, ErrNumberOverflow, ...).
type ValueError struct {
	kind valueErrorKind
	// cause is DynamoDB's text of a number or duplicate error.
	cause error
	// path locates a number error, built on the way out of the walk, only when there is one.
	path   string
	number string
	// attribute is the name of a null value of a map; top is set when it is a value of the
	// map being validated rather than one nested in it.
	attribute string
	top       bool
	// index is the position of a null element of a list.
	index int
}

// message is DynamoDB's text.
func (e *ValueError) message() string {
	switch e.kind {
	case kindNullValue:
		return nullAttributeValueText + ": " + e.attribute + " is null"
	case kindNullElement:
		return nullAttributeValueText
	default:
		return e.cause.Error()
	}
}

// detail is what minidyn adds to the text.
func (e *ValueError) detail() string {
	switch e.kind {
	case kindNumber:
		if e.path == "" {
			return Detail("number", e.number)
		}

		return Detail("path", e.path, "number", e.number)
	case kindNullValue:
		return Detail("attribute", e.attribute)
	case kindNullElement:
		return Detail("reason", "list element "+strconv.Itoa(e.index)+" is nil")
	default:
		return ""
	}
}

func (e *ValueError) Error() string { return e.message() + e.detail() }

func (e *ValueError) Unwrap() error { return e.cause }

// prependKey and prependIndex place the entry the error was found under at the start of its path.
func (e *ValueError) prependKey(key string) { e.path = key + pathSeparator(e.path) + e.path }

func (e *ValueError) prependIndex(i int) {
	e.path = "[" + strconv.Itoa(i) + "]" + pathSeparator(e.path) + e.path
}

func pathSeparator(path string) string {
	if path == "" || path[0] == '[' {
		return ""
	}

	return "."
}

// expressionValueError is a ValueError of the value bound to an expression placeholder.
type expressionValueError struct {
	value *ValueError
	key   string
}

func (e *expressionValueError) Error() string {
	text := e.value.message()
	if e.value.kind == kindNullValue && e.value.top {
		text = nullAttributeValueText
	}

	return "ExpressionAttributeValues contains invalid value: " + text + " for key " + e.key + e.value.detail()
}

func (e *expressionValueError) Unwrap() error { return e.value }

func checkNumber(s string) *ValueError {
	if err := ValidateNumber(s); err != nil {
		return &ValueError{kind: kindNumber, cause: err, number: s}
	}

	return nil
}

func validateValue(av *Item) *ValueError {
	if av == nil {
		return nil
	}

	if av.N != nil {
		if ve := checkNumber(*av.N); ve != nil {
			return ve
		}
	}

	if ve := validateStringSet(av.SS); ve != nil {
		return ve
	}

	if ve := validateNumberSet(av.NS); ve != nil {
		return ve
	}

	if ve := validateBinarySet(av.BS); ve != nil {
		return ve
	}

	return validateNested(av)
}

// validateNested walks the elements of an L and the values of an M. It builds no path: an error
// gets it as it leaves each level.
func validateNested(av *Item) *ValueError {
	for i, elem := range av.L {
		if elem == nil {
			return &ValueError{kind: kindNullElement, index: i}
		}

		if ve := validateValue(elem); ve != nil {
			ve.prependIndex(i)

			return ve
		}
	}

	for k, v := range av.M {
		if v == nil {
			return &ValueError{kind: kindNullValue, attribute: k}
		}

		if ve := validateValue(v); ve != nil {
			ve.prependKey(k)

			return ve
		}
	}

	return nil
}

func duplicateCollectionError(displayParts []string) *ValueError {
	inner := strings.Join(displayParts, ", ")

	// Wording matches DynamoDB ValidationException; linters prefer generic Go error style.
	//nolint:revive,staticcheck // error-strings / ST1005 — message aligned with AWS.
	return &ValueError{kind: kindDuplicate, cause: fmt.Errorf("One or more parameter values were invalid: Input collection [%s] contains duplicates.", inner)}
}

func validateStringSet(ss []*string) *ValueError {
	if len(ss) < 2 {
		return nil
	}

	parts := make([]string, len(ss))
	for i, p := range ss {
		parts[i] = StringValue(p)
	}

	seen := make(map[string]struct{}, len(parts))
	for _, v := range parts {
		if _, ok := seen[v]; ok {
			return duplicateCollectionError(parts)
		}

		seen[v] = struct{}{}
	}

	return nil
}

// validateNumberSet checks the members in order, as DynamoDB does: each must be a number, and
// the first repeated one is the duplicate. Duplicates follow the exact string sent for each N.
func validateNumberSet(ns []*string) *ValueError {
	seen := make(map[string]struct{}, len(ns))

	for _, p := range ns {
		v := StringValue(p)

		if ve := checkNumber(v); ve != nil {
			return ve
		}

		if _, ok := seen[v]; ok {
			parts := make([]string, len(ns))
			for i, q := range ns {
				parts[i] = StringValue(q)
			}

			return duplicateCollectionError(parts)
		}

		seen[v] = struct{}{}
	}

	return nil
}

func validateBinarySet(bs [][]byte) *ValueError {
	if len(bs) < 2 {
		return nil
	}

	parts := make([]string, len(bs))
	for i, b := range bs {
		parts[i] = base64.StdEncoding.EncodeToString(b)
	}

	for i := range bs {
		for j := i + 1; j < len(bs); j++ {
			if bytes.Equal(bs[i], bs[j]) {
				return duplicateCollectionError(parts)
			}
		}
	}

	return nil
}
