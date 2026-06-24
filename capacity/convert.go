package capacity

import (
	"encoding/base64"

	docsize "github.com/truora/minidyn/calculate-size"
	"github.com/truora/minidyn/types"
)

// Size returns the estimated DynamoDB byte size of an item. A nil/empty item is 0.
// docsize returns 0 on any conversion error, so capacity accounting never breaks an operation.
func Size(item map[string]*types.Item) int {
	n, _ := docsize.CalculateDocumentSize(itemToDoc(item))

	return n
}

// SumSize returns the combined Size of every item (used by Query/Scan/batch accounting).
func SumSize(items []map[string]*types.Item) int {
	total := 0
	for _, item := range items {
		total += Size(item)
	}

	return total
}

// itemToDoc converts minidyn's internal item form into the low-level DynamoDB-JSON
// map[string]any that docsize.CalculateDocumentSize expects.
func itemToDoc(item map[string]*types.Item) map[string]any {
	doc := make(map[string]any, len(item))
	for name, value := range item {
		doc[name] = attrToAny(value)
	}

	return doc
}

// attrToAny renders a single types.Item as a one-key AttributeValue map. It branches on
// the first populated field using the same precedence as the SDK/wire mappers.
func attrToAny(it *types.Item) map[string]any {
	if it == nil {
		return map[string]any{"NULL": true}
	}

	switch {
	case len(it.B) != 0:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(it.B)}
	case it.BOOL != nil:
		return map[string]any{"BOOL": *it.BOOL}
	case len(it.BS) != 0:
		return map[string]any{"BS": encodeBinarySet(it.BS)}
	case it.N != nil:
		return map[string]any{"N": *it.N}
	case len(it.NS) != 0:
		return map[string]any{"NS": derefStrings(it.NS)}
	case it.S != nil:
		return map[string]any{"S": *it.S}
	case len(it.SS) != 0:
		return map[string]any{"SS": derefStrings(it.SS)}
	case len(it.L) != 0:
		return map[string]any{"L": listToAny(it.L)}
	case len(it.M) != 0:
		return map[string]any{"M": itemToDoc(it.M)}
	case it.NULL != nil:
		return map[string]any{"NULL": *it.NULL}
	default:
		return map[string]any{"NULL": true}
	}
}

func listToAny(items []*types.Item) []any {
	out := make([]any, len(items))
	for i, el := range items {
		out[i] = attrToAny(el)
	}

	return out
}

func encodeBinarySet(bs [][]byte) []any {
	out := make([]any, len(bs))
	for i, b := range bs {
		out[i] = base64.StdEncoding.EncodeToString(b)
	}

	return out
}

func derefStrings(ptrs []*string) []any {
	out := make([]any, len(ptrs))
	for i, p := range ptrs {
		if p != nil {
			out[i] = *p
		} else {
			out[i] = ""
		}
	}

	return out
}
