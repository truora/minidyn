package docsize

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	nestedItemOverhead    = 1
	compositeItemOverhead = 3
)

// DocumentSet mirrors the AWS SDK for JavaScript DocumentClient "Set" wrapper
// (wrapperName === 'Set') so set attributes can be sized when not using DynamoDB JSON.
type DocumentSet struct {
	// Type is one of: String, Number, NumberValue, Binary.
	Type string
	// Values holds set elements (strings, numbers, or binary blobs per Type).
	Values []any
}

// CalculateDocumentSize returns the estimated byte size of a DynamoDB item: sum over
// attributes of (UTF-8 length of attribute name + size of the AttributeValue).
func CalculateDocumentSize(doc map[string]any) (int, error) {
	if doc == nil {
		return 0, fmt.Errorf("doc: expected non-nil map")
	}

	total := 0

	for name, val := range doc {
		total += calculateString(name)

		switch x := val.(type) {
		case map[string]any:
			sz, err := sizeAttributeValue(x)
			if err != nil {
				return 0, fmt.Errorf("attribute %q: %w", name, err)
			}

			total += sz
		case DocumentSet:
			total += calculateSet(x)
		case *DocumentSet:
			if x == nil {
				return 0, fmt.Errorf("attribute %q: nil DocumentSet", name)
			}

			total += calculateSet(*x)
		default:
			return 0, fmt.Errorf("attribute %q: expected AttributeValue map or DocumentSet, got %T", name, val)
		}
	}

	return total, nil
}

func calculateString(s string) int {
	return len(s)
}

func calculateBoolean() int {
	return 1
}

func calculateNull() int {
	return 1
}

func calculateSet(x DocumentSet) int {
	var calc func(any) int

	switch x.Type {
	case "String":
		calc = func(v any) int {
			s, _ := v.(string)

			return calculateString(s)
		}
	case "Number", "NumberValue":
		calc = func(v any) int {
			if s, ok := v.(string); ok {
				return dynamoNumberSizeBytes(s)
			}

			f, ok := toFloat64(v)
			if !ok {
				return 0
			}

			return dynamoNumberSizeBytes(trimFloatJSON(f))
		}
	case "Binary":
		calc = calculateBinaryDecoded
	default:
		return 0
	}

	total := 0

	for _, item := range x.Values {
		total += calc(item)
	}

	return total
}

func trimFloatJSON(f float64) string {
	// Best-effort decimal string for DocumentSet numeric literals (non-DynamoDB-JSON).
	return trimTrailingZerosFloat(strconv.FormatFloat(f, 'f', -1, 64))
}

func trimTrailingZerosFloat(s string) string {
	if strings.Contains(s, ".") {
		s = trimRightChar(s, '0')
		s = trimRightChar(s, '.')
	}

	return s
}

func trimRightChar(s string, c byte) string {
	for s != "" && s[len(s)-1] == c {
		s = s[:len(s)-1]
	}

	return s
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false
		}

		return f, true
	default:
		return 0, false
	}
}

func calculateBinaryDecoded(x any) int {
	switch b := x.(type) {
	case []byte:
		return len(b)
	case string:
		raw, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return 0
		}

		return len(raw)
	default:
		return 0
	}
}
