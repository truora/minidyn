package docsize

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// sizeAttributeValue sizes one DynamoDB AttributeValue object (exactly one of S, N, M, L, ...).
// This matches AWS item accounting and https://zaccharles.github.io/dynamodb-calculator/ (not dynamodb-size-js Map-wrapping).
func sizeAttributeValue(attr map[string]any) (int, error) { //nolint:gocognit,gocyclo // single switch over every AttributeValue type
	if len(attr) != 1 {
		return 0, fmt.Errorf("attribute value must have exactly one type key, got %d keys", len(attr))
	}

	for k, v := range attr {
		switch k {
		case "S":
			s, ok := v.(string)
			if !ok {
				return 0, fmt.Errorf("value for S must be string, got %T", v)
			}

			return calculateString(s), nil
		case "N":
			ns, err := coerceNumberString(v)
			if err != nil {
				return 0, err
			}

			return dynamoNumberSizeBytes(ns), nil
		case "B":
			s, ok := v.(string)
			if !ok {
				return 0, fmt.Errorf("value for B must be base64 string, got %T", v)
			}

			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return 0, fmt.Errorf("decoding B as base64: %w", err)
			}

			return len(raw), nil
		case "BOOL":
			switch v.(type) {
			case bool, string:
				return calculateBoolean(), nil
			default:
				return 0, fmt.Errorf("BOOL value must be bool or string, got %T", v)
			}
		case "NULL":
			switch v.(type) {
			case bool, string:
				return calculateNull(), nil
			default:
				return 0, fmt.Errorf("NULL value must be bool or string, got %T", v)
			}
		case "SS":
			arr, err := coerceStringSlice(v)
			if err != nil {
				return 0, err
			}

			sum := 0

			for _, s := range arr {
				sum += calculateString(s)
			}

			return sum, nil
		case "NS":
			arr, ok := v.([]any)
			if !ok {
				return 0, fmt.Errorf("NS value must be array, got %T", v)
			}

			sum := 0

			for i, el := range arr {
				ns, err := coerceNumberString(el)
				if err != nil {
					return 0, fmt.Errorf("NS[%d]: %w", i, err)
				}

				sum += dynamoNumberSizeBytes(ns)
			}

			return sum, nil
		case "BS":
			arr, ok := v.([]any)
			if !ok {
				return 0, fmt.Errorf("BS value must be array, got %T", v)
			}

			sum := 0

			for i, el := range arr {
				s, ok := el.(string)
				if !ok {
					return 0, fmt.Errorf("BS[%d] must be base64 string, got %T", i, el)
				}

				raw, err := base64.StdEncoding.DecodeString(s)
				if err != nil {
					return 0, fmt.Errorf("BS[%d] base64: %w", i, err)
				}

				sum += len(raw)
			}

			return sum, nil
		case "M":
			m, ok := v.(map[string]any)
			if !ok {
				return 0, fmt.Errorf("value for M must be object, got %T", v)
			}

			return sizeDynamoMapValue(m)
		case "L":
			arr, ok := v.([]any)
			if !ok {
				return 0, fmt.Errorf("value for L must be array, got %T", v)
			}

			return sizeDynamoListValue(arr)
		default:
			return 0, fmt.Errorf("unknown attribute type key %q", k)
		}
	}

	return 0, fmt.Errorf("empty attribute value")
}

func sizeDynamoMapValue(m map[string]any) (int, error) {
	size := compositeItemOverhead

	for key, val := range m {
		av, ok := val.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("map key %q: expected AttributeValue object, got %T", key, val)
		}

		inner, err := sizeAttributeValue(av)
		if err != nil {
			return 0, fmt.Errorf("map key %q: %w", key, err)
		}

		size += calculateString(key) + inner + nestedItemOverhead
	}

	return size, nil
}

func sizeDynamoListValue(items []any) (int, error) {
	size := compositeItemOverhead

	for i, el := range items {
		av, ok := el.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("list index %d: expected AttributeValue object, got %T", i, el)
		}

		inner, err := sizeAttributeValue(av)
		if err != nil {
			return 0, fmt.Errorf("list index %d: %w", i, err)
		}

		size += inner + nestedItemOverhead
	}

	return size, nil
}

func coerceNumberString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case json.Number:
		return string(x), nil
	default:
		return "", fmt.Errorf("value for N must be string or number, got %T", v)
	}
}

func coerceStringSlice(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("SS value must be array, got %T", v)
	}

	out := make([]string, len(arr))

	for i, el := range arr {
		s, ok := el.(string)
		if !ok {
			return nil, fmt.Errorf("SS[%d] must be string, got %T", i, el)
		}

		out[i] = s
	}

	return out, nil
}
