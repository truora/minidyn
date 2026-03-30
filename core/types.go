package core

import (
	"errors"
	"fmt"
	"maps"
	"reflect"

	"github.com/truora/minidyn/types"
)

var (
	// revive:disable-next-line
	errMissingField = errors.New("One of the required keys was not given a value") //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors

	// ErrConditionalRequestFailed when the conditional write is not meet
	ErrConditionalRequestFailed = errors.New("Conditional request failed") //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors

	// ErrInvalidAtrributeValue when the attributte value is invalid
	ErrInvalidAtrributeValue = errors.New("Invalid attribute value type") //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors
)

const (
	// PrimaryIndexName default primary index name
	PrimaryIndexName = ""
)

func copyItem(item map[string]*types.Item) map[string]*types.Item {
	copy := map[string]*types.Item{}
	maps.Copy(copy, item)

	return copy
}

func mapSliceType(t reflect.Type) string {
	e := t.Elem()

	if e.Kind() == reflect.Uint8 {
		return "B"
	}

	return "L"
}

func mapToDynamoDBType(v any) string {
	t := reflect.TypeOf(v)
	if t == nil {
		return "NULL"
	}

	switch k := t.Kind(); {
	case k == reflect.String:
		return "S"
	case k == reflect.Bool:
		return "BOOL"
	case (k >= reflect.Int && k <= reflect.Float64):
		return "N"
	case (k == reflect.Map || k == reflect.Struct):
		return "M"
	case k == reflect.Slice:
		return mapSliceType(t)
	}

	return ""
}

func getItemValue(item map[string]*types.Item, field, typ string) (any, error) {
	val, ok := item[field]
	if !ok {
		return nil, fmt.Errorf("%w; field: %q", errMissingField, field)
	}

	goVal, ok := getGoValue(val, typ)
	if !ok {
		// revive:disable-next-line
		return nil, fmt.Errorf("%w; field %q", ErrInvalidAtrributeValue, field)
	}

	return goVal, nil
}

func getGoValue(val *types.Item, typ string) (any, bool) {
	switch typ {
	case "S":
		return types.StringValue(val.S), val.S != nil
	case "BOOL":
		return val.BOOL, val.BOOL != nil
	case "N":
		return types.StringValue(val.N), val.N != nil
	}

	return getGoComplexValue(val, typ)
}

func getGoComplexValue(val *types.Item, typ string) (any, bool) {
	switch typ {
	case "B":
		return val.B, val.B != nil
	case "L":
		return val.L, val.L != nil
	case "M":
		return val.M, val.M != nil
	case "BS":
		return val.BS, val.BS != nil
	case "SS":
		return val.SS, val.SS != nil
	case "NS":
		return val.NS, val.NS != nil
	}

	return nil, false
}
