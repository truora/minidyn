package core

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/truora/minidyn/types"
)

var (
	// revive:disable-next-line
	errMissingField = errors.New("The number of conditions on the keys is invalid")

	// ErrConditionalRequestFailed when the conditional write is not meet
	ErrConditionalRequestFailed = errors.New("the conditional request failed")
)

const (
	PrimaryIndexName = ""
)

func copyItem(item map[string]types.Item) map[string]types.Item {
	copy := map[string]types.Item{}
	for key, val := range item {
		copy[key] = val
	}

	return copy
}

func mapSliceType(t reflect.Type) string {
	e := t.Elem()

	if e.Kind() == reflect.Uint8 {
		return "B"
	}

	return "L"
}

func mapToDynamoDBType(v interface{}) string {
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

func getItemValue(item map[string]types.Item, field, typ string) (interface{}, error) {
	val, ok := item[field]
	if !ok {
		return nil, fmt.Errorf("%w; field: %q", errMissingField, field)
	}

	goVal, ok := getGoValue(val, typ)
	if !ok {
		// revive:disable-next-line
		return nil, fmt.Errorf("Invalid attribute value type; field %q", field)
	}

	return goVal, nil
}

func getGoValue(val types.Item, typ string) (interface{}, bool) {
	switch typ {
	case "S":
		return val.S, val.S != ""
	case "BOOL":
		return aws.BoolValue(val.BOOL), val.BOOL != nil
	case "N":
		return val.N, val.N != ""
	}

	return getGoComplexValue(val, typ)
}

func getGoComplexValue(val types.Item, typ string) (interface{}, bool) {
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
