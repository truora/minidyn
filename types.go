package minidyn

import (
	"errors"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	// revive:disable-next-line
	errMissingField = errors.New("The number of conditions on the keys is invalid")
)

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

func getItemValue(item map[string]*dynamodb.AttributeValue, field, typ string) (interface{}, error) {
	val, ok := item[field]
	if !ok {
		return nil, errMissingField
	}

	goVal, ok := getGoValue(val, typ)
	if !ok {
		// revive:disable-next-line
		return nil, errors.New("Invalid attribute value type")
	}

	return goVal, nil
}

func getGoValue(val *dynamodb.AttributeValue, typ string) (interface{}, bool) {
	switch typ {
	case "S":
		return aws.StringValue(val.S), val.S != nil
	case "BOOL":
		return aws.BoolValue(val.BOOL), val.BOOL != nil
	case "N":
		return aws.StringValue(val.N), val.N != nil
	}

	return getGoComplexValue(val, typ)
}

func getGoComplexValue(val *dynamodb.AttributeValue, typ string) (interface{}, bool) {
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
