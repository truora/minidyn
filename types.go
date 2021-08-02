package minidyn

import (
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func getItemValue(item map[string]*dynamodb.AttributeValue, field, typ string) (interface{}, bool) {
	val, ok := item[field]
	if !ok {
		return nil, false
	}

	return getGoValue(val, typ)
}

func getGoValue(val *dynamodb.AttributeValue, typ string) (interface{}, bool) {
	switch typ {
	case "S":
		return aws.StringValue(val.S), true
	case "BOOL":
		return aws.BoolValue(val.BOOL), true
	case "N":
		return aws.StringValue(val.N), true
	}

	return getGoComplexValue(val, typ)
}

func getGoComplexValue(val *dynamodb.AttributeValue, typ string) (interface{}, bool) {
	switch typ {
	case "B":
		return val.B, true
	case "L":
		return val.L, true
	case "M":
		return val.M, true
	case "BS":
		return val.BS, true
	case "SS":
		return val.SS, true
	case "NS":
		return val.NS, true
	}

	return nil, false
}
