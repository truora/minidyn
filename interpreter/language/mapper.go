package language

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// MapToObject convert an dynamodb attribute value to an object representation
func MapToObject(val *dynamodb.AttributeValue) (Object, error) {
	switch {
	case val.BOOL != nil:
		b := *val.BOOL
		if b {
			return TRUE, nil
		}

		return FALSE, nil
	case val.N != nil:
		n, err := strconv.ParseFloat(*val.N, 64)

		return &Number{Value: n}, err
	case val.S != nil:
		return &String{Value: *val.S}, nil
	case val.NULL != nil && *val.NULL:
		return &Null{}, nil
	}

	return mapComplexAttributeToObject(val)
}

func mapComplexAttributeToObject(val *dynamodb.AttributeValue) (Object, error) {
	switch {
	case len(val.B) != 0:
		b := make([]byte, len(val.B))
		copy(b, val.B)

		return &Binary{
			Value: b,
		}, nil
	case val.M != nil:
		return mapAttributeToMap(val)
	case val.L != nil:
		return mapAttributeToList(val)
	case val.SS != nil:
		return mapAttributeToStringSet(val)
	case val.BS != nil:
		return mapAttributeToBinarySet(val)
	case val.NS != nil:
		return mapAttributeToNumberSet(val)
	}

	return nil, fmt.Errorf("value type is not supported yet %#v", val)
}

func mapAttributeToMap(val *dynamodb.AttributeValue) (Object, error) {
	m := make(map[string]Object)

	for k, attr := range val.M {
		obj, err := MapToObject(attr)
		if err != nil {
			return nil, err
		}

		m[k] = obj
	}

	return &Map{
		Value: m,
	}, nil
}

func mapAttributeToList(val *dynamodb.AttributeValue) (Object, error) {
	l := make([]Object, len(val.L))

	for i, attr := range val.L {
		obj, err := MapToObject(attr)
		if err != nil {
			return nil, err
		}

		l[i] = obj
	}

	return &List{
		Value: l,
	}, nil
}

func mapAttributeToStringSet(val *dynamodb.AttributeValue) (Object, error) {
	ss := map[string]bool{}

	for _, val := range val.SS {
		ss[*val] = true
	}

	return &StringSet{
		Value: ss,
	}, nil
}

func mapAttributeToBinarySet(val *dynamodb.AttributeValue) (Object, error) {
	bs := BinarySet{
		Value: make([][]byte, 0, len(val.BS)),
	}

	for _, val := range val.BS {
		if containedInBinaryArray(bs.Value, val) {
			continue
		}

		bs.Value = append(bs.Value, val)
	}

	return &bs, nil
}

func mapAttributeToNumberSet(val *dynamodb.AttributeValue) (Object, error) {
	ns := map[float64]bool{}

	for _, val := range val.NS {
		n, err := strconv.ParseFloat(*val, 64)
		if err != nil {
			return nil, err
		}

		ns[n] = true
	}

	return &NumberSet{
		Value: ns,
	}, nil
}
