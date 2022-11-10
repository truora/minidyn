package language

import (
	"fmt"
	"strconv"

	"github.com/truora/minidyn/types"
)

// MapToObject convert an types attribute value to an object representation
func MapToObject(val *types.Item) (Object, error) {
	switch {
	case val.BOOL != nil:
		b := *val.BOOL
		if b {
			return TRUE, nil
		}

		return FALSE, nil
	case val.N != nil:
		n, err := strconv.ParseFloat(types.StringValue(val.N), 64)

		return &Number{Value: n}, err
	case val.S != nil:
		return &String{Value: types.StringValue(val.S)}, nil
	case val.NULL != nil && *val.NULL:
		return &Null{}, nil
	}

	return mapComplexAttributeToObject(val)
}

func mapComplexAttributeToObject(val *types.Item) (Object, error) {
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

func mapAttributeToMap(val *types.Item) (Object, error) {
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

func mapAttributeToList(val *types.Item) (Object, error) {
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

func mapAttributeToStringSet(val *types.Item) (Object, error) {
	ss := map[string]bool{}

	for _, val := range val.SS {
		ss[types.StringValue(val)] = true
	}

	return &StringSet{
		Value: ss,
	}, nil
}

func mapAttributeToBinarySet(val *types.Item) (Object, error) {
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

func mapAttributeToNumberSet(val *types.Item) (Object, error) {
	ns := map[float64]bool{}

	for _, val := range val.NS {
		n, err := strconv.ParseFloat(types.StringValue(val), 64)
		if err != nil {
			return nil, err
		}

		ns[n] = true
	}

	return &NumberSet{
		Value: ns,
	}, nil
}
