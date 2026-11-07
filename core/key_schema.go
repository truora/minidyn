package core

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/truora/minidyn/types"
)

// errInvalidKeyConditionCount matches DynamoDB ValidationException when the Key map
// does not contain exactly the expected number of attributes (hash + range if defined).
//
//nolint:stylecheck,staticcheck,ST1005 // AWS error message
var errInvalidKeyConditionCount = errors.New("The number of conditions on the keys is invalid")

type keySchema struct {
	HashKey   string
	RangeKey  string
	Secondary bool
}

// validatePrimaryKeyMap ensures Key-shaped maps (GetItem, DeleteItem, UpdateItem)
// contain exactly the hash key and, when applicable, the range key attribute names.
func (ks keySchema) validatePrimaryKeyMap(key map[string]*types.Item) error {
	if ks.Secondary {
		return nil
	}

	want := 1
	if ks.RangeKey != "" {
		want = 2
	}

	if len(key) != want {
		return fmt.Errorf("%w%s", errInvalidKeyConditionCount,
			types.Detail("expected keys", strconv.Itoa(want), "got keys", strconv.Itoa(len(key))))
	}

	if _, ok := key[ks.HashKey]; !ok {
		return fmt.Errorf("%w%s", errMissingField, types.Detail("missing", "hash key"))
	}

	if ks.RangeKey != "" {
		if _, ok := key[ks.RangeKey]; !ok {
			return fmt.Errorf("%w%s", errMissingField, types.Detail("missing", "range key"))
		}
	}

	return nil
}

func (ks keySchema) GetKey(attrs map[string]string, item map[string]*types.Item) (string, error) {
	key, err := ks.getKeyValue(attrs, item)
	if ks.Secondary && errors.Is(err, errMissingField) {
		// secondary indexes are sparse
		err = nil
	}

	return key, err
}

// keyAttrValue reads a key attribute, reporting a DynamoDB-style type mismatch
// for primary keys whose value type differs from the declared one. See
// ErrKeyTypeMismatch for why the detail goes after " | ". Values with no
// recognizable type (nil or empty) keep the generic invalid attribute error.
func (ks keySchema) keyAttrValue(attrs map[string]string, item map[string]*types.Item, field string) (any, error) {
	val, err := getItemValue(item, field, attrs[field])
	if err != nil && !ks.Secondary && errors.Is(err, ErrInvalidAtrributeValue) && attrs[field] != "" {
		if actual := itemTypeName(item[field]); actual != "" {
			return nil, fmt.Errorf("%w%s", ErrKeyTypeMismatch,
				types.Detail("field", field, "expected", attrs[field], "actual", actual))
		}
	}

	return val, err
}

func itemTypeName(it *types.Item) string {
	switch {
	case it == nil:
		return ""
	case it.S != nil:
		return "S"
	case it.N != nil:
		return "N"
	case it.B != nil:
		return "B"
	case it.BOOL != nil:
		return "BOOL"
	case it.NULL != nil:
		return "NULL"
	case it.SS != nil:
		return "SS"
	case it.NS != nil:
		return "NS"
	case it.BS != nil:
		return "BS"
	case it.L != nil:
		return "L"
	case it.M != nil:
		return "M"
	}

	return ""
}

// getKeyValue returns the encoded key of item (see key_encoding.go): the hash key part,
// followed by the range key part when the schema has one.
func (ks keySchema) getKeyValue(attrs map[string]string, item map[string]*types.Item) (string, error) {
	val, err := ks.keyAttrValue(attrs, item, ks.HashKey)
	if err != nil {
		return "", err
	}

	key, err := appendKeyPart(make([]byte, 0, 32), ks.HashKey, attrs[ks.HashKey], val)
	if err != nil {
		return "", err
	}

	if ks.RangeKey != "" {
		val, err = ks.keyAttrValue(attrs, item, ks.RangeKey)
		if err != nil {
			return "", err
		}

		key, err = appendKeyPart(key, ks.RangeKey, attrs[ks.RangeKey], val)
		if err != nil {
			return "", err
		}
	}

	return string(key), nil
}

func (ks *keySchema) describe() []types.KeySchemaElement {
	desc := []types.KeySchemaElement{}

	keySchemaElement := types.KeySchemaElement{
		AttributeName: ks.HashKey,
		KeyType:       "HASH",
	}
	desc = append(desc, keySchemaElement)

	if ks.RangeKey != "" {
		keySchemaElement := types.KeySchemaElement{
			AttributeName: ks.RangeKey,
			KeyType:       "RANGE",
		}
		desc = append(desc, keySchemaElement)
	}

	return desc
}

func (ks *keySchema) getKeyItem(item map[string]*types.Item) map[string]*types.Item {
	keyItem := map[string]*types.Item{}

	if v, ok := item[ks.HashKey]; ok {
		keyItem[ks.HashKey] = v
	}

	if v, ok := item[ks.RangeKey]; ok && ks.RangeKey != "" {
		keyItem[ks.RangeKey] = v
	}

	return keyItem
}
