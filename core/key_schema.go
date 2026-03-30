package core

import (
	"errors"
	"fmt"
	"strings"

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
		return fmt.Errorf("%w; expected %d keys, got %d", errInvalidKeyConditionCount, want, len(key))
	}

	if _, ok := key[ks.HashKey]; !ok {
		return fmt.Errorf("%w; missing hash key", errMissingField)
	}

	if ks.RangeKey != "" {
		if _, ok := key[ks.RangeKey]; !ok {
			return fmt.Errorf("%w; missing range key", errMissingField)
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

func (ks keySchema) getKeyValue(attrs map[string]string, item map[string]*types.Item) (string, error) {
	key := []string{}

	val, err := getItemValue(item, ks.HashKey, attrs[ks.HashKey])
	if err != nil {
		return "", err
	}

	hashKeyStr := fmt.Sprintf("%v", val)

	if ks.RangeKey == "" {
		return hashKeyStr, nil
	}

	key = append(key, hashKeyStr)

	val, err = getItemValue(item, ks.RangeKey, attrs[ks.RangeKey])
	if err != nil {
		return "", err
	}

	key = append(key, fmt.Sprintf("%v", val))

	return strings.Join(key, "."), nil
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
