package core

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type keySchema struct {
	HashKey   string
	RangeKey  string
	Secondary bool
}

func (ks keySchema) GetKey(attrs map[string]string, item map[string]*Item) (string, error) {
	key, err := ks.getKeyValue(attrs, item)
	if ks.Secondary && errors.Is(err, errMissingField) {
		// secondary indexes are sparse
		err = nil
	}

	return key, err
}

func (ks keySchema) getKeyValue(attrs map[string]string, item map[string]*Item) (string, error) {
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

func (ks *keySchema) describe() []*dynamodb.KeySchemaElement {
	desc := []*dynamodb.KeySchemaElement{}

	keySchemaElement := &dynamodb.KeySchemaElement{
		AttributeName: aws.String(ks.HashKey),
		KeyType:       aws.String("HASH"),
	}
	desc = append(desc, keySchemaElement)

	if ks.RangeKey != "" {
		keySchemaElement := &dynamodb.KeySchemaElement{
			AttributeName: aws.String(ks.RangeKey),
			KeyType:       aws.String("RANGE"),
		}
		desc = append(desc, keySchemaElement)
	}

	return desc
}

func (ks *keySchema) getKeyItem(item map[string]*Item) map[string]*Item {
	keyItem := map[string]*Item{}

	if v, ok := item[ks.HashKey]; ok {
		keyItem[ks.HashKey] = v
	}

	if v, ok := item[ks.RangeKey]; ok && ks.RangeKey != "" {
		keyItem[ks.RangeKey] = v
	}

	return keyItem
}
