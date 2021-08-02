package minidyn

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type keySchema struct {
	HashKey  string
	RangeKey string
}

func (ks keySchema) getKey(attrs map[string]string, item map[string]*dynamodb.AttributeValue) (string, bool) {
	key := []string{}

	val, ok := getItemValue(item, ks.HashKey, attrs[ks.HashKey])
	if !ok {
		return "", false
	}

	hashKeyStr := fmt.Sprintf("%v", val)

	if ks.RangeKey == "" {
		return hashKeyStr, true
	}

	key = append(key, hashKeyStr)

	val, ok = getItemValue(item, ks.RangeKey, attrs[ks.RangeKey])
	if !ok {
		return "", false
	}

	key = append(key, fmt.Sprintf("%v", val))

	return strings.Join(key, "."), true
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

func (ks *keySchema) getKeyItem(item map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	keyItem := map[string]*dynamodb.AttributeValue{}

	if v, ok := item[ks.HashKey]; ok {
		keyItem[ks.HashKey] = v
	}

	if v, ok := item[ks.RangeKey]; ok && ks.RangeKey != "" {
		keyItem[ks.RangeKey] = v
	}

	return keyItem
}
