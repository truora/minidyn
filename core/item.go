package core

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func copyItem(item map[string]*dynamodb.AttributeValue) map[string]*dynamodb.AttributeValue {
	copy := map[string]*dynamodb.AttributeValue{}
	for key, val := range item {
		copy[key] = val
	}

	return copy
}
