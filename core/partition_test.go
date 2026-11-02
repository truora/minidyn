package core

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestPartitionPrefix(t *testing.T) {
	table := NewTable("partition")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "S", "n": "N"}

	composite := keySchema{HashKey: "pk", RangeKey: "sk"}
	hashOnly := keySchema{HashKey: "pk"}
	numeric := keySchema{HashKey: "n"}

	values := map[string]*types.Item{
		":p": {S: aws.String("alice")},
		":n": {N: aws.String("7")},
	}

	tests := []struct {
		name   string
		input  QueryInput
		schema keySchema
		want   string
		ok     bool
	}{
		{"composite key", QueryInput{KeyConditionExpression: "pk = :p AND sk > :p", ExpressionAttributeValues: values}, composite, "alice.", true},
		{"hash-only key", QueryInput{KeyConditionExpression: "pk = :p", ExpressionAttributeValues: values}, hashOnly, "alice", true},
		{"numeric hash key", QueryInput{KeyConditionExpression: "n = :n", ExpressionAttributeValues: values}, numeric, "7", true},
		{"scan", QueryInput{Scan: true, KeyConditionExpression: "pk = :p", ExpressionAttributeValues: values}, composite, "", false},
		{"no key condition", QueryInput{ExpressionAttributeValues: values}, composite, "", false},
		{"no hash equality", QueryInput{KeyConditionExpression: "sk = :p", ExpressionAttributeValues: values}, composite, "", false},
		{"missing value", QueryInput{KeyConditionExpression: "pk = :missing", ExpressionAttributeValues: values}, composite, "", false},
		{"nil value", QueryInput{KeyConditionExpression: "pk = :p", ExpressionAttributeValues: map[string]*types.Item{":p": nil}}, composite, "", false},
		{"type mismatch", QueryInput{KeyConditionExpression: "pk = :n", ExpressionAttributeValues: values}, composite, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := table.partitionPrefix(tt.input, tt.schema)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestKeysWithPrefix(t *testing.T) {
	keys := []string{"a", "a.1", "a.2", "a.b.1", "ab.1", "b.1"}

	require.Equal(t, []string{"a.1", "a.2", "a.b.1"}, keysWithPrefix(keys, "a."))
	require.Equal(t, []string{"b.1"}, keysWithPrefix(keys, "b."))
	require.Empty(t, keysWithPrefix(keys, "c."))
	require.Empty(t, keysWithPrefix(nil, "a."))
}
