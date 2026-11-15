package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// DynamoDB checks the ProjectionExpression of a GetItem before the key.
func TestGetItemProjectionBeforeKey(t *testing.T) {
	const (
		undeclared = "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"
		syntaxErr  = "Invalid ProjectionExpression: "
		emptyKey   = "The AttributeValue for a key attribute cannot contain an empty string value. Key: id"
		keyCount   = "The number of conditions on the keys is invalid"
		keyType    = "Type mismatch for key"
	)

	client := setupClient(tableName)
	require.NoError(t, ensurePokemonTable(client))
	require.NoError(t, createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}))

	key := func(av dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
		return map[string]dynamodbtypes.AttributeValue{"id": av}
	}
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	tests := []struct {
		name       string
		key        map[string]dynamodbtypes.AttributeValue
		projection string
		names      map[string]string
		want       string
	}{
		{"empty key, undeclared name", key(str("")), "#x", nil, undeclared},
		{"empty key, syntax error", key(str("")), "id,,", nil, syntaxErr},
		{"missing key, undeclared name", map[string]dynamodbtypes.AttributeValue{}, "#x", nil, undeclared},
		{"missing key, syntax error", map[string]dynamodbtypes.AttributeValue{}, "id,,", nil, syntaxErr},
		{"wrong key type, undeclared name", key(&dynamodbtypes.AttributeValueMemberN{Value: "1"}), "#x", nil, undeclared},
		{"empty key, valid projection", key(str("")), "id", nil, emptyKey},
		{"missing key, valid projection", map[string]dynamodbtypes.AttributeValue{}, "id", nil, keyCount},
		{"wrong key type, valid projection", key(&dynamodbtypes.AttributeValueMemberN{Value: "1"}), "id", nil, keyType},
		{"valid key, undeclared name", key(str("001")), "#x", nil, undeclared},
		{"valid key, syntax error", key(str("001")), "id,,", nil, syntaxErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
				TableName:                aws.String(tableName),
				Key:                      tt.key,
				ProjectionExpression:     aws.String(tt.projection),
				ExpressionAttributeNames: tt.names,
			})

			require.ErrorContains(t, err, "ValidationException")
			require.ErrorContains(t, err, tt.want)
		})
	}
}

const rangeTableName = "ranged"

func rangeKeyClient(t *testing.T) FakeClient {
	t.Helper()

	client := setupClient(rangeTableName)
	require.NoError(t, AddTable(context.Background(), client, rangeTableName, "pk", "sk"))

	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(rangeTableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"},
			"sk": &dynamodbtypes.AttributeValueMemberS{Value: "1"},
			"l":  &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{&dynamodbtypes.AttributeValueMemberS{Value: "x"}}},
		},
	})
	require.NoError(t, err)

	return client
}

// A key with a missing range key gets the key error only once the projection is valid.
func TestGetItemProjectionRangeKeyTable(t *testing.T) {
	client := rangeKeyClient(t)
	noSK := map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}

	_, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(rangeTableName), Key: noSK, ProjectionExpression: aws.String("#x"),
	})
	require.ErrorContains(t, err, "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x")

	_, err = client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(rangeTableName), Key: noSK, ProjectionExpression: aws.String("l"),
	})
	require.ErrorContains(t, err, "The number of conditions on the keys is invalid")
}

// BatchGetItem is the opposite of GetItem in DynamoDB: a key problem comes before the projection.
func TestBatchGetItemKeyBeforeProjection(t *testing.T) {
	const (
		emptyKey = "The AttributeValue for a key attribute cannot contain an empty string value. Key: sk"
		keyCount = "The number of conditions on the keys is invalid"
	)

	client := rangeKeyClient(t)
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	tests := []struct {
		name       string
		keys       []map[string]dynamodbtypes.AttributeValue
		projection string
		want       string
	}{
		{"empty key, undeclared name", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("")}}, "#x", emptyKey},
		{"empty key, syntax error", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("")}}, "l,,", emptyKey},
		{"missing range key, undeclared name", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a")}}, "#x", keyCount},
		{"valid key, undeclared name", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}}, "#x", "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"},
		{"valid key, syntax error", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}}, "l,,", "Invalid ProjectionExpression: "},
		{"ok then empty key, undeclared name", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("a"), "sk": str("")}}, "#x", emptyKey},
		{"ok then empty key, syntax error", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("a"), "sk": str("")}}, "l,,", emptyKey},
		{"ok then missing range key, undeclared name", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("a")}}, "#x", keyCount},
		{"two valid keys, undeclared name", []map[string]dynamodbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("b"), "sk": str("2")}}, "#x", "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
					rangeTableName: {Keys: tt.keys, ProjectionExpression: aws.String(tt.projection)},
				},
			})

			require.ErrorContains(t, err, "ValidationException")
			require.ErrorContains(t, err, tt.want)
		})
	}
}
