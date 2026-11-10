package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// BatchWriteItem validates every request before it applies any (#216): an invalid request
// rejects the whole batch and the valid requests around it are not written.
func TestBatchWriteItemValidatesBeforeWriting(t *testing.T) {
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	put := func(id string) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{
			Item: map[string]dynamodbtypes.AttributeValue{"id": str(id)},
		}}
	}
	del := func(key map[string]dynamodbtypes.AttributeValue) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key}}
	}
	num := &dynamodbtypes.AttributeValueMemberN{Value: "1"}

	tests := []struct {
		name    string
		reqs    []dynamodbtypes.WriteRequest
		message string
	}{
		{
			"PutEmptyKey",
			[]dynamodbtypes.WriteRequest{put("001"), put("")},
			"The AttributeValue for a key attribute cannot contain an empty string value. Key: id",
		},
		{"PutWrongKeyType", []dynamodbtypes.WriteRequest{put("001"), {PutRequest: &dynamodbtypes.PutRequest{
			Item: map[string]dynamodbtypes.AttributeValue{"id": num},
		}}}, "Type mismatch for key"},
		{"PutMissingKey", []dynamodbtypes.WriteRequest{put("001"), {PutRequest: &dynamodbtypes.PutRequest{
			Item: map[string]dynamodbtypes.AttributeValue{"other": str("x")},
		}}}, "One of the required keys was not given a value"},
		{
			"DeleteEmptyKey",
			[]dynamodbtypes.WriteRequest{put("001"), del(map[string]dynamodbtypes.AttributeValue{"id": str("")})},
			"The AttributeValue for a key attribute cannot contain an empty string value. Key: id",
		},
		{
			"DeleteWrongKeyType",
			[]dynamodbtypes.WriteRequest{put("001"), del(map[string]dynamodbtypes.AttributeValue{"id": num})},
			"Type mismatch for key",
		},
		{
			"DeleteExtraAttribute",
			[]dynamodbtypes.WriteRequest{put("001"), del(map[string]dynamodbtypes.AttributeValue{"id": str("a"), "x": str("b")})},
			"The number of conditions on the keys is invalid",
		},
		{
			"InvalidFirst",
			[]dynamodbtypes.WriteRequest{put(""), put("001")},
			"The AttributeValue for a key attribute cannot contain an empty string value. Key: id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := setupClient(tableName)

			c.NoError(ensurePokemonTable(client))

			_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodbtypes.WriteRequest{tableName: tt.reqs},
			})
			c.Error(err)
			c.Contains(err.Error(), "ValidationException")
			c.Contains(err.Error(), tt.message)

			c.Empty(getPokemon(client, "001"))
		})
	}

	t.Run("UnknownTable", func(t *testing.T) {
		c := require.New(t)
		client := setupClient(tableName)

		c.NoError(ensurePokemonTable(client))

		_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]dynamodbtypes.WriteRequest{
				tableName: {put("001")},
				"missing": {put("001")},
			},
		})
		c.Error(err)
		c.Contains(err.Error(), "Cannot do operations on a non-existent table")

		c.Empty(getPokemon(client, "001"))
	})
}

// A table with a range key and a GSI has more to validate than a hash-only one: a put needs the
// range key and the GSI key values, a delete needs the whole primary key. An invalid request
// writes nothing, not even the valid request before it.
func TestBatchWriteItemValidatesRangeKeyAndGSI(t *testing.T) {
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	put := func(item map[string]dynamodbtypes.AttributeValue) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{Item: item}}
	}
	del := func(key map[string]dynamodbtypes.AttributeValue) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key}}
	}
	valid := put(map[string]dynamodbtypes.AttributeValue{"pk": str("a"), "sk": str("1"), "g": str("x")})

	tests := []struct {
		name    string
		bad     dynamodbtypes.WriteRequest
		message string
	}{
		{
			"PutMissingRangeKey",
			put(map[string]dynamodbtypes.AttributeValue{"pk": str("a")}),
			"One of the required keys was not given a value",
		},
		{
			"PutEmptyGSIKey",
			put(map[string]dynamodbtypes.AttributeValue{"pk": str("a"), "sk": str("2"), "g": str("")}),
			"A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: by-g, IndexKey: g",
		},
		{
			"DeleteMissingRangeKey",
			del(map[string]dynamodbtypes.AttributeValue{"pk": str("a")}),
			"The number of conditions on the keys is invalid",
		},
	}

	setup := func(t *testing.T, name string) FakeClient {
		t.Helper()

		client := setupClient(name)
		require.NoError(t, AddTable(context.Background(), client, name, "pk", "sk"))
		require.NoError(t, AddIndex(context.Background(), client, name, "by-g", "g", ""))

		return client
	}

	scanCount := func(t *testing.T, client FakeClient, name string) int {
		t.Helper()

		out, err := client.Scan(context.Background(), &dynamodb.ScanInput{TableName: &name})
		require.NoError(t, err)

		return len(out.Items)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := setup(t, "ranked")

			_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodbtypes.WriteRequest{"ranked": {valid, tt.bad}},
			})
			c.Error(err)
			c.Contains(err.Error(), "ValidationException")
			c.Contains(err.Error(), tt.message)

			c.Zero(scanCount(t, client, "ranked"))
		})
	}
}

// An unknown table is reported only once every request of every existing table is valid, even
// when its name sorts before the table that has the invalid request (D3).
func TestBatchWriteItemInvalidItemBeforeUnknownTable(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	c.NoError(ensurePokemonTable(client))

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	put := func(id string) dynamodbtypes.WriteRequest {
		return dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{
			Item: map[string]dynamodbtypes.AttributeValue{"id": str(id)},
		}}
	}

	_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			"aaa":     {put("001")},
			tableName: {put("")},
		},
	})
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
	c.Contains(err.Error(), "cannot contain an empty string value")
	c.NotContains(err.Error(), "non-existent")
}

// With two existing tables that each have an invalid request, the first table by name is
// reported. DynamoDB Local reports the first in the order of the request, which a Go map cannot
// carry, so minidyn picks a fixed order (D1 level 3).
func TestBatchWriteItemTwoInvalidTablesInNameOrder(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)

	c.NoError(ensurePokemonTable(client))
	c.NoError(AddTable(context.Background(), client, "aaa-ranked", "pk", "sk"))

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodbtypes.WriteRequest{
			tableName: {{PutRequest: &dynamodbtypes.PutRequest{
				Item: map[string]dynamodbtypes.AttributeValue{"id": str("")},
			}}},
			"aaa-ranked": {{PutRequest: &dynamodbtypes.PutRequest{
				Item: map[string]dynamodbtypes.AttributeValue{"pk": str("a")},
			}}},
		},
	})
	c.Error(err)
	c.Contains(err.Error(), "One of the required keys was not given a value")
}
