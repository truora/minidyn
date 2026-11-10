package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// BatchWriteItem validates every request before it applies any (#216): an invalid request
// rejects the whole batch and the valid requests around it are not written.
func TestServerBatchWriteItemValidatesBeforeWriting(t *testing.T) {
	ctx := context.Background()
	str := func(v string) *AttributeValue { return &AttributeValue{S: aws.String(v)} }
	num := &AttributeValue{N: aws.String("1")}
	put := func(item map[string]*AttributeValue) WriteRequest {
		return WriteRequest{PutRequest: &PutRequest{Item: item}}
	}
	del := func(key map[string]*AttributeValue) WriteRequest {
		return WriteRequest{DeleteRequest: &DeleteRequest{Key: key}}
	}
	id := func(v string) map[string]*AttributeValue { return map[string]*AttributeValue{"id": str(v)} }

	tests := []struct {
		name    string
		reqs    []WriteRequest
		message string
	}{
		{
			"PutEmptyKey",
			[]WriteRequest{put(id("001")), put(id(""))},
			"The AttributeValue for a key attribute cannot contain an empty string value. Key: id",
		},
		{
			"PutWrongKeyType",
			[]WriteRequest{put(id("001")), put(map[string]*AttributeValue{"id": num})},
			"Type mismatch for key",
		},
		{
			"PutMissingKey",
			[]WriteRequest{put(id("001")), put(map[string]*AttributeValue{"other": str("x")})},
			"One of the required keys was not given a value",
		},
		{
			"DeleteEmptyKey",
			[]WriteRequest{put(id("001")), del(id(""))},
			"The AttributeValue for a key attribute cannot contain an empty string value. Key: id",
		},
		{
			"DeleteWrongKeyType",
			[]WriteRequest{put(id("001")), del(map[string]*AttributeValue{"id": num})},
			"Type mismatch for key",
		},
		{
			"DeleteExtraAttribute",
			[]WriteRequest{put(id("001")), del(map[string]*AttributeValue{"id": str("a"), "x": str("b")})},
			"The number of conditions on the keys is invalid",
		},
		{
			"InvalidFirst",
			[]WriteRequest{put(id("")), put(id("001"))},
			"The AttributeValue for a key attribute cannot contain an empty string value. Key: id",
		},
	}

	setup := func(t *testing.T) *Client {
		t.Helper()

		srv := NewServer()

		_, err := srv.client.CreateTable(ctx, &CreateTableInput{
			TableName: aws.String("pokemons"),
			KeySchema: []ddbtypes.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
			},
			AttributeDefinitions: []ddbtypes.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			},
			BillingMode: ddbtypes.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		return srv.client
	}

	stored := func(t *testing.T, client *Client) bool {
		t.Helper()

		out, err := client.GetItem(ctx, &GetItemInput{TableName: aws.String("pokemons"), Key: id("001")})
		require.NoError(t, err)

		return len(out.Item) > 0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := setup(t)

			_, err := client.BatchWriteItem(ctx, &BatchWriteItemInput{
				RequestItems: map[string][]WriteRequest{"pokemons": tt.reqs},
			})
			c.Error(err)
			c.Contains(err.Error(), "ValidationException")
			c.Contains(err.Error(), tt.message)

			c.False(stored(t, client))
		})
	}

	t.Run("UnknownTable", func(t *testing.T) {
		c := require.New(t)
		client := setup(t)

		_, err := client.BatchWriteItem(ctx, &BatchWriteItemInput{
			RequestItems: map[string][]WriteRequest{
				"pokemons": {put(id("001"))},
				"missing":  {put(id("001"))},
			},
		})
		c.Error(err)
		c.Contains(err.Error(), "Cannot do operations on a non-existent table")

		c.False(stored(t, client))
	})
}

// A table with a range key and a GSI has more to validate than a hash-only one: a put needs the
// range key and the GSI key values, a delete needs the whole primary key. An invalid request
// writes nothing, not even the valid request before it.
func TestServerBatchWriteItemValidatesRangeKeyAndGSI(t *testing.T) {
	ctx := context.Background()
	str := func(v string) *AttributeValue { return &AttributeValue{S: aws.String(v)} }
	put := func(item map[string]*AttributeValue) WriteRequest {
		return WriteRequest{PutRequest: &PutRequest{Item: item}}
	}
	del := func(key map[string]*AttributeValue) WriteRequest {
		return WriteRequest{DeleteRequest: &DeleteRequest{Key: key}}
	}
	valid := put(map[string]*AttributeValue{"pk": str("a"), "sk": str("1"), "g": str("x")})

	tests := []struct {
		name    string
		bad     WriteRequest
		message string
	}{
		{
			"PutMissingRangeKey",
			put(map[string]*AttributeValue{"pk": str("a")}),
			"One of the required keys was not given a value",
		},
		{
			"PutEmptyGSIKey",
			put(map[string]*AttributeValue{"pk": str("a"), "sk": str("2"), "g": str("")}),
			"A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: by-g, IndexKey: g",
		},
		{
			"DeleteMissingRangeKey",
			del(map[string]*AttributeValue{"pk": str("a")}),
			"The number of conditions on the keys is invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)
			client := newRangedClient(t, "ranked")

			_, err := client.BatchWriteItem(ctx, &BatchWriteItemInput{
				RequestItems: map[string][]WriteRequest{"ranked": {valid, tt.bad}},
			})
			c.Error(err)
			c.Contains(err.Error(), "ValidationException")
			c.Contains(err.Error(), tt.message)

			out, err := client.Scan(ctx, &ScanInput{TableName: aws.String("ranked")})
			c.NoError(err)
			c.Empty(out.Items)
		})
	}
}

// newRangedClient returns a client with a table that has a hash key, a range key and a GSI on g.
func newRangedClient(t *testing.T, name string) *Client {
	t.Helper()

	client := NewServer().client
	attr := func(n string) ddbtypes.AttributeDefinition {
		return ddbtypes.AttributeDefinition{AttributeName: aws.String(n), AttributeType: ddbtypes.ScalarAttributeTypeS}
	}

	_, err := client.CreateTable(context.Background(), &CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{attr("pk"), attr("sk"), attr("g")},
		GlobalSecondaryIndexes: []ddbtypes.GlobalSecondaryIndex{{
			IndexName:  aws.String("by-g"),
			KeySchema:  []ddbtypes.KeySchemaElement{{AttributeName: aws.String("g"), KeyType: ddbtypes.KeyTypeHash}},
			Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
		}},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return client
}

// An unknown table is reported only once every request of every existing table is valid, even
// when its name sorts before the table that has the invalid request (D3).
func TestServerBatchWriteItemInvalidItemBeforeUnknownTable(t *testing.T) {
	c := require.New(t)
	client := newRangedClient(t, "ranked")

	str := func(v string) *AttributeValue { return &AttributeValue{S: aws.String(v)} }

	_, err := client.BatchWriteItem(context.Background(), &BatchWriteItemInput{
		RequestItems: map[string][]WriteRequest{
			"aaa":    {{PutRequest: &PutRequest{Item: map[string]*AttributeValue{"pk": str("a"), "sk": str("1")}}}},
			"ranked": {{PutRequest: &PutRequest{Item: map[string]*AttributeValue{"pk": str("a")}}}},
		},
	})
	c.Error(err)
	c.Contains(err.Error(), "ValidationException")
	c.Contains(err.Error(), "One of the required keys was not given a value")
	c.NotContains(err.Error(), "non-existent")
}

// With two existing tables that each have an invalid request, the first table by name is
// reported. DynamoDB Local reports the first in the order of the request, which a Go map cannot
// carry, so minidyn picks a fixed order (D1 level 3).
func TestServerBatchWriteItemTwoInvalidTablesInNameOrder(t *testing.T) {
	c := require.New(t)
	client := newRangedClient(t, "bbb")

	_, err := client.CreateTable(context.Background(), &CreateTableInput{
		TableName:            aws.String("aaa"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	str := func(v string) *AttributeValue { return &AttributeValue{S: aws.String(v)} }

	_, err = client.BatchWriteItem(context.Background(), &BatchWriteItemInput{
		RequestItems: map[string][]WriteRequest{
			"bbb": {{PutRequest: &PutRequest{Item: map[string]*AttributeValue{"pk": str("a")}}}},
			"aaa": {{PutRequest: &PutRequest{Item: map[string]*AttributeValue{"id": str("")}}}},
		},
	})
	c.Error(err)
	c.Contains(err.Error(), "cannot contain an empty string value")
}
