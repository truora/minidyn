package server

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

type batchKey = map[string]ddbtypes.AttributeValue

func batchStr(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }

func batchNum(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberN{Value: v} }

func batchPK(pk, sk string) batchKey { return batchKey{"pk": batchStr(pk), "sk": batchStr(sk)} }

func createOtherRanged(t *testing.T, client *dynamodb.Client) {
	t.Helper()

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("other-ranged"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

const batchDuplicateKeys = "Provided list of item keys contains duplicates"

// BatchWriteItem rejects a key named twice in one table, a put and a delete included, before it
// writes anything (#235).
func TestServerBatchWriteItemRejectsDuplicateKeys(t *testing.T) {
	put := func(k batchKey) ddbtypes.WriteRequest {
		return ddbtypes.WriteRequest{PutRequest: &ddbtypes.PutRequest{Item: k}}
	}
	del := func(k batchKey) ddbtypes.WriteRequest {
		return ddbtypes.WriteRequest{DeleteRequest: &ddbtypes.DeleteRequest{Key: k}}
	}
	withX := func(k batchKey, v string) batchKey { k["x"] = batchStr(v); return k }

	tests := []struct {
		name string
		reqs []ddbtypes.WriteRequest
		want string
	}{
		{"two puts", []ddbtypes.WriteRequest{put(batchPK("b", "1")), put(batchPK("b", "1"))}, batchDuplicateKeys},
		{"put and delete", []ddbtypes.WriteRequest{put(batchPK("b", "1")), del(batchPK("b", "1"))}, batchDuplicateKeys},
		{"delete and put", []ddbtypes.WriteRequest{del(batchPK("a", "1")), put(batchPK("a", "1"))}, batchDuplicateKeys},
		{"two deletes", []ddbtypes.WriteRequest{del(batchPK("a", "1")), del(batchPK("a", "1"))}, batchDuplicateKeys},
		{"other attributes differ", []ddbtypes.WriteRequest{
			put(withX(batchPK("b", "1"), "1")), put(withX(batchPK("b", "1"), "2")),
		}, batchDuplicateKeys},
		{"an invalid request comes first", []ddbtypes.WriteRequest{
			put(batchPK("", "1")), put(batchPK("b", "1")), put(batchPK("b", "1")),
		}, "The AttributeValue for a key attribute cannot contain an empty string value. Key: pk"},
		{"a duplicate comes first", []ddbtypes.WriteRequest{
			put(batchPK("b", "1")), put(batchPK("b", "1")), put(batchPK("", "1")),
		}, batchDuplicateKeys},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, closeFn := newRangedTable(t)
			defer closeFn()

			_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]ddbtypes.WriteRequest{"ranged": tt.reqs},
			})
			require.ErrorContains(t, err, "ValidationException")
			require.ErrorContains(t, err, tt.want)

			// nothing was written or deleted
			out, err := client.Scan(context.Background(), &dynamodb.ScanInput{TableName: aws.String("ranged")})
			require.NoError(t, err)
			require.Len(t, out.Items, 1)
			require.Equal(t, batchStr("a"), out.Items[0]["pk"])
		})
	}

	t.Run("the same key in two tables", func(t *testing.T) {
		client, closeFn := newRangedTable(t)
		defer closeFn()

		createOtherRanged(t, client)

		_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]ddbtypes.WriteRequest{
				"ranged":       {put(batchPK("b", "1"))},
				"other-ranged": {put(batchPK("b", "1"))},
			},
		})
		require.NoError(t, err)
	})
}

// DynamoDB rejects a batch write with no tables, or a table with no requests.
func TestServerBatchWriteItemRejectsEmptyRequests(t *testing.T) {
	client, closeFn := newRangedTable(t)
	defer closeFn()

	_, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbtypes.WriteRequest{},
	})
	require.ErrorContains(t, err, "ValidationException: BatchWriteItem cannot have a null or no requests set")

	_, err = client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]ddbtypes.WriteRequest{"ranged": {}},
	})
	require.ErrorContains(t, err, "ValidationException: The batch write request list for a table cannot be null or empty: "+"ranged")
}

// BatchGetItem checks its keys before it looks at their values, the tables or the projection
// (#246): a key named twice is rejected even when it is invalid, equal numbers spelled
// differently are different keys, and a table with no keys has DynamoDB's own error.
func TestServerBatchGetItemRejectsDuplicateKeys(t *testing.T) {
	many := func(count int) []batchKey {
		keys := make([]batchKey, count)
		for i := range keys {
			keys[i] = batchPK("a", strconv.Itoa(i))
		}

		return keys
	}

	tests := []struct {
		name       string
		keys       []batchKey
		projection string
		want       string
	}{
		{"duplicate", []batchKey{batchPK("a", "1"), batchPK("a", "1")}, "", batchDuplicateKeys},
		{"duplicate with an undeclared name", []batchKey{batchPK("a", "1"), batchPK("a", "1")}, "#x", batchDuplicateKeys},
		{"duplicate empty key", []batchKey{batchPK("a", ""), batchPK("a", "")}, "", batchDuplicateKeys},
		{"duplicate missing range key", []batchKey{{"pk": batchStr("a")}, {"pk": batchStr("a")}}, "", batchDuplicateKeys},
		{"duplicate of the wrong type", []batchKey{{"pk": batchNum("1"), "sk": batchStr("1")}, {"pk": batchNum("1"), "sk": batchStr("1")}}, "", batchDuplicateKeys},
		{"duplicate after the limit", append(many(100), batchPK("a", "1")), "", batchDuplicateKeys},
		// numbers are compared as spelled, so these reach the key check
		{"equal numbers", []batchKey{{"pk": batchNum("1"), "sk": batchStr("1")}, {"pk": batchNum("1.0"), "sk": batchStr("1")}}, "", "Type mismatch for key"},
		{"extra attributes differ", []batchKey{
			{"pk": batchStr("a"), "sk": batchStr("1"), "x": batchStr("1")}, {"pk": batchStr("a"), "sk": batchStr("1"), "x": batchStr("2")},
		}, "", "The number of conditions on the keys is invalid"},
		{"no keys", []batchKey{}, "#x", "The list of keys in RequestItems for BatchGetItem is required: " + "ranged" + " has empty list"},
		{"too many keys", many(101), "#x", "Too many items requested for the BatchGetItem call"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, closeFn := newRangedTable(t)
			defer closeFn()

			in := ddbtypes.KeysAndAttributes{Keys: tt.keys}
			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			_, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: map[string]ddbtypes.KeysAndAttributes{"ranged": in},
			})
			require.ErrorContains(t, err, "ValidationException")
			require.ErrorContains(t, err, tt.want)
		})
	}

	t.Run("no tables", func(t *testing.T) {
		client, closeFn := newRangedTable(t)
		defer closeFn()

		_, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
			RequestItems: map[string]ddbtypes.KeysAndAttributes{},
		})
		require.ErrorContains(t, err, "ValidationException: BatchGetItem must have some requests set")
	})

	t.Run("the same key in two tables", func(t *testing.T) {
		client, closeFn := newRangedTable(t)
		defer closeFn()

		createOtherRanged(t, client)

		out, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
			RequestItems: map[string]ddbtypes.KeysAndAttributes{
				"ranged":       {Keys: []batchKey{batchPK("a", "1")}},
				"other-ranged": {Keys: []batchKey{batchPK("a", "1")}},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.Responses["ranged"], 1)
		require.Empty(t, out.Responses["other-ranged"])
	})
}

// BatchGetItem runs its checks in the order BatchWriteItem does (D13): ReturnConsumedCapacity,
// then the emulated failure, then the keys. The 100 key limit goes with the duplicate check, as
// Local reports a duplicate ahead of the limit.
func TestServerBatchGetItemChecksInTheOrderOfBatchWriteItem(t *testing.T) {
	ctx := context.Background()
	errEmulated := errors.New("emulated")
	dup := map[string]KeysAndAttributes{"pokemons": {Keys: []map[string]*AttributeValue{idKey("1"), idKey("1")}}}

	t.Run("an emulated failure comes before a duplicate", func(t *testing.T) {
		client := newServerWithPokemonTable(t)
		client.setFailureCondition(errEmulated)

		_, err := client.BatchGetItem(ctx, &BatchGetItemInput{RequestItems: dup})
		require.ErrorIs(t, err, errEmulated)
	})

	t.Run("an invalid ReturnConsumedCapacity comes before a duplicate and an emulated failure", func(t *testing.T) {
		client := newServerWithPokemonTable(t)
		client.setFailureCondition(errEmulated)

		_, err := client.BatchGetItem(ctx, &BatchGetItemInput{RequestItems: dup, ReturnConsumedCapacity: "FOO"})
		require.ErrorContains(t, err, "returnConsumedCapacity")
		require.NotErrorIs(t, err, errEmulated)
	})

	t.Run("a duplicate follows both", func(t *testing.T) {
		client := newServerWithPokemonTable(t)

		_, err := client.BatchGetItem(ctx, &BatchGetItemInput{RequestItems: dup})
		require.ErrorContains(t, err, batchDuplicateKeys)
	})
}

// The tables of a BatchGetItem are read by name, so which table's error is reported does not
// depend on the order of a Go map (D1 level 3).
func TestServerBatchGetItemReadsTablesByName(t *testing.T) {
	client := newServerWithPokemonTable(t)
	keys := []map[string]*AttributeValue{idKey("1")}

	// pokemons has an undeclared name, zzz does not exist: by name, pokemons is first
	in := map[string]KeysAndAttributes{
		"pokemons": {Keys: keys, ProjectionExpression: aws.String("#x")},
		"zzz":      {Keys: keys},
	}

	for range 50 {
		_, err := client.BatchGetItem(context.Background(), &BatchGetItemInput{RequestItems: in})
		require.ErrorContains(t, err, "An expression attribute name used in the document path is not defined")
	}
}

// The error of a duplicate names the table and the position of the repeated key, not its value.
func TestServerBatchDuplicateKeysDetail(t *testing.T) {
	client := newServerWithPokemonTable(t)

	_, err := client.BatchGetItem(context.Background(), &BatchGetItemInput{RequestItems: map[string]KeysAndAttributes{
		"pokemons": {Keys: []map[string]*AttributeValue{idKey("secret"), idKey("b"), idKey("secret")}},
	}})
	require.ErrorContains(t, err, batchDuplicateKeys+" | table: pokemons; key: 2")
	require.NotContains(t, err.Error(), "secret")

	_, err = client.BatchWriteItem(context.Background(), &BatchWriteItemInput{RequestItems: map[string][]WriteRequest{
		"pokemons": {
			{PutRequest: &PutRequest{Item: idKey("secret")}},
			{PutRequest: &PutRequest{Item: idKey("b")}},
			{DeleteRequest: &DeleteRequest{Key: idKey("secret")}},
		},
	}})
	require.ErrorContains(t, err, batchDuplicateKeys+" | table: pokemons; request: 2")
	require.NotContains(t, err.Error(), "secret")
}
