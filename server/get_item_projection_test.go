package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// DynamoDB checks the ProjectionExpression of a GetItem before the key.
func TestServerGetItemProjectionBeforeKey(t *testing.T) {
	const (
		undeclared = "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"
		syntaxErr  = "Invalid ProjectionExpression: "
		emptyKey   = "The AttributeValue for a key attribute cannot contain an empty string value. Key: customer"
		keyCount   = "The number of conditions on the keys is invalid"
		keyType    = "Type mismatch for key"
	)

	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("orders"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("customer"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("customer"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	key := func(av ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
		return map[string]ddbtypes.AttributeValue{"customer": av}
	}
	str := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
	num := ddbtypes.AttributeValue(&ddbtypes.AttributeValueMemberN{Value: "1"})

	_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders"), Item: key(str("ana"))})
	require.NoError(t, err)

	tests := []struct {
		name       string
		key        map[string]ddbtypes.AttributeValue
		projection string
		want       string
	}{
		{"empty key, undeclared name", key(str("")), "#x", undeclared},
		{"empty key, syntax error", key(str("")), "customer,,", syntaxErr},
		{"missing key, undeclared name", map[string]ddbtypes.AttributeValue{}, "#x", undeclared},
		{"missing key, syntax error", map[string]ddbtypes.AttributeValue{}, "customer,,", syntaxErr},
		{"wrong key type, undeclared name", key(num), "#x", undeclared},
		{"empty key, valid projection", key(str("")), "customer", emptyKey},
		{"missing key, valid projection", map[string]ddbtypes.AttributeValue{}, "customer", keyCount},
		{"wrong key type, valid projection", key(num), "customer", keyType},
		{"valid key, undeclared name", key(str("ana")), "#x", undeclared},
		{"valid key, syntax error", key(str("ana")), "customer,,", syntaxErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cli.GetItem(ctx, &dynamodb.GetItemInput{
				TableName:            aws.String("orders"),
				Key:                  tt.key,
				ProjectionExpression: aws.String(tt.projection),
			})

			require.ErrorContains(t, err, "ValidationException")
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func newRangedTable(t *testing.T) (*dynamodb.Client, func()) {
	t.Helper()

	ts := httptest.NewServer(NewServer())
	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("ranged"),
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

	_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("ranged"), Item: map[string]ddbtypes.AttributeValue{
		"pk": &ddbtypes.AttributeValueMemberS{Value: "a"},
		"sk": &ddbtypes.AttributeValueMemberS{Value: "1"},
		"a": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"b": &ddbtypes.AttributeValueMemberS{Value: "deep"}, "c": &ddbtypes.AttributeValueMemberS{Value: "other"},
		}},
		"l": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
			&ddbtypes.AttributeValueMemberS{Value: "first"}, &ddbtypes.AttributeValueMemberS{Value: "second"},
		}},
		"n": &ddbtypes.AttributeValueMemberS{Value: "named"},
	}})
	require.NoError(t, err)

	return cli, ts.Close
}

func TestServerGetItemProjectionRangeKeyTable(t *testing.T) {
	cli, closeFn := newRangedTable(t)
	defer closeFn()

	ctx := context.Background()
	str := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }
	noSK := map[string]ddbtypes.AttributeValue{"pk": str("a")}

	_, err := cli.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("ranged"), Key: noSK, ProjectionExpression: aws.String("#x")})
	require.ErrorContains(t, err, "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x")

	_, err = cli.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("ranged"), Key: noSK, ProjectionExpression: aws.String("l")})
	require.ErrorContains(t, err, "The number of conditions on the keys is invalid")
}

func TestServerGetItemNestedProjection(t *testing.T) {
	cli, closeFn := newRangedTable(t)
	defer closeFn()

	out, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:                aws.String("ranged"),
		Key:                      map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "a"}, "sk": &ddbtypes.AttributeValueMemberS{Value: "1"}},
		ProjectionExpression:     aws.String("a.b, l[0], #n"),
		ExpressionAttributeNames: map[string]string{"#n": "n"},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]ddbtypes.AttributeValue{
		"a": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"b": &ddbtypes.AttributeValueMemberS{Value: "deep"}}},
		"l": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{&ddbtypes.AttributeValueMemberS{Value: "first"}}},
		"n": &ddbtypes.AttributeValueMemberS{Value: "named"},
	}, out.Item)
}

func TestServerGetItemUnknownTableBeforeProjection(t *testing.T) {
	cli, closeFn := newRangedTable(t)
	defer closeFn()

	_, err := cli.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:            aws.String("no-such-table"),
		Key:                  map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "a"}},
		ProjectionExpression: aws.String("#x"),
	})
	require.ErrorContains(t, err, "ResourceNotFoundException")
}

// BatchGetItem is the opposite of GetItem in DynamoDB: a key problem comes before the projection.
func TestServerBatchGetItemKeyBeforeProjection(t *testing.T) {
	const (
		emptyKey = "The AttributeValue for a key attribute cannot contain an empty string value. Key: sk"
		keyCount = "The number of conditions on the keys is invalid"
	)

	cli, closeFn := newRangedTable(t)
	defer closeFn()

	str := func(v string) ddbtypes.AttributeValue { return &ddbtypes.AttributeValueMemberS{Value: v} }

	tests := []struct {
		name       string
		keys       []map[string]ddbtypes.AttributeValue
		projection string
		want       string
	}{
		{"empty key, undeclared name", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("")}}, "#x", emptyKey},
		{"empty key, syntax error", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("")}}, "l,,", emptyKey},
		{"missing range key, undeclared name", []map[string]ddbtypes.AttributeValue{{"pk": str("a")}}, "#x", keyCount},
		{"valid key, undeclared name", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}}, "#x", "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"},
		{"valid key, syntax error", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}}, "l,,", "Invalid ProjectionExpression: "},
		{"ok then empty key, undeclared name", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("a"), "sk": str("")}}, "#x", emptyKey},
		{"ok then empty key, syntax error", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("a"), "sk": str("")}}, "l,,", emptyKey},
		{"ok then missing range key, undeclared name", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("a")}}, "#x", keyCount},
		{"two valid keys, undeclared name", []map[string]ddbtypes.AttributeValue{{"pk": str("a"), "sk": str("1")}, {"pk": str("b"), "sk": str("2")}}, "#x", "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cli.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: map[string]ddbtypes.KeysAndAttributes{
					"ranged": {Keys: tt.keys, ProjectionExpression: aws.String(tt.projection)},
				},
			})

			require.ErrorContains(t, err, "ValidationException")
			require.ErrorContains(t, err, tt.want)
		})
	}
}
