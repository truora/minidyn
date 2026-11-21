package server

import (
	"context"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// createProjectionTable has the LSIs lko (KEYS_ONLY) and linc (INCLUDE a), and a GSI that
// projects keys only. Its only item has the attributes a and b too, and b is never projected.
func createProjectionTable(t *testing.T) *dynamodb.Client {
	t.Helper()

	ts := httptest.NewServer(NewServer())
	t.Cleanup(ts.Close)

	client := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	s := func(n string) dynamodbtypes.AttributeDefinition {
		return dynamodbtypes.AttributeDefinition{AttributeName: aws.String(n), AttributeType: dynamodbtypes.ScalarAttributeTypeS}
	}
	ks := func(n string, k dynamodbtypes.KeyType) dynamodbtypes.KeySchemaElement {
		return dynamodbtypes.KeySchemaElement{AttributeName: aws.String(n), KeyType: k}
	}
	keysOnly := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeKeysOnly}
	include := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeInclude, NonKeyAttributes: []string{"a"}}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("proj"),
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{s("pk"), s("sk"), s("g"), s("l")},
		KeySchema:            []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("sk", dynamodbtypes.KeyTypeRange)},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
			{IndexName: aws.String("gko"), KeySchema: []dynamodbtypes.KeySchemaElement{ks("g", dynamodbtypes.KeyTypeHash)}, Projection: keysOnly},
		},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{
			{
				IndexName: aws.String("lko"), Projection: keysOnly,
				KeySchema: []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("l", dynamodbtypes.KeyTypeRange)},
			},
			{
				IndexName: aws.String("linc"), Projection: include,
				KeySchema: []dynamodbtypes.KeySchemaElement{ks("pk", dynamodbtypes.KeyTypeHash), ks("l", dynamodbtypes.KeyTypeRange)},
			},
		},
	})
	require.NoError(t, err)

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("proj"), Item: map[string]dynamodbtypes.AttributeValue{
		"pk": str("p"), "sk": str("s"), "g": str("g"), "l": str("l"), "a": str("a"), "b": str("b"),
	}})
	require.NoError(t, err)

	return client
}

func sortedProjectionNames(items []map[string]dynamodbtypes.AttributeValue) []string {
	names := []string{}

	for _, item := range items {
		for k := range item {
			names = append(names, k)
		}
	}

	sort.Strings(names)

	return names
}

// An LSI reads the attributes it does not project from the table (#202).
func TestLSIQueryAndScanFetchFromTable(t *testing.T) {
	client := createProjectionTable(t)
	ctx := context.Background()
	val := map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "p"}}

	tests := []struct {
		name       string
		index      string
		sel        dynamodbtypes.Select
		projection string
		want       []string
	}{
		{name: "KeysOnlyAllAttributes", index: "lko", sel: dynamodbtypes.SelectAllAttributes, want: []string{"a", "b", "g", "l", "pk", "sk"}},
		{name: "KeysOnlySpecific", index: "lko", sel: dynamodbtypes.SelectSpecificAttributes, projection: "b", want: []string{"b"}},
		{name: "KeysOnlyProjectionOnly", index: "lko", projection: "b, g", want: []string{"b", "g"}},
		{name: "KeysOnlyDefault", index: "lko", want: []string{"l", "pk", "sk"}},
		{name: "KeysOnlyAllProjected", index: "lko", sel: dynamodbtypes.SelectAllProjectedAttributes, want: []string{"l", "pk", "sk"}},
		{name: "IncludeAllAttributes", index: "linc", sel: dynamodbtypes.SelectAllAttributes, want: []string{"a", "b", "g", "l", "pk", "sk"}},
		{name: "IncludeSpecific", index: "linc", sel: dynamodbtypes.SelectSpecificAttributes, projection: "b", want: []string{"b"}},
		{name: "IncludeProjectionOnly", index: "linc", projection: "a, b", want: []string{"a", "b"}},
		{name: "IncludeDefault", index: "linc", want: []string{"a", "l", "pk", "sk"}},
		{name: "IncludeAllProjected", index: "linc", sel: dynamodbtypes.SelectAllProjectedAttributes, want: []string{"a", "l", "pk", "sk"}},
	}

	for _, tt := range tests {
		t.Run("Query"+tt.name, func(t *testing.T) {
			in := &dynamodb.QueryInput{
				TableName: aws.String("proj"), IndexName: aws.String(tt.index), Select: tt.sel,
				KeyConditionExpression: aws.String("pk = :v"), ExpressionAttributeValues: val,
			}
			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			out, err := client.Query(ctx, in)
			require.NoError(t, err)
			require.Equal(t, tt.want, sortedProjectionNames(out.Items))
		})

		t.Run("Scan"+tt.name, func(t *testing.T) {
			in := &dynamodb.ScanInput{TableName: aws.String("proj"), IndexName: aws.String(tt.index), Select: tt.sel}
			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			out, err := client.Scan(ctx, in)
			require.NoError(t, err)
			require.Equal(t, tt.want, sortedProjectionNames(out.Items))
		})
	}
}

// A GSI cannot fetch from the table: asking it for an attribute it does not project is rejected.
func TestGSIQueryAndScanRejectNonProjectedAttributes(t *testing.T) {
	client := createProjectionTable(t)
	ctx := context.Background()

	const want = "ValidationException: One or more parameter values were invalid: Global secondary index gko does not project [b]"

	for _, sel := range []dynamodbtypes.Select{"", dynamodbtypes.SelectSpecificAttributes} {
		_, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName: aws.String("proj"), IndexName: aws.String("gko"), Select: sel,
			KeyConditionExpression: aws.String("g = :v"), ProjectionExpression: aws.String("b"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: "g"}},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), want)

		_, err = client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String("proj"), IndexName: aws.String("gko"), Select: sel, ProjectionExpression: aws.String("b"),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), want)
	}

	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("proj"), IndexName: aws.String("gko"), ProjectionExpression: aws.String("g"),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"g"}, sortedProjectionNames(out.Items))
}

// A Query's FilterExpression can only name attributes the index projects. A Scan's filter reads
// the stored item.
func TestIndexQueryFilterMustBeProjected(t *testing.T) {
	client := createProjectionTable(t)
	ctx := context.Background()
	b := &dynamodbtypes.AttributeValueMemberS{Value: "b"}

	for _, tt := range []struct{ index, key, val string }{{"lko", "pk", "p"}, {"gko", "g", "g"}} {
		t.Run("Query"+tt.index, func(t *testing.T) {
			_, err := client.Query(ctx, &dynamodb.QueryInput{
				TableName: aws.String("proj"), IndexName: aws.String(tt.index),
				KeyConditionExpression: aws.String(tt.key + " = :v"), FilterExpression: aws.String("b = :b"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":v": &dynamodbtypes.AttributeValueMemberS{Value: tt.val}, ":b": b,
				},
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "ValidationException: One or more parameter values were invalid: Secondary index "+
				tt.index+" does not project one or more filter attributes: [b]")
		})

		t.Run("Scan"+tt.index, func(t *testing.T) {
			out, err := client.Scan(ctx, &dynamodb.ScanInput{
				TableName: aws.String("proj"), IndexName: aws.String(tt.index), FilterExpression: aws.String("b = :b"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":b": b},
			})
			require.NoError(t, err)
			require.Len(t, out.Items, 1)
		})
	}
}
