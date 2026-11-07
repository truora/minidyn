package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const paritySelectTableName = "sel"

func paritySelectTable(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	s := func(n string) dynamodbtypes.AttributeDefinition {
		return dynamodbtypes.AttributeDefinition{AttributeName: aws.String(n), AttributeType: dynamodbtypes.ScalarAttributeTypeS}
	}
	ks := func(n string, k dynamodbtypes.KeyType) dynamodbtypes.KeySchemaElement {
		return dynamodbtypes.KeySchemaElement{AttributeName: aws.String(n), KeyType: k}
	}
	ko := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeKeysOnly}
	inc := &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeInclude, NonKeyAttributes: []string{"a"}}
	h, r := dynamodbtypes.KeyTypeHash, dynamodbtypes.KeyTypeRange

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(paritySelectTableName),
		BillingMode:          dynamodbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{s("pk"), s("sk"), s("g"), s("l")},
		KeySchema:            []dynamodbtypes.KeySchemaElement{ks("pk", h), ks("sk", r)},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
			{IndexName: aws.String("gko"), KeySchema: []dynamodbtypes.KeySchemaElement{ks("g", h)}, Projection: ko},
			{IndexName: aws.String("ginc"), KeySchema: []dynamodbtypes.KeySchemaElement{ks("g", h)}, Projection: inc},
		},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{
			{IndexName: aws.String("lko"), KeySchema: []dynamodbtypes.KeySchemaElement{ks("pk", h), ks("l", r)}, Projection: ko},
			{IndexName: aws.String("linc"), KeySchema: []dynamodbtypes.KeySchemaElement{ks("pk", h), ks("l", r)}, Projection: inc},
		},
	})
	require.NoError(t, err)

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(paritySelectTableName), Item: map[string]dynamodbtypes.AttributeValue{
		"pk": str("p"), "sk": str("s"), "g": str("g"), "l": str("l"), "a": str("a"), "b": str("b"),
	}})
	require.NoError(t, err)
}

func paritySortedAttrNames(items []map[string]dynamodbtypes.AttributeValue) string {
	s := ""

	for _, it := range items {
		ks := []string{}
		for k := range it {
			ks = append(ks, k)
		}

		sort.Strings(ks)
		s += fmt.Sprint(ks)
	}

	return s
}

// paritySelectRun runs a Scan or Query against the select table and returns the error text, or
// the sorted attribute names of the returned items.
func paritySelectRun(ctx context.Context, client *dynamodb.Client, scan bool, index string, sel dynamodbtypes.Select, withProjection bool) any {
	var (
		items     []map[string]dynamodbtypes.AttributeValue
		err       error
		names     map[string]string
		expr      *string
		indexName *string
	)

	if withProjection {
		expr, names = aws.String("#b"), map[string]string{"#b": "b"}
	}

	if index != "" {
		indexName = aws.String(index)
	}

	if scan {
		var out *dynamodb.ScanOutput

		out, err = client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(paritySelectTableName), IndexName: indexName, Select: sel,
			ProjectionExpression: expr, ExpressionAttributeNames: names,
		})
		if err == nil {
			items = out.Items
		}
	} else {
		key, val := "pk", "p"
		if strings.HasPrefix(index, "g") {
			key, val = "g", "g"
		}

		var out *dynamodb.QueryOutput

		out, err = client.Query(ctx, &dynamodb.QueryInput{
			TableName: aws.String(paritySelectTableName), IndexName: indexName, Select: sel,
			KeyConditionExpression: aws.String(key + " = :v"), ProjectionExpression: expr,
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":v": &dynamodbtypes.AttributeValueMemberS{Value: val}},
		})
		if err == nil {
			items = out.Items
		}
	}

	if err != nil {
		return normalizeSDKErrorString(err.Error())
	}

	return paritySortedAttrNames(items)
}
