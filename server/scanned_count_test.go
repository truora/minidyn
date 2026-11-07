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

func TestServerCountsAndSelectCount(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("customer"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("customer"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	seed := [][3]string{
		{"alice", "order#1", "shipped"},
		{"alice", "order#2", "pending"},
		{"alice", "order#3", "shipped"},
		{"alice", "order#4", "pending"},
		{"bob", "order#1", "shipped"},
	}

	for _, s := range seed {
		_, err = cli.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("orders"),
			Item: map[string]ddbtypes.AttributeValue{
				"customer": &ddbtypes.AttributeValueMemberS{Value: s[0]},
				"sk":       &ddbtypes.AttributeValueMemberS{Value: s[1]},
				"status":   &ddbtypes.AttributeValueMemberS{Value: s[2]},
			},
		})
		require.NoError(t, err)
	}

	query := func(mutate func(*dynamodb.QueryInput)) *dynamodb.QueryOutput {
		in := &dynamodb.QueryInput{
			TableName:              aws.String("orders"),
			KeyConditionExpression: aws.String("customer = :c"),
			FilterExpression:       aws.String("#s = :s"),
			ExpressionAttributeNames: map[string]string{
				"#s": "status",
			},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":c": &ddbtypes.AttributeValueMemberS{Value: "alice"},
				":s": &ddbtypes.AttributeValueMemberS{Value: "shipped"},
			},
		}
		mutate(in)

		out, qerr := cli.Query(ctx, in)
		require.NoError(t, qerr)

		return out
	}

	out := query(func(*dynamodb.QueryInput) {})
	require.Equal(t, []int32{2, 4}, []int32{out.Count, out.ScannedCount})
	require.Len(t, out.Items, 2)

	out = query(func(in *dynamodb.QueryInput) { in.Limit = aws.Int32(2) })
	require.Equal(t, []int32{1, 2}, []int32{out.Count, out.ScannedCount})
	require.Len(t, out.Items, 1)

	out = query(func(in *dynamodb.QueryInput) { in.Select = ddbtypes.SelectCount })
	require.Equal(t, []int32{2, 4}, []int32{out.Count, out.ScannedCount})
	require.Empty(t, out.Items)

	// Nothing matches: Count 0 and ScannedCount 0 are sent, and so is an empty Items list.
	out = query(func(in *dynamodb.QueryInput) {
		in.ExpressionAttributeValues[":c"] = &ddbtypes.AttributeValueMemberS{Value: "nobody"}
	})
	require.Equal(t, []int32{0, 0}, []int32{out.Count, out.ScannedCount})
	require.NotNil(t, out.Items)
	require.Empty(t, out.Items)

	scan := func(mutate func(*dynamodb.ScanInput)) *dynamodb.ScanOutput {
		in := &dynamodb.ScanInput{
			TableName:        aws.String("orders"),
			FilterExpression: aws.String("#s = :s"),
			ExpressionAttributeNames: map[string]string{
				"#s": "status",
			},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":s": &ddbtypes.AttributeValueMemberS{Value: "shipped"},
			},
		}
		mutate(in)

		sout, serr := cli.Scan(ctx, in)
		require.NoError(t, serr)

		return sout
	}

	sout := scan(func(in *dynamodb.ScanInput) { in.Select = ddbtypes.SelectCount })
	require.Equal(t, []int32{3, 5}, []int32{sout.Count, sout.ScannedCount})
	require.Empty(t, sout.Items)

	sout = scan(func(in *dynamodb.ScanInput) { in.Limit = aws.Int32(2) })
	require.Equal(t, []int32{1, 2}, []int32{sout.Count, sout.ScannedCount})
	require.NotEmpty(t, sout.LastEvaluatedKey)

	// The next page starts after the key: only the remaining 3 items are read.
	sout = scan(func(in *dynamodb.ScanInput) { in.ExclusiveStartKey = sout.LastEvaluatedKey })
	require.Equal(t, []int32{2, 3}, []int32{sout.Count, sout.ScannedCount})

	sout = scan(func(in *dynamodb.ScanInput) {
		in.ExpressionAttributeValues[":s"] = &ddbtypes.AttributeValueMemberS{Value: "none"}
	})
	require.Equal(t, []int32{0, 5}, []int32{sout.Count, sout.ScannedCount})
	require.NotNil(t, sout.Items)
}
