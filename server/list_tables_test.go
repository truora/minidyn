package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestServerListTablesWithSDKv2(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	ctx := context.Background()

	out, err := cli.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	require.NotNil(t, out.TableNames)
	require.Empty(t, out.TableNames)
	require.Nil(t, out.LastEvaluatedTableName)

	for _, name := range []string{"zeta", "Beta", "alpha", "alpha-2", "alpha.1", "gamma_1"} {
		makeBasicTable(t, cli, name, "pk")
	}

	out, err = cli.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	require.Equal(t, []string{"Beta", "alpha", "alpha-2", "alpha.1", "gamma_1", "zeta"}, out.TableNames)
	require.Nil(t, out.LastEvaluatedTableName)

	input := &dynamodb.ListTablesInput{Limit: aws.Int32(2)}

	var (
		pages    [][]string
		lastKeys []string
	)

	for {
		page, pageErr := cli.ListTables(ctx, input)
		require.NoError(t, pageErr)

		pages = append(pages, page.TableNames)
		lastKeys = append(lastKeys, aws.ToString(page.LastEvaluatedTableName))

		if page.LastEvaluatedTableName == nil {
			break
		}

		input.ExclusiveStartTableName = page.LastEvaluatedTableName
	}

	require.Equal(t, [][]string{{"Beta", "alpha"}, {"alpha-2", "alpha.1"}, {"gamma_1", "zeta"}}, pages)
	require.Equal(t, []string{"alpha", "alpha.1", ""}, lastKeys)

	out, err = cli.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(6)})
	require.NoError(t, err)
	require.Len(t, out.TableNames, 6)
	require.Nil(t, out.LastEvaluatedTableName)

	out, err = cli.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: aws.String("b")})
	require.NoError(t, err)
	require.Equal(t, []string{"gamma_1", "zeta"}, out.TableNames)

	out, err = cli.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: aws.String("zzz")})
	require.NoError(t, err)
	require.NotNil(t, out.TableNames)
	require.Empty(t, out.TableNames)
}

func TestServerListTablesInvalidLimit(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)

	for limit, msg := range map[int32]string{
		0:   "Limit must be greater than or equal to 1",
		-1:  "Limit must be greater than or equal to 1",
		101: "Limit in ListTables must be <= 100",
	} {
		_, err := cli.ListTables(context.Background(), &dynamodb.ListTablesInput{Limit: aws.Int32(limit)})

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "ValidationException", apiErr.ErrorCode())
		require.Equal(t, msg, apiErr.ErrorMessage())
	}
}
