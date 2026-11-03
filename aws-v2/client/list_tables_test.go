package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func TestListTables(t *testing.T) {
	ctx := context.Background()

	client := NewClient()

	out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	require.NotNil(t, out.TableNames)
	require.Empty(t, out.TableNames)
	require.Nil(t, out.LastEvaluatedTableName)

	for _, name := range []string{"zeta", "Beta", "alpha", "alpha-2", "alpha.1", "gamma_1"} {
		require.NoError(t, AddTable(ctx, client, name, "pk", ""))
	}

	t.Run("all tables in byte order", func(t *testing.T) {
		out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
		require.NoError(t, err)
		require.Equal(t, []string{"Beta", "alpha", "alpha-2", "alpha.1", "gamma_1", "zeta"}, out.TableNames)
		require.Nil(t, out.LastEvaluatedTableName)
	})

	t.Run("pages with limit", func(t *testing.T) {
		input := &dynamodb.ListTablesInput{Limit: aws.Int32(2)}

		var (
			pages    [][]string
			lastKeys []string
		)

		for {
			out, err := client.ListTables(ctx, input)
			require.NoError(t, err)

			pages = append(pages, out.TableNames)
			lastKeys = append(lastKeys, aws.ToString(out.LastEvaluatedTableName))

			if out.LastEvaluatedTableName == nil {
				break
			}

			input.ExclusiveStartTableName = out.LastEvaluatedTableName
		}

		require.Equal(t, [][]string{{"Beta", "alpha"}, {"alpha-2", "alpha.1"}, {"gamma_1", "zeta"}}, pages)
		require.Equal(t, []string{"alpha", "alpha.1", ""}, lastKeys)
	})

	t.Run("limit equal to remaining tables has no last key", func(t *testing.T) {
		out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(6)})
		require.NoError(t, err)
		require.Len(t, out.TableNames, 6)
		require.Nil(t, out.LastEvaluatedTableName)
	})

	t.Run("exclusive start name that does not exist", func(t *testing.T) {
		out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: aws.String("b")})
		require.NoError(t, err)
		require.Equal(t, []string{"gamma_1", "zeta"}, out.TableNames)

		out, err = client.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: aws.String("zzz")})
		require.NoError(t, err)
		require.NotNil(t, out.TableNames)
		require.Empty(t, out.TableNames)
	})

	t.Run("invalid limit", func(t *testing.T) {
		for limit, msg := range map[int32]string{
			0:   "Limit must be greater than or equal to 1",
			-1:  "Limit must be greater than or equal to 1",
			101: "Limit in ListTables must be <= 100",
		} {
			_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(limit)})

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "ValidationException", apiErr.ErrorCode())
			require.Equal(t, msg, apiErr.ErrorMessage())
		}

		_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(100)})
		require.NoError(t, err)
	})
}
