package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// A number key value DynamoDB can't store is rejected with its own message: text that is not a
// number, an exponent beyond 32 bits, and magnitudes outside 1E-130 to 9.99...E+125. The same
// check applies to an ExclusiveStartKey, where silently ignoring it would restart the page.
func TestE2E_TypedKeysInvalidNumber(t *testing.T) {
	s := dynamodbtypes.ScalarAttributeTypeS
	n := dynamodbtypes.ScalarAttributeTypeN
	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	num := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

	invalid := []string{
		"abc", "", "-", "1e", "0x10", " 5", "NaN", "Infinity",
		"1e126", "1e200", "1e40000", "1e2147483647",
		"1e-131", "1e-200", "-1e-40000",
		"1e2147483648", "1e-2147483649", "1e9999999999999999999", "0e99999999999",
	}

	for _, v := range invalid {
		name := v
		if name == "" {
			name = "empty"
		}

		t.Run("PutItem/"+name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityTypedCreate(ctx, t, client, s, n)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityTypedTable),
					Item:      map[string]dynamodbtypes.AttributeValue{"pk": str("a"), "rk": num(v)},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})
	}

	for _, v := range []string{"abc", "1e200", "1e-200"} {
		t.Run("QueryExclusiveStartKey/"+v, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityTypedCreate(ctx, t, client, s, n)

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:                 aws.String(parityTypedTable),
					KeyConditionExpression:    aws.String("pk = :h"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":h": str("a")},
					ExclusiveStartKey:         map[string]dynamodbtypes.AttributeValue{"pk": str("a"), "rk": num(v)},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})

		t.Run("ScanExclusiveStartKey/"+v, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityTypedCreate(ctx, t, client, s, n)

				_, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:         aws.String(parityTypedTable),
					ExclusiveStartKey: map[string]dynamodbtypes.AttributeValue{"pk": str("a"), "rk": num(v)},
				})
				require.Error(t, err)

				return normalizeSDKErrorString(err.Error())
			})
		})
	}

	t.Run("PutItemAtTheLimits", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
			t.Helper()
			ctx := context.Background()

			parityTypedCreate(ctx, t, client, s, n)

			for _, v := range []string{
				"9.9999999999999999999999999999999999999E+125", "-9.9999999999999999999999999999999999999E+125",
				"1e-130", "-1e-130", "0e500", "0e-500", "+5", "5.", ".5",
			} {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityTypedTable),
					Item:      map[string]dynamodbtypes.AttributeValue{"pk": str("a"), "rk": num(v)},
				})
				require.NoError(t, err, v)
			}

			scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(parityTypedTable)})
			require.NoError(t, err)

			return scan.Count
		})
	})
}
