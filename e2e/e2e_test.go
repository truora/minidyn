package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const e2eBasicTable = "e2e_basic_ops"

// RunE2E runs fn against minidyn (httptest) and DynamoDB Local (Docker), then
// asserts both invocations return equal results.
func RunE2E[T any](t *testing.T, fn func(t *testing.T, client *dynamodb.Client) T) {
	t.Helper()

	gotMinidyn := fn(t, setupMinidynClient(t))
	gotLocal := fn(t, setupDynamoDBLocalClient(t))

	assert.Equal(t, gotMinidyn, gotLocal)
}

func TestE2E_BasicOperations(t *testing.T) {
	RunE2E(t, func(t *testing.T, client *dynamodb.Client) map[string]types.AttributeValue {
		t.Helper()

		ctx := context.Background()

		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(e2eBasicTable),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			BillingMode: types.BillingModePayPerRequest,
		})
		require.NoError(t, err)

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(e2eBasicTable),
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "k1"},
				"attr": &types.AttributeValueMemberS{Value: "v1"},
			},
		})
		require.NoError(t, err)

		out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(e2eBasicTable),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "k1"},
			},
			ConsistentRead: aws.Bool(true),
		})
		require.NoError(t, err)

		return out.Item
	})
}
