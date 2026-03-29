package e2e

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const e2eBasicTable = "e2e_basic_ops"

const e2eBatchWriteRequestsLimit = 25

var sdkErrRequestIDRE = regexp.MustCompile(`RequestID:\s*[^,]+,\s*`)

// normalizeSDKErrorString makes minidyn and DynamoDB Local operation errors comparable by
// stripping request IDs and DynamoDB-Local-specific validation suffixes.
func normalizeSDKErrorString(s string) string {
	s = sdkErrRequestIDRE.ReplaceAllString(s, "RequestID: , ")
	s = strings.ReplaceAll(s, " Type unknown.", "")
	s = strings.TrimSpace(s)
	// DynamoDB Local vs minidyn phrasing
	s = strings.ReplaceAll(s,
		"Local Secondary Index range key not specified in Attribute Definitions",
		"Local Secondary Index Range Key not specified in Attribute Definitions",
	)

	s = strings.ReplaceAll(s, "The number of conditions on the keys is invalid", "number of conditions on the keys is invalid")

	if i := strings.Index(s, ";"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}

	return s
}

// RunE2E runs fn against minidyn (httptest) and DynamoDB Local (Docker), then
// asserts both invocations return equal results.
func RunE2E[T any](t *testing.T, fn func(t *testing.T, client *dynamodb.Client) T) {
	t.Helper()

	var gotMinidyn, gotLocal T
	var minidynFailed, localFailed bool

	t.Run("minidyn", func(t *testing.T) {
		gotMinidyn = fn(t, setupMinidynClient(t))
		minidynFailed = t.Failed()
	})

	t.Run("dynamodb-local", func(t *testing.T) {
		gotLocal = fn(t, setupDynamoDBLocalClient(t))
		localFailed = t.Failed()
	})

	if !minidynFailed && !localFailed {
		assert.Equal(t, gotMinidyn, gotLocal)
	}
}

func TestE2E_Basic(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "BasicOperations",
			fn: func(t *testing.T, client *dynamodb.Client) any {
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
