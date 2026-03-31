package e2e

import (
	"context"
	"reflect"
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

// DynamoDB Local often omits the ": keys: {#...}" suffix for unused ExpressionAttributeNames
// when no expression is present (for example GetItem with only ExpressionAttributeNames).
// Minidyn includes that detail; strip it so parity compares the shared ValidationException text.
var sdkErrUnusedExprAttrNamesKeysRE = regexp.MustCompile(
	`ExpressionAttributeNames can only be specified when using expressions: keys: \{[^}]+\}`,
)

// DynamoDB Local appends ": <ExpressionType> is null" to the "ExpressionAttributeValues can
// only be specified when using expressions" message (e.g. ": ConditionExpression is null").
// Minidyn omits this suffix because the generic validation helper is not aware of the
// calling operation's expression type. Strip it so both sides compare equal.
var sdkErrExprAttrValuesNullExprRE = regexp.MustCompile(
	`(ExpressionAttributeValues can only be specified when using expressions): \w+Expression is null`,
)

// normalizeSDKErrorString makes minidyn and DynamoDB Local operation errors comparable by
// stripping request IDs and DynamoDB-Local-specific validation suffixes.
func normalizeSDKErrorString(s string) string {
	s = sdkErrRequestIDRE.ReplaceAllString(s, "RequestID: , ")
	s = strings.TrimSpace(s)

	if i := strings.Index(s, ";"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}

	s = sdkErrUnusedExprAttrNamesKeysRE.ReplaceAllString(s,
		"ExpressionAttributeNames can only be specified when using expressions")

	s = sdkErrExprAttrValuesNullExprRE.ReplaceAllString(s, "$1")

	return s
}

// assertParityEqual compares minidyn vs DynamoDB Local results. For slices it
// asserts each index separately so failures name the server and position.
func assertParityEqual(t *testing.T, gotMinidyn, gotLocal any) {
	t.Helper()

	vm := reflect.ValueOf(gotMinidyn)
	vl := reflect.ValueOf(gotLocal)
	if vm.Kind() == reflect.Slice && vl.Kind() == reflect.Slice {
		if vm.Len() != vl.Len() {
			t.Errorf("slice length mismatch (minidyn=%d, dynamodb-local=%d)\n  minidyn:         %#v\n  dynamodb-local: %#v",
				vm.Len(), vl.Len(), gotMinidyn, gotLocal)
			return
		}
		for i := 0; i < vm.Len(); i++ {
			em := vm.Index(i).Interface()
			el := vl.Index(i).Interface()
			assert.Equal(t, em, el,
				"index %d — minidyn %#v, dynamodb-local %#v", i, em, el)
		}
		return
	}

	assert.Equal(t, gotMinidyn, gotLocal,
		"minidyn %#v vs dynamodb-local %#v", gotMinidyn, gotLocal)
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
		assertParityEqual(t, gotMinidyn, gotLocal)
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
