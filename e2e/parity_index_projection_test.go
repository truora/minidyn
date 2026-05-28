package e2e

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const parityIndexProjTable = "e2e_index_projection"

func parityCreateIndexProjectionTable(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	input := &dynamodb.CreateTableInput{
		// Only key attributes for the table and GSIs belong here (not projected non-key attrs).
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsi_pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("keys-only-gsi"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: dynamodbtypes.KeyTypeHash},
				},
				Projection: &dynamodbtypes.Projection{
					ProjectionType: dynamodbtypes.ProjectionTypeKeysOnly,
				},
			},
			{
				IndexName: aws.String("include-gsi"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("gsi_pk"), KeyType: dynamodbtypes.KeyTypeHash},
				},
				Projection: &dynamodbtypes.Projection{
					ProjectionType:   dynamodbtypes.ProjectionTypeInclude,
					NonKeyAttributes: []string{"title"},
				},
			},
		},
		TableName: aws.String(parityIndexProjTable),
	}

	_, err := client.CreateTable(ctx, input)
	if err == nil {
		return
	}

	var inUse *dynamodbtypes.ResourceInUseException
	if errors.As(err, &inUse) {
		return
	}

	require.NoError(t, err)
}

func sortedAttrNames(item map[string]dynamodbtypes.AttributeValue) []string {
	names := make([]string, 0, len(item))
	for k := range item {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

func TestE2E_IndexProjection(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "QueryAndScanGSIReturnsProjectedAttributes",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateIndexProjectionTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityIndexProjTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id":     &dynamodbtypes.AttributeValueMemberS{Value: "item-1"},
						"gsi_pk": &dynamodbtypes.AttributeValueMemberS{Value: "pkval"},
						"title":  &dynamodbtypes.AttributeValueMemberS{Value: "MyTitle"},
						"extra":  &dynamodbtypes.AttributeValueMemberS{Value: "secret"},
					},
				})
				require.NoError(t, err)

				qKeysOnly, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(parityIndexProjTable),
					IndexName:              aws.String("keys-only-gsi"),
					KeyConditionExpression: aws.String("gsi_pk = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":g": &dynamodbtypes.AttributeValueMemberS{Value: "pkval"},
					},
				})
				require.NoError(t, err)
				require.Len(t, qKeysOnly.Items, 1)

				qInclude, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(parityIndexProjTable),
					IndexName:              aws.String("include-gsi"),
					KeyConditionExpression: aws.String("gsi_pk = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":g": &dynamodbtypes.AttributeValueMemberS{Value: "pkval"},
					},
				})
				require.NoError(t, err)
				require.Len(t, qInclude.Items, 1)

				sKeysOnly, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(parityIndexProjTable),
					IndexName: aws.String("keys-only-gsi"),
				})
				require.NoError(t, err)
				require.Len(t, sKeysOnly.Items, 1)

				sInclude, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(parityIndexProjTable),
					IndexName: aws.String("include-gsi"),
				})
				require.NoError(t, err)
				require.Len(t, sInclude.Items, 1)

				return []any{
					sortedAttrNames(qKeysOnly.Items[0]),
					sortedAttrNames(qInclude.Items[0]),
					sortedAttrNames(sKeysOnly.Items[0]),
					sortedAttrNames(sInclude.Items[0]),
				}
			},
		},
		{
			name: "QueryAndScanIncludeGSIWithProjectionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateIndexProjectionTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityIndexProjTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id":     &dynamodbtypes.AttributeValueMemberS{Value: "proj-expr-1"},
						"gsi_pk": &dynamodbtypes.AttributeValueMemberS{Value: "pk-pe"},
						"title":  &dynamodbtypes.AttributeValueMemberS{Value: "ScopedTitle"},
						"extra":  &dynamodbtypes.AttributeValueMemberS{Value: "hidden"},
					},
				})
				require.NoError(t, err)

				qTitleOnly, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(parityIndexProjTable),
					IndexName:              aws.String("include-gsi"),
					KeyConditionExpression: aws.String("gsi_pk = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":g": &dynamodbtypes.AttributeValueMemberS{Value: "pk-pe"},
					},
					ProjectionExpression: aws.String("#t"),
					ExpressionAttributeNames: map[string]string{
						"#t": "title",
					},
				})
				require.NoError(t, err)
				require.Len(t, qTitleOnly.Items, 1)

				sTitleOnly, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:            aws.String(parityIndexProjTable),
					IndexName:            aws.String("include-gsi"),
					ProjectionExpression: aws.String("#t"),
					ExpressionAttributeNames: map[string]string{
						"#t": "title",
					},
				})
				require.NoError(t, err)
				require.Len(t, sTitleOnly.Items, 1)

				return []any{
					sortedAttrNames(qTitleOnly.Items[0]),
					sortedAttrNames(sTitleOnly.Items[0]),
				}
			},
		},
		{
			name: "QueryKeysOnlyGSIWithProjectionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()
				ctx := context.Background()

				parityCreateIndexProjectionTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(parityIndexProjTable),
					Item: map[string]dynamodbtypes.AttributeValue{
						"id":     &dynamodbtypes.AttributeValueMemberS{Value: "keys-pe-1"},
						"gsi_pk": &dynamodbtypes.AttributeValueMemberS{Value: "pk-ko-pe"},
						"title":  &dynamodbtypes.AttributeValueMemberS{Value: "NotInKeysOnly"},
						"extra":  &dynamodbtypes.AttributeValueMemberS{Value: "x"},
					},
				})
				require.NoError(t, err)

				qIDOnly, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(parityIndexProjTable),
					IndexName:              aws.String("keys-only-gsi"),
					KeyConditionExpression: aws.String("gsi_pk = :g"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":g": &dynamodbtypes.AttributeValueMemberS{Value: "pk-ko-pe"},
					},
					ProjectionExpression: aws.String("#i"),
					ExpressionAttributeNames: map[string]string{
						"#i": "id",
					},
				})
				require.NoError(t, err)
				require.Len(t, qIDOnly.Items, 1)

				return sortedAttrNames(qIDOnly.Items[0])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
