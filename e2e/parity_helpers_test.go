package e2e

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

const (
	parityPokemonTable    = "pokemons"
	parityByTypeIndexName = "by-type"
)

// parityPokemon mirrors aws-v2/client/client_test pokemon struct (json tag for MarshalMapWithOptions).
type parityPokemon struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	SecondType string   `json:"second_type"`
	Name       string   `json:"name"`
	Level      int64    `json:"lvl"`
	Moves      []string `json:"moves" dynamodbav:"moves,stringset,omitempty"`
	Local      []string `json:"local"`
}

func parityCreatePokemonTable(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	var cunit int64 = 10

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		TableName: aws.String(parityPokemonTable),
		ProvisionedThroughput: &dynamodbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  &cunit,
			WriteCapacityUnits: &cunit,
		},
	}

	_, err := client.CreateTable(ctx, input)
	if err == nil {
		return
	}

	var oe smithy.APIError
	var inUse *dynamodbtypes.ResourceInUseException
	if errors.As(err, &oe) && errors.As(err, &inUse) {
		return
	}

	require.NoError(t, err)
}

// parityWaitUntilGlobalSecondaryIndexRemoved waits until the named GSI is no longer reported on
// the table. DynamoDB Local removes GSIs asynchronously; minidyn removes synchronously.
func parityWaitUntilGlobalSecondaryIndexRemoved(ctx context.Context, t *testing.T, client *dynamodb.Client, table, indexName string) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)

	for time.Now().Before(deadline) {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			time.Sleep(200 * time.Millisecond)

			continue
		}

		stillThere := false

		for _, g := range out.Table.GlobalSecondaryIndexes {
			if aws.ToString(g.IndexName) == indexName {
				stillThere = true

				break
			}
		}

		if !stillThere {
			return
		}

		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for GSI %q to be removed from table %q", indexName, table)
}

func parityMarshalPokemon(t *testing.T, p parityPokemon) map[string]dynamodbtypes.AttributeValue {
	t.Helper()

	opt := func(o *attributevalue.EncoderOptions) {
		o.TagKey = "json"
	}

	item, err := attributevalue.MarshalMapWithOptions(p, opt)
	require.NoError(t, err)

	return item
}

func parityCreatePokemon(ctx context.Context, t *testing.T, client *dynamodb.Client, p parityPokemon) {
	t.Helper()

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      parityMarshalPokemon(t, p),
		TableName: aws.String(parityPokemonTable),
	})
	require.NoError(t, err)
}

func parityGetPokemon(ctx context.Context, t *testing.T, client *dynamodb.Client, id string) map[string]dynamodbtypes.AttributeValue {
	t.Helper()

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(parityPokemonTable),
		Key: map[string]dynamodbtypes.AttributeValue{
			"id": &dynamodbtypes.AttributeValueMemberS{Value: id},
		},
	})
	require.NoError(t, err)

	return out.Item
}

func parityGetPokemonsByType(ctx context.Context, t *testing.T, client *dynamodb.Client, typ string) []map[string]dynamodbtypes.AttributeValue {
	t.Helper()

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":type": &dynamodbtypes.AttributeValueMemberS{Value: typ},
		},
		ExpressionAttributeNames: map[string]string{
			"#type": "type",
		},
		KeyConditionExpression: aws.String("#type = :type"),
		TableName:              aws.String(parityPokemonTable),
		IndexName:              aws.String(parityByTypeIndexName),
	})
	if err != nil {
		return nil
	}

	return out.Items
}

func parityWaitUntilGSIActive(ctx context.Context, t *testing.T, client *dynamodb.Client, table, indexName string) {
	t.Helper()

	deadline := time.Now().Add(90 * time.Second)

	for time.Now().Before(deadline) {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			time.Sleep(200 * time.Millisecond)

			continue
		}

		for _, g := range out.Table.GlobalSecondaryIndexes {
			if aws.ToString(g.IndexName) != indexName {
				continue
			}

			// Minidyn DescribeTable omits IndexStatus; DynamoDB Local reports CREATING then ACTIVE.
			if g.IndexStatus == dynamodbtypes.IndexStatusActive || g.IndexStatus == "" {
				return
			}
		}

		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for GSI %q ACTIVE on table %q", indexName, table)
}

func parityAddByTypeGlobalSecondaryIndex(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		TableName: aws.String(parityPokemonTable),
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{
				Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(parityByTypeIndexName),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("type"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					Projection: &dynamodbtypes.Projection{
						ProjectionType: dynamodbtypes.ProjectionTypeAll,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, parityPokemonTable, parityByTypeIndexName)
}

func parityAddSortBySecondTypeIndex(ctx context.Context, t *testing.T, client *dynamodb.Client) {
	t.Helper()

	const indexName = "sort-by-second-type"

	_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("type"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("second_type"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		TableName: aws.String(parityPokemonTable),
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{
			{
				Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String(indexName),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String("second_type"), KeyType: dynamodbtypes.KeyTypeRange},
					},
					Projection: &dynamodbtypes.Projection{
						ProjectionType: dynamodbtypes.ProjectionTypeAll,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	parityWaitUntilGSIActive(ctx, t, client, parityPokemonTable, indexName)
}

func parityAttrValueString(av dynamodbtypes.AttributeValue) string {
	switch v := av.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		return "S:" + v.Value
	case *dynamodbtypes.AttributeValueMemberN:
		return "N:" + v.Value
	default:
		return "OTHER"
	}
}

func parityKeySignature(key map[string]dynamodbtypes.AttributeValue) string {
	parts := make([]string, 0, len(key))

	for attr, v := range key {
		parts = append(parts, attr+"="+parityAttrValueString(v))
	}

	sort.Strings(parts)

	return strings.Join(parts, ",")
}

// paritySortedBatchGetIDs returns sorted "id" values from BatchGetItem responses (order is undefined in DynamoDB).
func paritySortedBatchGetIDs(out *dynamodb.BatchGetItemOutput, table string) []string {
	items := out.Responses[table]
	ids := make([]string, 0, len(items))

	for _, it := range items {
		idv, ok := it["id"].(*dynamodbtypes.AttributeValueMemberS)
		if ok {
			ids = append(ids, idv.Value)
		}
	}

	sort.Strings(ids)

	return ids
}

// paritySortedUnprocessedKeySignatures sorts BatchGetItem unprocessed key descriptions for stable comparison.
func paritySortedUnprocessedKeySignatures(out *dynamodb.BatchGetItemOutput, table string) []string {
	ka, ok := out.UnprocessedKeys[table]
	if !ok {
		return nil
	}

	sigs := make([]string, 0, len(ka.Keys))

	for _, k := range ka.Keys {
		sigs = append(sigs, parityKeySignature(k))
	}

	sort.Strings(sigs)

	return sigs
}
