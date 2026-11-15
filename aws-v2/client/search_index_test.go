package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const unknownIndexMsg = "The table does not have the specified index: nope | table: " + tableName

// An IndexName the table does not have is rejected before anything else of the request,
// including the pre-check of the expression attributes, as DynamoDB does.
func TestQueryAndScanUnknownIndex(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))
	c.NoError(createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}))

	id := func(v string) map[string]dynamodbtypes.AttributeValue {
		return map[string]dynamodbtypes.AttributeValue{":id": &dynamodbtypes.AttributeValueMemberS{Value: v}}
	}

	queries := map[string]func(in *dynamodb.QueryInput){
		"Plain":          func(*dynamodb.QueryInput) {},
		"UnusedName":     func(in *dynamodb.QueryInput) { in.ExpressionAttributeNames = map[string]string{"#u": "id"} },
		"UnusedValue":    func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues[":u"] = id("x")[":id"] },
		"UndeclaredName": func(in *dynamodb.QueryInput) { in.KeyConditionExpression = aws.String("#n = :id") },
		"UndefinedValue": func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues = nil },
		"InvalidSelect":  func(in *dynamodb.QueryInput) { in.Select = "BOGUS" },
		"EmptyHashValue": func(in *dynamodb.QueryInput) { in.ExpressionAttributeValues = id("") },
	}

	for name, mod := range queries {
		t.Run("Query"+name, func(t *testing.T) {
			in := &dynamodb.QueryInput{
				TableName:                 aws.String(tableName),
				IndexName:                 aws.String("nope"),
				KeyConditionExpression:    aws.String("id = :id"),
				ExpressionAttributeValues: id("001"),
			}
			mod(in)

			_, err := client.Query(ctx, in)
			requireValidationError(t, err, unknownIndexMsg)
		})
	}

	scans := map[string]func(in *dynamodb.ScanInput){
		"Plain":      func(*dynamodb.ScanInput) {},
		"UnusedName": func(in *dynamodb.ScanInput) { in.ExpressionAttributeNames = map[string]string{"#u": "id"} },
		"UndeclaredName": func(in *dynamodb.ScanInput) {
			in.FilterExpression = aws.String("#n = :id")
			in.ExpressionAttributeValues = id("001")
		},
		"UndefinedValue": func(in *dynamodb.ScanInput) { in.FilterExpression = aws.String("id = :id") },
		"InvalidSelect":  func(in *dynamodb.ScanInput) { in.Select = "BOGUS" },
	}

	for name, mod := range scans {
		t.Run("Scan"+name, func(t *testing.T) {
			in := &dynamodb.ScanInput{TableName: aws.String(tableName), IndexName: aws.String("nope")}
			mod(in)

			_, err := client.Scan(ctx, in)
			requireValidationError(t, err, unknownIndexMsg)
		})
	}
}

// A missing table is reported before the pre-check of the expression attributes.
func TestQueryAndScanMissingTableBeforeUnusedName(t *testing.T) {
	client := setupClient(tableName)
	ctx := context.Background()

	_, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("missing"),
		KeyConditionExpression:    aws.String("id = :id"),
		ExpressionAttributeNames:  map[string]string{"#u": "id"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":id": &dynamodbtypes.AttributeValueMemberS{Value: "x"}},
	})
	require.ErrorContains(t, err, "ResourceNotFoundException")

	_, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName:                aws.String("missing"),
		ExpressionAttributeNames: map[string]string{"#u": "id"},
	})
	require.ErrorContains(t, err, "ResourceNotFoundException")
}
