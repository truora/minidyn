package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestQueryAndScanSelectValidation(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))
	c.NoError(ensurePokemonTypeIndex(client))
	c.NoError(createPokemon(client, pokemon{ID: "001", Type: "grass", Name: "Bulbasaur"}))

	tests := []struct {
		name       string
		index      string
		sel        dynamodbtypes.Select
		projection string
		wantErr    string
	}{
		{name: "SpecificWithoutProjection", sel: "SPECIFIC_ATTRIBUTES", wantErr: "Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES"},
		{name: "AllProjectedOnTable", sel: "ALL_PROJECTED_ATTRIBUTES", wantErr: "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"},
		{name: "CountWithProjection", sel: "COUNT", projection: "id", wantErr: "Cannot specify the ProjectionExpression when choosing to get only the Count"},
		{name: "AllAttributesWithProjection", sel: "ALL_ATTRIBUTES", projection: "id", wantErr: "Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES"},
		{name: "Unknown", sel: "BOGUS", wantErr: "Select must be ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT, or SPECIFIC_ATTRIBUTES"},
		{name: "AllProjectedOnIndex", index: "by-type", sel: "ALL_PROJECTED_ATTRIBUTES"},
		{name: "SpecificWithProjection", sel: "SPECIFIC_ATTRIBUTES", projection: "id"},
	}

	for _, tt := range tests {
		t.Run("Query"+tt.name, func(t *testing.T) {
			in := &dynamodb.QueryInput{
				TableName:                 aws.String(tableName),
				Select:                    tt.sel,
				KeyConditionExpression:    aws.String("id = :id"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":id": &dynamodbtypes.AttributeValueMemberS{Value: "001"}},
			}

			if tt.index != "" {
				in.IndexName = aws.String(tt.index)
				in.KeyConditionExpression = aws.String("#t = :id")
				in.ExpressionAttributeNames = map[string]string{"#t": "type"}
				in.ExpressionAttributeValues[":id"] = &dynamodbtypes.AttributeValueMemberS{Value: "grass"}
			}

			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			_, err := client.Query(ctx, in)
			assertSelectResult(t, err, tt.wantErr)
		})

		t.Run("Scan"+tt.name, func(t *testing.T) {
			in := &dynamodb.ScanInput{TableName: aws.String(tableName), Select: tt.sel}

			if tt.index != "" {
				in.IndexName = aws.String(tt.index)
			}

			if tt.projection != "" {
				in.ProjectionExpression = aws.String(tt.projection)
			}

			_, err := client.Scan(ctx, in)
			assertSelectResult(t, err, tt.wantErr)
		})
	}
}

// A key condition error comes before an invalid Select, as in DynamoDB.
func TestQueryKeyConditionErrorBeforeInvalidSelect(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	str := func(s string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: s} }
	num := func(s string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: s} }

	tests := []struct {
		name    string
		value   dynamodbtypes.AttributeValue
		wantErr string
	}{
		{name: "EmptyHash", value: str(""), wantErr: "empty string value"},
		{name: "TypeMismatch", value: num("1"), wantErr: "Condition parameter type does not match schema type"},
		{name: "ValidKeyReportsSelect", value: str("001"), wantErr: "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String(tableName),
				Select:                    "ALL_PROJECTED_ATTRIBUTES",
				KeyConditionExpression:    aws.String("id = :id"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":id": tt.value},
			})

			assertSelectResult(t, err, tt.wantErr)
		})
	}
}

func assertSelectResult(t *testing.T, err error, wantErr string) {
	t.Helper()

	if wantErr == "" {
		require.NoError(t, err)

		return
	}

	require.Error(t, err)
	require.Contains(t, err.Error(), "ValidationException")
	require.Contains(t, err.Error(), wantErr)
}
